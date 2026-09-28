import { getEventListeners } from "node:events";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { SyscallsImpl } from "./syscalls";

function makeSyscalls(hasIsolateWorkerAncestor: boolean): SyscallsImpl {
  return new SyscallsImpl(
    { canonicalizedPath: "actions.js", function: "run" },
    "lambda-execute-id",
    "http://127.0.0.1:3210",
    "callback-token",
    null,
    null,
    {
      requestId: "request-id",
      executionId: "execution-id",
      isRoot: false,
      parentScheduledJob: null,
      parentScheduledJobComponentId: null,
      ip: null,
      userAgent: null,
    },
    hasIsolateWorkerAncestor,
    null,
    { name: "local-test", region: null, class: "s16" },
  );
}

test("action callbacks propagate isolate-worker ancestry", () => {
  expect(makeSyscalls(true).headers("1.0")).toMatchObject({
    "Convex-Isolate-Worker-Ancestor": "true",
  });
  expect(makeSyscalls(false).headers("1.0")).not.toHaveProperty(
    "Convex-Isolate-Worker-Ancestor",
  );
});

describe("action callback retries", () => {
  const fetchMock = vi.fn<typeof fetch>();
  const syscallArgs = JSON.stringify({
    requestId: "lambda-execute-id",
    name: "functions:run",
    args: [{ input: "value" }],
    version: "1.0",
  });
  const occBody = JSON.stringify(
    {
      code: "OptimisticConcurrencyControlFailure",
      message: "Documents changed on every retry: café.",
    },
    null,
    2,
  );

  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    fetchMock.mockReset();
    fetchMock.mockImplementation(
      async () =>
        new Response(
          JSON.stringify({ status: "success", value: { result: "ok" } }),
        ),
    );
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  test.each(["query", "mutation"])(
    "%s forwards exhausted OCC without restarting the backend retry budget",
    async (operation) => {
      fetchMock.mockResolvedValueOnce(new Response(occBody, { status: 503 }));
      const syscalls = makeSyscalls(false);

      await expect(
        syscalls.asyncSyscall(`1.0/actions/${operation}`, syscallArgs),
      ).rejects.toEqual(new Error(occBody));

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
      expect(syscalls.pendingSyscallCount[`1.0/actions/${operation}`]).toBe(0);
    },
  );

  test.each([500, 502, 503, 504, 599])(
    "retries HTTP %i and preserves the mutation result and deduplication identifier",
    async (status) => {
      fetchMock.mockResolvedValueOnce(
        new Response(
          JSON.stringify({ code: "Overloaded", message: "Try again" }),
          { status },
        ),
      );
      fetchMock.mockRejectedValueOnce(
        new TypeError("fetch failed", { cause: { code: "ECONNRESET" } }),
      );
      const syscalls = makeSyscalls(false);
      const result = expect(
        syscalls.asyncSyscall("1.0/actions/mutation", syscallArgs),
      ).resolves.toBe(JSON.stringify({ result: "ok" }));
      await vi.runAllTimersAsync();
      await result;

      await expect(
        syscalls.asyncSyscall("1.0/actions/mutation", syscallArgs),
      ).resolves.toBe(JSON.stringify({ result: "ok" }));
      expect(fetchMock).toHaveBeenCalledTimes(4);
      const requests: unknown[] = fetchMock.mock.calls.map(([url, init]) => {
        expect(String(url)).toBe("http://127.0.0.1:3210/api/actions/mutation");
        expect(init?.signal).toBe(syscalls.abortController.signal);
        expect(typeof init?.body).toBe("string");
        return JSON.parse(init!.body as string);
      });
      expect(requests[0]).toEqual({
        path: "functions:run",
        args: [{ input: "value" }],
        mutationIdentifier: {
          sessionId: syscalls.mutationSessionId,
          requestId: 0,
        },
      });
      expect(requests[1]).toEqual(requests[0]);
      expect(requests[2]).toEqual(requests[0]);
      expect(requests[3]).toMatchObject({
        mutationIdentifier: {
          sessionId: syscalls.mutationSessionId,
          requestId: 1,
        },
      });
    },
  );

  test("queries retry ordinary 503 responses without mutation identifiers", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response("Unavailable", { status: 503 }),
    );
    const result = expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/query", syscallArgs),
    ).resolves.toBe(JSON.stringify({ result: "ok" }));
    await vi.runAllTimersAsync();
    await result;

    expect(fetchMock).toHaveBeenCalledTimes(2);
    for (const [url, init] of fetchMock.mock.calls) {
      expect(String(url)).toBe("http://127.0.0.1:3210/api/actions/query");
      expect(JSON.parse(init!.body as string)).not.toHaveProperty(
        "mutationIdentifier",
      );
    }
  });

  test("ordinary 503 failures retain the five-attempt limit and final error body", async () => {
    fetchMock.mockImplementation(
      async () => new Response("Proxy unavailable", { status: 503 }),
    );
    const result = expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).rejects.toEqual(new Error("Proxy unavailable"));
    await vi.runAllTimersAsync();
    await result;

    expect(fetchMock).toHaveBeenCalledTimes(5);
    expect(vi.getTimerCount()).toBe(0);
  });

  test.each([
    ["empty body", null],
    ["HTML", "<html>Service unavailable</html>"],
    ["plain OCC text", "OptimisticConcurrencyControlFailure"],
    ["truncated JSON", '{"code":"OptimisticConcurrencyControlFailure",'],
    ["JSON null", "null"],
    ["JSON string", JSON.stringify("OptimisticConcurrencyControlFailure")],
    ["array", `[${occBody}]`],
    ["nested code", JSON.stringify({ error: JSON.parse(occBody) })],
    ["missing message", '{"code":"OptimisticConcurrencyControlFailure"}'],
    [
      "nonstring message",
      '{"code":"OptimisticConcurrencyControlFailure","message":null}',
    ],
    ["nonstring code", '{"code":503,"message":"Unavailable"}'],
    [
      "code suffix",
      '{"code":"OptimisticConcurrencyControlFailureExtra","message":"Unavailable"}',
    ],
    [
      "wrong code case",
      '{"code":"optimisticConcurrencyControlFailure","message":"Unavailable"}',
    ],
    [
      "OCC mentioned in another error",
      '{"code":"Overloaded","message":"OptimisticConcurrencyControlFailure"}',
    ],
  ])("retries a 503 with %s", async (_description, body) => {
    fetchMock.mockResolvedValueOnce(new Response(body, { status: 503 }));
    const result = expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).resolves.toBe(JSON.stringify({ result: "ok" }));
    await vi.runAllTimersAsync();
    await result;

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  test("does not infer OCC from its code on a different HTTP status", async () => {
    fetchMock.mockResolvedValueOnce(new Response(occBody, { status: 502 }));
    const result = expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).resolves.toBe(JSON.stringify({ result: "ok" }));
    await vi.runAllTimersAsync();
    await result;

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  test("forwards OCC after a transient failure and permits a separate caller retry", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response("Unavailable", { status: 503 }),
    );
    fetchMock.mockResolvedValueOnce(new Response(occBody, { status: 503 }));
    const syscalls = makeSyscalls(false);
    const result = expect(
      syscalls.asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).rejects.toEqual(new Error(occBody));
    await vi.runAllTimersAsync();
    await result;
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][1]?.body).toBe(
      fetchMock.mock.calls[1][1]?.body,
    );

    await expect(
      syscalls.asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).resolves.toBe(JSON.stringify({ result: "ok" }));
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(
      JSON.parse(fetchMock.mock.calls[2][1]!.body as string),
    ).toMatchObject({
      mutationIdentifier: {
        sessionId: syscalls.mutationSessionId,
        requestId: 1,
      },
    });
  });

  test.each(["query", "mutation", "action"])(
    "%s preserves structured UDF errors without retrying",
    async (operation) => {
      fetchMock.mockResolvedValueOnce(
        new Response(
          JSON.stringify({ status: "error", errorMessage: "Function failed" }),
          { status: 560 },
        ),
      );
      await expect(
        makeSyscalls(false).asyncSyscall(
          `1.0/actions/${operation}`,
          syscallArgs,
        ),
      ).rejects.toEqual(new Error("Function failed"));
      expect(fetchMock).toHaveBeenCalledTimes(1);
    },
  );

  test("does not retry non-idempotent action 503 responses", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response("Unavailable", { status: 503 }),
    );
    await expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/action", syscallArgs),
    ).rejects.toEqual(new Error("Unavailable"));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  test("accepts an OCC error split across response chunks", async () => {
    const bytes = new TextEncoder().encode("\uFEFF" + occBody);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const byte of bytes) {
          controller.enqueue(Uint8Array.of(byte));
        }
        controller.close();
      },
    });
    fetchMock.mockResolvedValueOnce(new Response(body, { status: 503 }));
    await expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).rejects.toEqual(new Error(occBody));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  test("retries after a response stream fails during OCC inspection", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(occBody));
      },
      pull(controller) {
        controller.error(
          new Error("Connection reset before response completed"),
        );
      },
    });
    fetchMock.mockResolvedValueOnce(new Response(body, { status: 503 }));
    const result = expect(
      makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
    ).resolves.toBe(JSON.stringify({ result: "ok" }));
    await vi.runAllTimersAsync();
    await result;
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  test.each(["oversized", "stalled"])(
    "bounds inspection and cancels an %s 503 response before retrying",
    async (kind) => {
      const cancel = vi.fn(() => new Promise<void>(() => {}));
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          const text =
            kind === "oversized" ? occBody + " ".repeat(128 * 1024) : occBody;
          controller.enqueue(new TextEncoder().encode(text));
        },
        cancel,
      });
      fetchMock.mockResolvedValueOnce(new Response(body, { status: 503 }));
      const result = expect(
        makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
      ).resolves.toBe(JSON.stringify({ result: "ok" }));
      await vi.advanceTimersByTimeAsync(0);
      expect(cancel).toHaveBeenCalledTimes(kind === "oversized" ? 1 : 0);
      expect(fetchMock).toHaveBeenCalledTimes(1);
      await vi.runAllTimersAsync();
      await result;
      expect(cancel).toHaveBeenCalledTimes(1);
      expect(body.locked).toBe(false);
      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  test("disposing during OCC inspection parks the callback and cancels its stream", async () => {
    const cancel = vi.fn();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(occBody));
      },
      cancel,
    });
    fetchMock.mockResolvedValueOnce(new Response(body, { status: 503 }));
    const syscalls = makeSyscalls(false);
    const settled = vi.fn();
    void syscalls
      .asyncSyscall("1.0/actions/mutation", syscallArgs)
      .then(settled, settled);
    await vi.advanceTimersByTimeAsync(0);
    expect(body.locked).toBe(true);

    syscalls.dispose();
    await vi.runAllTimersAsync();

    expect(cancel).toHaveBeenCalledTimes(1);
    expect(body.locked).toBe(false);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(settled).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  test("disposing during backoff stops retries and parks the callback", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response("Unavailable", { status: 503 }),
    );
    const syscalls = makeSyscalls(false);
    const settled = vi.fn();
    void syscalls
      .asyncSyscall("1.0/actions/mutation", syscallArgs)
      .then(settled, settled);
    await vi.advanceTimersByTimeAsync(0);

    syscalls.dispose();
    await vi.runAllTimersAsync();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(settled).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  test.each(["timeout", "dispose"])(
    "handles a late stream cancellation failure after inspection ends by %s",
    async (stop) => {
      let rejectCancellation!: (reason: Error) => void;
      const cancellation = new Promise<void>((_resolve, reject) => {
        rejectCancellation = reject;
      });
      const cancel = vi.fn(() => cancellation);
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(occBody));
        },
        cancel,
      });
      fetchMock.mockResolvedValueOnce(new Response(body, { status: 503 }));
      const syscalls = makeSyscalls(false);
      const settled = vi.fn();
      void syscalls
        .asyncSyscall("1.0/actions/mutation", syscallArgs)
        .then(settled, settled);
      await vi.advanceTimersByTimeAsync(0);
      expect(body.locked).toBe(true);

      if (stop === "dispose") {
        syscalls.dispose();
      }
      await vi.runAllTimersAsync();
      expect(settled).toHaveBeenCalledTimes(stop === "timeout" ? 1 : 0);
      expect(cancel).toHaveBeenCalledTimes(1);
      expect(body.locked).toBe(false);
      expect(
        getEventListeners(syscalls.abortController.signal, "abort"),
      ).toEqual([]);

      syscalls.dispose();
      // Rejection must remain observed after the inspection race and action end,
      // when an unhandled rejection could affect another invocation.
      rejectCancellation(new Error("Response cancellation failed"));
      await vi.runAllTimersAsync();
      await expect(
        makeSyscalls(false).asyncSyscall("1.0/actions/mutation", syscallArgs),
      ).resolves.toBe(JSON.stringify({ result: "ok" }));
      expect(settled).toHaveBeenCalledTimes(stop === "timeout" ? 1 : 0);
      expect(fetchMock).toHaveBeenCalledTimes(stop === "timeout" ? 3 : 2);
      expect(vi.getTimerCount()).toBe(0);
    },
  );
});
