import { describe, expect, it } from "vitest";
import { NodePoolRetirement } from "./retirement";

describe("NodePoolRetirement", () => {
  it("closes registration and runs one callback", async () => {
    const retirement = new NodePoolRetirement();
    retirement.enable();
    let calls = 0;
    retirement.register(async ({ deadline }) => {
      calls += 1;
      expect(deadline.remainingMs()).toBeGreaterThan(0);
    });
    await expect(
      retirement.retire({ reason: "source_change", remainingMs: 100 }),
    ).resolves.toEqual({ type: "completed" });
    await expect(
      retirement.retire({ reason: "source_change", remainingMs: 100 }),
    ).resolves.toEqual({ type: "completed" });
    expect(calls).toBe(1);
  });

  it("reports callback errors and timeout", async () => {
    const error = new NodePoolRetirement();
    error.enable();
    error.register(async () => {
      throw new Error("cleanup failed");
    });
    await expect(
      error.retire({ reason: "source_change", remainingMs: 100 }),
    ).resolves.toEqual({
      type: "callback_error",
    });

    const timeout = new NodePoolRetirement();
    timeout.enable();
    timeout.register(() => new Promise<void>(() => {}));
    await expect(
      timeout.retire({ reason: "source_change", remainingMs: 1 }),
    ).resolves.toEqual({
      type: "timeout",
    });
  });

  it("rejects fractional deadlines", () => {
    const retirement = new NodePoolRetirement();
    retirement.enable();
    expect(() =>
      retirement.retire({ reason: "source_change", remainingMs: 1.5 }),
    ).toThrow("Invalid Node pool retirement request.");
  });

  it("accepts every supervisor retirement reason", async () => {
    const reasons = [
      "generation_age",
      "rss_limit",
      "package_limit",
      "memory_pressure",
      "source_change",
      "topology_change",
      "shutdown",
      "request_failure",
    ] as const;
    for (const reason of reasons) {
      const retirement = new NodePoolRetirement();
      retirement.enable();
      let received: string | undefined;
      retirement.register(async (input) => {
        received = input.reason;
      });
      await expect(retirement.retire({ reason, remainingMs: 100 })).resolves.toEqual({
        type: "completed",
      });
      expect(received).toBe(reason);
    }
  });
});
