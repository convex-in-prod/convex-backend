import { afterEach, expect, test, vi } from "vitest";
import { ConvexError } from "../../values/errors.js";
import { performAsyncSyscall } from "./syscall.js";

afterEach(() => {
  vi.unstubAllGlobals();
});

async function rejectedAsyncSyscall(hostError: Error): Promise<Error> {
  vi.stubGlobal("Convex", {
    asyncSyscall: () => Promise.reject(hostError),
  });

  try {
    await performAsyncSyscall("1.0/test", {});
  } catch (error) {
    return error as Error;
  }
  throw new Error("Expected async syscall to reject");
}

function expectCauseDescriptor(wrapper: Error, hostError: Error) {
  expect(Object.getOwnPropertyDescriptor(wrapper, "cause")).toEqual({
    configurable: true,
    enumerable: false,
    value: hostError,
    writable: true,
  });
  expect(wrapper.stack).toContain(wrapper.message);
  expect(wrapper.stack).not.toBe(hostError.stack);
}

test("plain async syscall normalization preserves the host error as cause", async () => {
  const hostError = new Error("host rejection");
  hostError.stack = "host stack";

  const wrapper = await rejectedAsyncSyscall(hostError);

  expect(wrapper).toBeInstanceOf(Error);
  expect(wrapper).not.toBe(hostError);
  expect(wrapper.name).toBe("Error");
  expect(wrapper.message).toBe(hostError.message);
  expect(Object.prototype.hasOwnProperty.call(wrapper, "data")).toBe(false);
  expectCauseDescriptor(wrapper, hostError);
});

test("ConvexError async syscall normalization preserves the host error as cause", async () => {
  const hostError = Object.assign(new Error("host rejection"), {
    data: { code: "missing" },
  });
  hostError.stack = "host stack";

  const wrapper = await rejectedAsyncSyscall(hostError);

  expect(wrapper).toBeInstanceOf(ConvexError);
  expect(wrapper).not.toBe(hostError);
  expect(wrapper.name).toBe("ConvexError");
  expect(wrapper.message).toBe(hostError.message);
  expect((wrapper as Error & { data: unknown }).data).toEqual(hostError.data);
  expectCauseDescriptor(wrapper, hostError);
});
