import {
  AbortController,
  AbortSignal,
} from "abortcontroller-polyfill/dist/abortcontroller";
import { performAsyncOp } from "./syscall";

// These alarms belong to one invocation. Unlike setTimeout, an unused deadline
// has no callback registration in ACTIVE_TIMERS that could survive reuse.
AbortSignal.timeout = (time: number): AbortSignal => {
  const controller = new AbortController();
  // Keep the polyfill's existing setTimeout coercion and reason while changing
  // ownership of the alarm.
  const timeoutMs = time ? Number(time) : 0;
  void performAsyncOp("abortSignalTimeout", timeoutMs).then(() => {
    controller.abort(
      new DOMException(`This signal is timeout in ${time}ms`, "TimeoutError"),
    );
  });
  return controller.signal;
};

Object.defineProperty(AbortController.prototype, Symbol.toStringTag, {
  value: "AbortController",
  enumerable: false,
  writable: false,
  configurable: true,
});
Object.defineProperty(AbortSignal.prototype, Symbol.toStringTag, {
  value: "AbortSignal",
  enumerable: false,
  writable: false,
  configurable: true,
});

export const setupAbortSignal = (global) => {
  global.AbortController = AbortController;
  global.AbortSignal = AbortSignal;
};
