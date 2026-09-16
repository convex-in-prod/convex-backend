import { performance } from "node:perf_hooks";
import type {
  NodePoolRetirementDeadline,
  NodePoolRetirementHandler,
  NodePoolRetirementReason,
} from "convex/server";

type RetirementResult = {
  type: "not_registered" | "completed" | "callback_error" | "timeout";
};

const reasons: ReadonlySet<string> = new Set<NodePoolRetirementReason>([
  "generation_age",
  "rss_limit",
  "package_limit",
  "memory_pressure",
  "source_change",
  "topology_change",
  "shutdown",
  "request_failure",
]);

export class NodePoolRetirement {
  private enabled = false;
  private callback: NodePoolRetirementHandler | null = null;
  private result: Promise<RetirementResult> | null = null;

  enable(): void {
    this.enabled = true;
  }

  register(callback: NodePoolRetirementHandler): void {
    if (!this.enabled)
      throw new Error("This runtime does not support Node pool retirement.");
    if (typeof callback !== "function")
      throw new TypeError("Node pool retirement requires a callback.");
    if (this.result !== null)
      throw new Error("Node pool retirement has already started.");
    if (this.callback !== null)
      throw new Error("A Node pool retirement callback is already registered.");
    this.callback = callback;
  }

  retire(input: unknown): Promise<RetirementResult> {
    if (
      typeof input !== "object" ||
      input === null ||
      !("reason" in input) ||
      typeof input.reason !== "string" ||
      !reasons.has(input.reason) ||
      !("remainingMs" in input) ||
      typeof input.remainingMs !== "number" ||
      !Number.isSafeInteger(input.remainingMs) ||
      !Number.isFinite(input.remainingMs) ||
      input.remainingMs < 0 ||
      input.remainingMs > 2_147_483_647
    )
      throw new Error("Invalid Node pool retirement request.");
    // One promise closes registration before invoking application code. Repeated
    // requests join the same outcome and cannot restart the deadline or callback.
    if (this.result !== null) return this.result;
    const reason = input.reason as NodePoolRetirementReason;
    const remainingMs = input.remainingMs;
    this.result = Promise.resolve().then(async () => {
      if (this.callback === null) return { type: "not_registered" };
      const controller = new AbortController();
      const deadlineAt = performance.now() + remainingMs;
      const deadline: NodePoolRetirementDeadline = Object.freeze({
        remainingMs: () => Math.max(0, deadlineAt - performance.now()),
        signal: controller.signal,
      });
      if (remainingMs === 0) {
        controller.abort();
        return { type: "timeout" };
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          Promise.resolve()
            .then(() => this.callback!({ reason, deadline }))
            .then(
              (): RetirementResult => ({ type: "completed" }),
              (): RetirementResult => ({ type: "callback_error" }),
            ),
          new Promise<RetirementResult>((resolve) => {
            timer = setTimeout(() => {
              controller.abort();
              resolve({ type: "timeout" });
            }, remainingMs);
          }),
        ]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    });
    return this.result;
  }
}

export const nodePoolRetirement = new NodePoolRetirement();
