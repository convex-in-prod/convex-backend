/** Reasons for a healthy local Node process generation to stop resident work. */
export type NodePoolRetirementReason =
  | "generation_age"
  | "rss_limit"
  | "package_limit"
  | "memory_pressure"
  | "source_change"
  | "topology_change"
  | "shutdown"
  | "request_failure";

/**
 * One fixed process-local monotonic deadline. The supervisor independently
 * enforces its earlier deadline, including IPC delivery time. Neither repeated
 * reads nor repeated retirement requests extend the allowed cleanup time.
 */
export type NodePoolRetirementDeadline = Readonly<{
  remainingMs: () => number;
  signal: AbortSignal;
}>;

/** Resolving declares that the resident has stopped and released its resources. */
export type NodePoolRetirementHandler = (input: {
  reason: NodePoolRetirementReason;
  deadline: NodePoolRetirementDeadline;
}) => Promise<void>;

declare const Convex: {
  onNodePoolRetirement?: (callback: NodePoolRetirementHandler) => void;
};

/**
 * Register one resident cleanup callback before starting detached work in a
 * local Node action. The callback belongs to the process, outlives the action,
 * and uses independently owned clients, not the returned action context.
 * A second registration or registration after retirement starts is rejected.
 * Emergency termination can bypass cleanup; durable fencing remains required.
 */
export function onNodePoolRetirement(
  callback: NodePoolRetirementHandler,
): void {
  if (typeof Convex === "undefined" || !Convex.onNodePoolRetirement) {
    const processGlobal = (
      globalThis as {
        process?: { env?: Record<string, string | undefined> };
      }
    ).process;
    // Vitest loads action modules outside the Node executor. Keep that one
    // explicit test runtime injectable without letting a production runtime
    // silently omit the resident-safety contract.
    if (processGlobal?.env?.VITEST === "true") return;
    throw new Error("This runtime does not support Node pool retirement.");
  }
  Convex.onNodePoolRetirement(callback);
}
