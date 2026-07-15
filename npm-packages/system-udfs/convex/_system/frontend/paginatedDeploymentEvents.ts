import {
  paginationOptsValidator,
  PaginationOptions,
  IndexRangeBuilder,
} from "convex/server";
import { queryPrivateSystem } from "../secretSystemTables";
import { Infer, v } from "convex/values";
import { maximumBytesRead, maximumRowsRead } from "../paginationLimits";
import { DatabaseReader } from "../../_generated/server";
import { Doc } from "../../_generated/dataModel";
import { getTableId } from "./indexes";

const deploymentEventFilters = v.object({
  minDate: v.number(),
  maxDate: v.optional(v.number()),
  authorMemberIds: v.optional(v.array(v.int64())),
  actions: v.optional(v.array(v.string())),
});

/** Paginated deployment events, from most recent to least recent. */
export default queryPrivateSystem("ViewAuditLog")({
  args: {
    paginationOpts: paginationOptsValidator,
    filters: deploymentEventFilters,
  },
  handler: async function ({ db }, { paginationOpts, filters }) {
    return paginateDeploymentEvents(db, paginationOpts, filters);
  },
});

export const listDeploymentMarkers = queryPrivateSystem("ViewAuditLog")({
  args: {
    paginationOpts: paginationOptsValidator,
    minDate: v.number(),
    action: v.union(
      v.literal("push_config"),
      v.literal("push_config_with_components"),
    ),
  },
  handler: async function ({ db }, { paginationOpts, minDate, action }) {
    const results = await paginateDeploymentEvents(db, paginationOpts, {
      minDate,
      actions: [action],
    });
    // Charts only need deployment times, not schema or component diffs.
    return {
      ...results,
      page: results.page.map((event) => event._creationTime),
    };
  },
});

async function paginateDeploymentEvents(
  db: DatabaseReader,
  paginationOpts: PaginationOptions,
  filters: Infer<typeof deploymentEventFilters>,
) {
  const minRetainedDate = await clampForAuditLogRetention(db, filters.minDate);
  // Unknown action strings are valid filter inputs; index equality matches no rows.
  const singleAction =
    filters.actions?.length === 1
      ? (filters.actions[0] as Doc<"_deployment_audit_log">["action"])
      : undefined;
  const singleMember =
    filters.authorMemberIds?.length === 1
      ? filters.authorMemberIds[0]
      : undefined;

  const candidateIndexes: (
    | "by_action_and_member_id_and_creation_time"
    | "by_action_and_creation_time"
    | "by_member_id_and_creation_time"
  )[] = [];
  if (singleAction !== undefined && singleMember !== undefined) {
    candidateIndexes.push("by_action_and_member_id_and_creation_time");
  }
  if (singleAction !== undefined) {
    candidateIndexes.push("by_action_and_creation_time");
  }
  if (singleMember !== undefined) {
    candidateIndexes.push("by_member_id_and_creation_time");
  }
  let indexName: "by_creation_time" | (typeof candidateIndexes)[number] =
    "by_creation_time";
  if (candidateIndexes.length > 0) {
    const tableId = await getTableId(db, "_deployment_audit_log", null);
    if (tableId === undefined) {
      throw new Error("Deployment audit log table is missing");
    }
    const indexes = await db
      .query("_index")
      // The index metadata table has no creation-time index.
      .withIndex("by_id")
      // eslint-disable-next-line @convex-dev/no-filter-in-query -- index metadata has no table/descriptor index
      .filter((q) =>
        q.and(
          q.eq(q.field("table_id"), tableId),
          q.or(
            ...candidateIndexes.map((descriptor) =>
              q.eq(q.field("descriptor"), descriptor),
            ),
          ),
        ),
      )
      .collect();
    for (const descriptor of candidateIndexes) {
      const matches = indexes.filter(
        (entry) => entry.descriptor === descriptor,
      );
      if (matches.length !== 1) {
        throw new Error("Deployment audit log index is missing or duplicated");
      }
      const index = matches[0];
      if (index.config.type !== "database") {
        throw new Error("Deployment audit log index is missing or invalid");
      }
      const state = index.config.onDiskState;
      if (state.type === "Enabled") {
        indexName = descriptor;
        break;
      }
      if (state.type !== "Backfilling") {
        throw new Error("Deployment audit log index has an unexpected state");
      }
      // Upgrades backfill system indexes asynchronously. Keep reading through
      // a completed index until a more specific one becomes available.
    }
  }

  const withDateBounds = (
    q: Pick<
      IndexRangeBuilder<Doc<"_deployment_audit_log">, ["_creationTime"]>,
      "gte"
    >,
  ) => {
    // Cursor fingerprints must retain the caller's bounds. A moving retention cutoff
    // makes split and continuation requests reject the previous page's cursors.
    const partial = q.gte("_creationTime", filters.minDate);
    return filters.maxDate !== undefined
      ? partial.lte("_creationTime", filters.maxDate)
      : partial;
  };
  const query = db.query("_deployment_audit_log");
  const indexedQuery =
    indexName === "by_action_and_member_id_and_creation_time" &&
    singleAction !== undefined &&
    singleMember !== undefined
      ? query.withIndex("by_action_and_member_id_and_creation_time", (q) =>
          withDateBounds(
            q.eq("action", singleAction).eq("member_id", singleMember),
          ),
        )
      : indexName === "by_action_and_creation_time" &&
        singleAction !== undefined
      ? query.withIndex("by_action_and_creation_time", (q) =>
          withDateBounds(q.eq("action", singleAction)),
        )
      : indexName === "by_member_id_and_creation_time" &&
        singleMember !== undefined
      ? query.withIndex("by_member_id_and_creation_time", (q) =>
          withDateBounds(q.eq("member_id", singleMember)),
        )
      : query.withIndex("by_creation_time", withDateBounds);

  const paginatedResults = await indexedQuery
    .order("desc")
    // eslint-disable-next-line @convex-dev/no-filter-in-query -- remaining filters can contain multiple member IDs/actions
    .filter((q) => {
      const queryFilters = [];
      if (
        filters.authorMemberIds !== undefined &&
        indexName !== "by_member_id_and_creation_time" &&
        indexName !== "by_action_and_member_id_and_creation_time"
      ) {
        queryFilters.push(
          q.or(
            ...filters.authorMemberIds.map((memberId) =>
              q.eq(memberId, q.field("member_id")),
            ),
          ),
        );
      }
      if (
        filters.actions !== undefined &&
        indexName !== "by_action_and_creation_time" &&
        indexName !== "by_action_and_member_id_and_creation_time"
      ) {
        queryFilters.push(
          q.or(
            ...filters.actions.map((action) => q.eq(action, q.field("action"))),
          ),
        );
      }
      return q.and(...queryFilters);
    })
    .paginate({
      ...paginationOpts,
      maximumBytesRead,
      maximumRowsRead,
    });

  const page = paginatedResults.page.filter(
    (event) => event._creationTime >= minRetainedDate,
  );
  // Descending pages cannot contain eligible rows after reaching expired history.
  // Do not split or load more pages solely to read records outside retention.
  const retentionEnded = page.length < paginatedResults.page.length;
  return {
    ...paginatedResults,
    page,
    ...(retentionEnded
      ? { isDone: true, splitCursor: null, pageStatus: null }
      : {}),
  };
}

export async function clampForAuditLogRetention(
  db: DatabaseReader,
  minDate: number,
) {
  const backendInfo = await db.query("_backend_info").first();
  const auditLogRetentionDays = Number(backendInfo?.auditLogRetentionDays || 0);
  // no limit if auditLogRetentionDays is -1
  if (auditLogRetentionDays === -1) {
    return minDate;
  }
  const minAllowable =
    Date.now() - (auditLogRetentionDays + 1) * 24 * 60 * 60 * 1000;
  if (minDate < minAllowable) {
    return minAllowable;
  }
  return minDate;
}
