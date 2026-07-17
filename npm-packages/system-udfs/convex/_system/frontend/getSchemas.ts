import type { DatabaseReader } from "../../_generated/server";
import type { Doc } from "../../_generated/dataModel";
import { queryPrivateSystem } from "../secretSystemTables";
import { v } from "convex/values";

const maxSafeInteger = BigInt(Number.MAX_SAFE_INTEGER);

type UniqueSchemaState = "pending" | "validated" | "active";

export const getSchemaByState = (
  db: DatabaseReader,
  state: UniqueSchemaState,
) =>
  db
    .query("_schemas")
    .withIndex("by_state", (q) => q.eq("state", { state }))
    .unique();

export default queryPrivateSystem("ViewData")({
  args: { componentId: v.optional(v.union(v.string(), v.null())) },
  handler: async function ({ db }): Promise<{
    active?: string;
    inProgress?: string;
  }> {
    const [active, pending, validated] = await Promise.all([
      getSchemaByState(db, "active"),
      getSchemaByState(db, "pending"),
      getSchemaByState(db, "validated"),
    ]);

    if (pending && validated) {
      throw new Error("Unexpectedly found both pending and validated schemas");
    }

    return {
      active: active?.schema,
      inProgress: pending?.schema ?? validated?.schema,
    };
  },
});

export const schemaValidationProgress = queryPrivateSystem("ViewData")({
  args: { componentId: v.optional(v.union(v.string(), v.null())) },
  handler: async function ({
    db,
  }): Promise<{ numDocsValidated: number; totalDocs: number | null } | null> {
    const [pending, validated] = await Promise.all([
      getSchemaByState(db, "pending"),
      getSchemaByState(db, "validated"),
    ]);
    if (pending && validated) {
      throw new Error("Unexpectedly found both pending and validated schemas");
    }
    if (!pending) {
      return null;
    }
    const attempts = await db
      .query("_schema_validations")
      .withIndex("by_schema_id_and_table_name", (q) =>
        q.eq("schemaId", pending._id),
      )
      .collect();
    const rows = await Promise.all(
      attempts.map((attempt) => validationProgress(db, attempt)),
    );
    if (rows.length === 0) {
      const legacy = await db
        .query("_schema_validation_progress")
        .withIndex("by_schema_id", (q) => q.eq("schemaId", pending._id))
        .unique();
      return legacy === null
        ? null
        : normalizeProgress(legacy.numDocsValidated, legacy.totalDocs);
    }
    const normalizedRows = rows.map((row) =>
      normalizeProgress(row.numDocsValidated, row.totalDocs),
    );
    return {
      numDocsValidated: normalizedRows.reduce(
        (sum, row) => sum + Number(row.numDocsValidated),
        0,
      ),
      totalDocs: normalizedRows.every((row) => row.totalDocs !== null)
        ? normalizedRows.reduce((sum, row) => sum + Number(row.totalDocs), 0)
        : null,
    };
  },
});

async function validationProgress(
  db: DatabaseReader,
  attempt: Doc<"_schema_validations">,
) {
  const progress = await db
    .query("_schema_validation_progress")
    .withIndex("by_validation_id", (q) => q.eq("validationId", attempt._id))
    .unique();
  return {
    ...attempt,
    numDocsValidated: progress?.numDocsValidated ?? BigInt(0),
    totalDocs: progress?.totalDocs ?? null,
  };
}

function normalizeProgress(numDocsValidated: bigint, totalDocs: bigint | null) {
  if (
    numDocsValidated < BigInt(0) ||
    (totalDocs !== null && totalDocs < BigInt(0))
  ) {
    throw new Error("Schema validation progress counts must be nonnegative");
  }
  if (
    numDocsValidated > maxSafeInteger ||
    (totalDocs !== null && totalDocs > maxSafeInteger)
  ) {
    throw new Error(
      "Schema validation progress counts exceed the safe integer range",
    );
  }
  return {
    numDocsValidated: Number(numDocsValidated),
    totalDocs: totalDocs === null ? null : Number(totalDocs),
  };
}
