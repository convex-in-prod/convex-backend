import { z } from "zod";
import {
  acquireSourcePackage,
  SourcePackage,
  SourcePackageLease,
} from "./source_package";

const packageSchema = z
  .object({
    uri: z.string(),
    key: z.string(),
    sha256: z.string(),
  })
  .strict();

const sourcePackageSchema = z
  .object({
    uri: z.string(),
    key: z.string(),
    sha256: z.string(),
    bundled_source: packageSchema,
    external_deps: packageSchema.nullish(),
  })
  .strict();

const preparationRequestSchema = z
  .object({
    sourcePackage: sourcePackageSchema,
    mode: z.enum(["warm", "resident"]),
  })
  .strict();

export type PreparationResult = {
  residency: "released" | "retained";
  result: "initialized" | "joined" | "reused";
};

type ResidentPreparationState =
  | { type: "uninitialized" }
  | {
      type: "initializing";
      identity: string;
      lease: Promise<SourcePackageLease>;
    }
  | {
      type: "ready";
      identity: string;
      lease: SourcePackageLease;
    };

let residentPreparation: ResidentPreparationState = { type: "uninitialized" };

function currentResidentPreparation(): ResidentPreparationState {
  return residentPreparation;
}

function residentPackageIdentity(sourcePackage: SourcePackage): string {
  return JSON.stringify([
    sourcePackage.bundled_source.key,
    sourcePackage.bundled_source.sha256,
    sourcePackage.external_deps?.key ?? null,
    sourcePackage.external_deps?.sha256 ?? null,
  ]);
}

async function retainResidentSourcePackage(
  sourcePackage: SourcePackage,
): Promise<PreparationResult> {
  const identity = residentPackageIdentity(sourcePackage);
  if (residentPreparation.type === "ready") {
    if (residentPreparation.identity !== identity) {
      throw new Error("Resident source package identity does not match");
    }
    return { residency: "retained", result: "reused" };
  }
  if (residentPreparation.type === "initializing") {
    if (residentPreparation.identity !== identity) {
      throw new Error("Resident source package identity does not match");
    }
    const initialization = residentPreparation;
    await initialization.lease;
    const completed = currentResidentPreparation();
    if (completed.type !== "ready" || completed.identity !== identity) {
      throw new Error("Resident source package initialization lost ownership");
    }
    return { residency: "retained", result: "joined" };
  }

  const initialization: ResidentPreparationState & { type: "initializing" } = {
    type: "initializing",
    identity,
    lease: acquireSourcePackage(sourcePackage, "resident", "prepare"),
  };
  // Install the shared initialization before awaiting package work so every
  // same-identity request converges on one permanent generation owner.
  residentPreparation = initialization;
  try {
    const lease = await initialization.lease;
    if (residentPreparation !== initialization) {
      await lease.release();
      throw new Error("Resident source package initialization lost ownership");
    }
    residentPreparation = { type: "ready", identity, lease };
    return { residency: "retained", result: "initialized" };
  } catch (error) {
    if (residentPreparation === initialization) {
      residentPreparation = { type: "uninitialized" };
    }
    throw error;
  }
}

export async function prepareSourcePackage(
  request: unknown,
): Promise<PreparationResult> {
  const { sourcePackage, mode } = preparationRequestSchema.parse(request);
  if (mode === "resident") {
    return await retainResidentSourcePackage(sourcePackage);
  }
  const lease = await acquireSourcePackage(sourcePackage, "request", "prepare");
  await lease.release();
  return { residency: "released", result: "initialized" };
}

export async function resetResidentSourcePackageForTests(): Promise<void> {
  if (residentPreparation.type === "initializing") {
    throw new Error("Cannot reset resident package during initialization");
  }
  if (residentPreparation.type === "ready") {
    const lease = residentPreparation.lease;
    residentPreparation = { type: "uninitialized" };
    await lease.release();
  }
}
