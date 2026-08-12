import { createHash } from "node:crypto";
import * as fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import AdmZip from "adm-zip";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  prepareSourcePackage,
  resetResidentSourcePackageForTests,
} from "./prepare";
import {
  ExecutorEnvironment,
  SystemOperationState,
  analyze,
  validateExecutorRole,
} from "./executor";
import {
  acquireSourcePackage,
  availableExternalPackages,
  availableSourcePackages,
  getPackageCacheStats,
  maybeDownloadAndLinkPackages,
  populatePrebuildPackages,
  recordSourcePackageImport,
  resetPackageCachesForTests,
  SourcePackage,
} from "./source_package";

let tmpdir: string | undefined;

beforeEach(() => {
  tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), "node-executor-test-"));
  vi.spyOn(os, "tmpdir").mockImplementation(() => tmpdir!);
  resetPackageCachesForTests();
});

afterEach(async () => {
  vi.useRealTimers();
  await resetResidentSourcePackageForTests();
  resetPackageCachesForTests();
  vi.restoreAllMocks();
  if (tmpdir !== undefined) {
    await fs.promises.rm(tmpdir, { recursive: true, force: true });
    tmpdir = undefined;
  }
});

test("preparation publishes source and external packages without evaluating modules", async () => {
  const sourceZip = makeSourcePackageZip(
    "external-package-key",
    1,
    "node",
    undefined,
    'throw new Error("application module was evaluated during preparation");\n',
  );
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    await prepareSourcePackage({ sourcePackage, mode: "warm" });

    expect(availableSourcePackages.has("source-package-key")).toBe(true);
    expect(availableExternalPackages.has("external-package-key")).toBe(true);
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
    expect(getPackageCacheStats()).toMatchObject({
      importedSourcePackages: 0,
      activeSourceOwners: 0,
    });
  } finally {
    await server.close();
  }
});

test("preparation rejects extra request and package fields before downloading", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    await expect(
      prepareSourcePackage({
        sourcePackage,
        mode: "warm",
        udfPath: "actions/example.js",
      }),
    ).rejects.toThrow();
    await expect(
      prepareSourcePackage({
        sourcePackage: {
          ...sourcePackage,
          bundled_source: {
            ...sourcePackage.bundled_source,
            arguments: [],
          },
        },
        mode: "warm",
      }),
    ).rejects.toThrow();

    expect(server.requestCounts.size).toBe(0);
  } finally {
    await server.close();
  }
});

test("concurrent package requests share one atomic source and external download", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    const locals = await Promise.all(
      Array.from({ length: 16 }, () =>
        maybeDownloadAndLinkPackages(sourcePackage),
      ),
    );

    expect(new Set(locals.map((local) => local.dir))).toHaveLength(1);
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);

    const local = locals[0];
    expect(fs.statSync(path.join(local.dir, "modules")).isDirectory()).toBe(
      true,
    );
    expect(
      fs.statSync(path.join(local.dir, "modules/actions/example.js")).isFile(),
    ).toBe(true);
    expect(
      fs.lstatSync(path.join(local.dir, "node_modules")).isSymbolicLink(),
    ).toBe(true);

    await Promise.all(
      Array.from({ length: 16 }, () =>
        maybeDownloadAndLinkPackages(sourcePackage),
      ),
    );
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
  } finally {
    await server.close();
  }
});

test.each([1, 32])(
  "warm source lease acquisition performs no completeness stats for %i modules",
  async (moduleCount) => {
    const sourceZip = makeSourceOnlyPackageZip(moduleCount);
    const server = await startPackageServer({ "/source.zip": sourceZip });
    try {
      const sourcePackage = makeSourceOnlyPackage(
        `${server.baseUrl}/source.zip`,
        sha256(sourceZip),
      );
      const initialLease = await acquireSourcePackage(sourcePackage);
      await initialLease.release();
      const validationCount =
        getPackageCacheStats().packageStageObservations.filter(
          (observation) => observation.stage === "validation",
        ).length;
      const stat = vi.spyOn(fs.promises, "stat");
      stat.mockClear();

      const lease = await acquireSourcePackage(sourcePackage);
      try {
        expect(stat).not.toHaveBeenCalled();
        const stats = getPackageCacheStats();
        expect(
          stats.packageStageObservations.filter(
            (observation) => observation.stage === "validation",
          ),
        ).toHaveLength(validationCount);
        expect(
          stats.packageStageObservations.filter(
            (observation) => observation.stage === "acquire",
          ),
        ).toContainEqual(
          expect.objectContaining({
            cacheResult: "current_hit",
            outcome: "success",
          }),
        );
      } finally {
        await lease.release();
      }
    } finally {
      await server.close();
    }
  },
);

test("acquisition timing retains work across an ownership retry", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const server = await startPackageServer({
    "/source.zip": { body: sourceZip, delayMs: 100 },
  });
  const sourcePackage = makeSourceOnlyPackage(
    `${server.baseUrl}/source.zip`,
    sha256(sourceZip),
  );
  const realGet = availableSourcePackages.get.bind(availableSourcePackages);
  let hidPublishedPackage = false;
  const get = vi
    .spyOn(availableSourcePackages, "get")
    .mockImplementation((key) => {
      const localPackage = realGet(key);
      if (
        key === sourcePackage.bundled_source.key &&
        localPackage !== undefined &&
        !hidPublishedPackage
      ) {
        // Force the post-materialization ownership check to retry once. The
        // separate cache-pressure test exercises the real retirement race.
        hidPublishedPackage = true;
        return undefined;
      }
      return localPackage;
    });

  try {
    const lease = await acquireSourcePackage(sourcePackage);
    get.mockRestore();
    try {
      expect(hidPublishedPackage).toBe(true);
      const firstSnapshot = getPackageCacheStats();
      const acquisition = firstSnapshot.packageStageObservations.find(
        (observation) => observation.stage === "acquire",
      );
      expect(acquisition).toMatchObject({
        cacheResult: "new_materialization",
        outcome: "success",
      });
      if (acquisition === undefined) {
        throw new Error("Expected an acquisition observation");
      }
      const durationMs = acquisition.durationMs;
      acquisition.durationMs = -1;
      expect(
        getPackageCacheStats().packageStageObservations.find(
          (observation) => observation.sequence === acquisition.sequence,
        )?.durationMs,
      ).toBe(durationMs);
    } finally {
      await lease.release();
    }
  } finally {
    get.mockRestore();
    await server.close();
  }
});

test("acquisition observations record failures once and remain bounded", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const server = await startPackageServer({
    "/failed-source.zip": { status: 500 },
    "/source.zip": sourceZip,
  });

  try {
    await expect(
      acquireSourcePackage(
        makeSourceOnlyPackage(
          `${server.baseUrl}/failed-source.zip`,
          sha256(sourceZip),
          "failed-source-package",
        ),
        "request",
        "analyze",
      ),
    ).rejects.toThrow("Failed to fetch package: HTTP 500");
    expect(
      getPackageCacheStats().packageStageObservations.filter(
        (observation) => observation.stage === "acquire",
      ),
    ).toEqual([
      expect.objectContaining({
        requestKind: "analyze",
        cacheResult: "new_materialization",
        outcome: "failure",
      }),
    ]);

    const sourcePackage = makeSourceOnlyPackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
    );
    for (let index = 0; index < 260; index += 1) {
      const lease = await acquireSourcePackage(sourcePackage);
      await lease.release();
    }

    const boundedStats = getPackageCacheStats();
    expect(boundedStats.packageStageObservationSequence).toBeGreaterThan(256);
    expect(boundedStats.packageStageObservations).toHaveLength(256);
    expect(boundedStats.packageStageObservationsDropped).toBe(
      boundedStats.packageStageObservationSequence - 256,
    );
    expect(boundedStats.packageStageObservations[0]?.sequence).toBe(
      boundedStats.packageStageObservationsDropped + 1,
    );
    expect(
      boundedStats.packageStageObservations[
        boundedStats.packageStageObservations.length - 1
      ]?.sequence,
    ).toBe(boundedStats.packageStageObservationSequence);

    resetPackageCachesForTests();
    expect(getPackageCacheStats()).toMatchObject({
      packageStageObservationSequence: 0,
      packageStageObservationsDropped: 0,
      packageStageObservations: [],
    });
  } finally {
    await server.close();
  }
});

test("resident preparation retains one package owner and reuses identity", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });
  const sourcePackage = makeSourcePackage(
    `${server.baseUrl}/source.zip`,
    sha256(sourceZip),
    `${server.baseUrl}/external.zip`,
    sha256(externalZip),
  );
  let serverClosed = false;
  try {
    const results = await Promise.all(
      Array.from({ length: 16 }, () =>
        prepareSourcePackage({ sourcePackage, mode: "resident" }),
      ),
    );
    expect(
      results.filter((result) => result.result === "initialized"),
    ).toHaveLength(1);
    expect(results.filter((result) => result.result === "joined")).toHaveLength(
      15,
    );
    expect(getPackageCacheStats()).toMatchObject({
      activeSourceOwners: 0,
      residentSourceOwners: 1,
      retainedExternalPackages: 1,
    });

    await server.close();
    serverClosed = true;
    await expect(
      prepareSourcePackage({
        sourcePackage: {
          ...sourcePackage,
          uri: "https://packages.invalid/expired.zip",
          bundled_source: {
            ...sourcePackage.bundled_source,
            uri: "https://packages.invalid/expired.zip",
          },
          external_deps: {
            ...sourcePackage.external_deps!,
            uri: "https://packages.invalid/expired-external.zip",
          },
        },
        mode: "resident",
      }),
    ).resolves.toEqual({ residency: "retained", result: "reused" });
    await expect(
      prepareSourcePackage({
        sourcePackage: {
          ...sourcePackage,
          bundled_source: {
            ...sourcePackage.bundled_source,
            sha256: "different",
          },
        },
        mode: "resident",
      }),
    ).rejects.toThrow("Resident source package identity does not match");
    await expect(
      prepareSourcePackage({
        sourcePackage: {
          ...sourcePackage,
          external_deps: {
            ...sourcePackage.external_deps!,
            sha256: "different",
          },
        },
        mode: "resident",
      }),
    ).rejects.toThrow("Resident source package identity does not match");
  } finally {
    if (!serverClosed) {
      await server.close();
    }
  }
});

test("failed resident initialization releases ownership and permits retry", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const server = await startPackageServer({ "/source.zip": sourceZip });
  try {
    const sourcePackage = makeSourceOnlyPackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
    );
    await expect(
      prepareSourcePackage({
        sourcePackage: {
          ...sourcePackage,
          uri: `${server.baseUrl}/missing.zip`,
          bundled_source: {
            ...sourcePackage.bundled_source,
            uri: `${server.baseUrl}/missing.zip`,
          },
        },
        mode: "resident",
      }),
    ).rejects.toThrow("Failed to fetch package");
    expect(getPackageCacheStats().residentSourceOwners).toBe(0);

    await expect(
      prepareSourcePackage({ sourcePackage, mode: "resident" }),
    ).resolves.toEqual({ residency: "retained", result: "initialized" });
    expect(getPackageCacheStats().residentSourceOwners).toBe(1);
  } finally {
    await server.close();
  }
});

test("local executor roles reject requests from the other role", () => {
  expect(() => validateExecutorRole("system", "execute", false)).toThrow(
    "System Node executor cannot execute application actions",
  );
  expect(() => validateExecutorRole("application", "analyze", false)).toThrow(
    "Application Node executor cannot run analysis",
  );
  expect(() =>
    validateExecutorRole("application", "build_deps", false),
  ).toThrow("Application Node executor cannot build dependencies");
  expect(() =>
    validateExecutorRole("application", "execute", false),
  ).not.toThrow();
  expect(() => validateExecutorRole("system", "analyze", false)).not.toThrow();
  expect(() =>
    validateExecutorRole("system", "build_deps", false),
  ).not.toThrow();
});

test("system analyses restore the captured baseline environment", async () => {
  const analysisValue = "CONVEX_NODE_EXECUTOR_ANALYSIS_ENV_TEST";
  const analysisLeak = "CONVEX_NODE_EXECUTOR_ANALYSIS_LEAK_TEST";
  const target: { environment: NodeJS.ProcessEnv } = {
    environment: { PATH: "/bin" },
  };
  const environment = new ExecutorEnvironment(target);
  await environment.runWithEnvironmentVariables(
    [{ name: analysisValue, value: "first" }],
    async () => {
      expect(target.environment[analysisValue]).toBe("first");
      target.environment[analysisLeak] = "leaked";
    },
    true,
    false,
  );

  await environment.runWithEnvironmentVariables(
    [{ name: analysisValue, value: "second" }],
    async () => {
      expect(target.environment[analysisValue]).toBe("second");
      expect(target.environment[analysisLeak]).toBeUndefined();
    },
    true,
    false,
  );
  expect(target.environment[analysisValue]).toBeUndefined();
  expect(target.environment[analysisLeak]).toBeUndefined();
});

test("system analyses apply environment variables to awaited module imports", async () => {
  const environmentName = `CONVEX_NODE_EXECUTOR_ANALYSIS_REAL_ENV_${process.pid}`;
  const expectedName = `${environmentName}_EXPECTED`;
  const leakName = `${environmentName}_LEAK`;
  const originalValues = new Map(
    [environmentName, expectedName, leakName].map((name) => [
      name,
      process.env[name],
    ]),
  );
  delete process.env[environmentName];
  delete process.env[expectedName];
  delete process.env[leakName];

  const sourceZip = makeSourcePackageZip(
    null,
    1,
    "node",
    undefined,
    `
      await Promise.resolve();
      if (process.env[${JSON.stringify(environmentName)}] !== process.env[${JSON.stringify(expectedName)}]) {
        throw new Error("analysis environment was not installed");
      }
      if (process.env[${JSON.stringify(leakName)}] !== undefined) {
        throw new Error("analysis environment leaked from a prior import");
      }
      process.env[${JSON.stringify(leakName)}] = "module-local";
      export const value = 1;
    `,
  );
  const server = await startPackageServer({ "/source.zip": sourceZip });

  try {
    const sourcePackage = makeSourceOnlyPackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
    );
    for (const value of ["first", "second"]) {
      const result = await analyze(
        {
          type: "analyze",
          requestId: `analysis-${value}`,
          sourcePackage,
          environmentVariables: [
            { name: environmentName, value },
            { name: expectedName, value },
          ],
        },
        true,
      );
      expect(result.type).toBe("success");
    }
    expect(process.env[environmentName]).toBeUndefined();
    expect(process.env[expectedName]).toBeUndefined();
    expect(process.env[leakName]).toBeUndefined();
  } finally {
    await server.close();
    for (const [name, value] of originalValues) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
  }
});

test("dependency builds run from the captured baseline environment", async () => {
  const buildValue = "CONVEX_NODE_EXECUTOR_BUILD_ENV_TEST";
  const target: { environment: NodeJS.ProcessEnv } = {
    environment: { PATH: "/bin" },
  };
  const environment = new ExecutorEnvironment(target);
  target.environment[buildValue] = "contaminated";

  await environment.runWithBaselineEnvironment(async () => {
    expect(target.environment[buildValue]).toBeUndefined();
    target.environment[buildValue] = "build-local";
  });

  expect(target.environment[buildValue]).toBeUndefined();
});

test("system child defensively rejects overlapping operations", () => {
  const operations = new SystemOperationState();
  const release = operations.tryAcquire();
  expect(release).not.toBeNull();
  expect(operations.tryAcquire()).toBeNull();

  release?.();
  expect(operations.tryAcquire()).not.toBeNull();
});

test("current external cache hits do not stat the published dependency path", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source-a.zip": sourceZip,
    "/source-b.zip": sourceZip,
    "/external.zip": externalZip,
  });
  try {
    await maybeDownloadAndLinkPackages(
      makeSourcePackage(
        `${server.baseUrl}/source-a.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external.zip`,
        sha256(externalZip),
        "source-a",
        "source-a",
      ),
    );
    const external = availableExternalPackages.get("external-package-key");
    if (external === undefined) {
      throw new Error("Expected external package to be cached");
    }
    const stat = vi.spyOn(fs.promises, "stat");
    stat.mockClear();

    await maybeDownloadAndLinkPackages(
      makeSourcePackage(
        `${server.baseUrl}/source-b.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external.zip`,
        sha256(externalZip),
        "source-b",
        "source-b",
      ),
    );

    expect(
      stat.mock.calls.some(([filePath]) =>
        String(filePath).startsWith(external.dir),
      ),
    ).toBe(false);
  } finally {
    await server.close();
  }
});

test("pooled Node environment markers load as Node modules", async () => {
  const sourceZip = makeSourcePackageZip(null, 1, "node:pool:consumer");
  const server = await startPackageServer({
    "/source.zip": sourceZip,
  });

  try {
    const local = await maybeDownloadAndLinkPackages(
      makeSourceOnlyPackage(
        `${server.baseUrl}/source.zip`,
        sha256(sourceZip),
        "pooled-source-package",
      ),
    );

    expect(local.modules).toContain("actions/example.js");
  } finally {
    await server.close();
  }
});

test("source package metadata rejects duplicate module environments", async () => {
  const sourceZip = makeSourcePackageZip(null, 1, "node", [
    ["_deps/chunk.js", "node"],
    ["actions/example.js", "node"],
    ["actions/example.js", "isolate"],
  ]);
  const server = await startPackageServer({
    "/source.zip": sourceZip,
  });

  try {
    await expect(
      maybeDownloadAndLinkPackages(
        makeSourceOnlyPackage(
          `${server.baseUrl}/source.zip`,
          sha256(sourceZip),
          "duplicate-environment-source-package",
        ),
      ),
    ).rejects.toThrow(
      "Source package metadata contains duplicate module environments",
    );
  } finally {
    await server.close();
  }
});

test("source package metadata rejects orphan source maps", async () => {
  const zip = new AdmZip();
  zip.addFile(
    "metadata.json",
    Buffer.from(JSON.stringify({ modulePaths: ["orphan.js.map"] })),
  );
  zip.addFile("modules/orphan.js.map", Buffer.from("{}"));
  const sourceZip = zip.toBuffer();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
  });

  try {
    await expect(
      maybeDownloadAndLinkPackages(
        makeSourceOnlyPackage(
          `${server.baseUrl}/source.zip`,
          sha256(sourceZip),
          "orphan-source-map-package",
        ),
      ),
    ).rejects.toThrow(
      "Source package metadata contains a source map for a missing module",
    );
  } finally {
    await server.close();
  }
});

test("prebuild initialization clears package roots from a prior runtime", async () => {
  const sourceRoot = path.join(tmpdir!, "source");
  const externalRoot = path.join(tmpdir!, "external_deps");
  const buildRoot = path.join(tmpdir!, "build_deps");
  await Promise.all([
    fs.promises.mkdir(path.join(sourceRoot, "stale-source"), {
      recursive: true,
    }),
    fs.promises.mkdir(path.join(externalRoot, "stale-external"), {
      recursive: true,
    }),
    fs.promises.mkdir(path.join(buildRoot, "stale-build"), {
      recursive: true,
    }),
  ]);

  await populatePrebuildPackages();

  expect(fs.existsSync(sourceRoot)).toBe(false);
  expect(fs.existsSync(externalRoot)).toBe(false);
  expect(fs.existsSync(buildRoot)).toBe(false);
});

test("Lambda reset cleanup waits for every selected removal", async () => {
  const sourceRoot = path.join(tmpdir!, "source");
  const externalRoot = path.join(tmpdir!, "external_deps");
  const buildRoot = path.join(tmpdir!, "build_deps");
  await Promise.all([
    fs.promises.mkdir(sourceRoot),
    fs.promises.mkdir(externalRoot),
    fs.promises.mkdir(buildRoot),
  ]);

  const realRm = fs.promises.rm;
  let releaseExternalRemoval!: () => void;
  const externalRemovalCanFinish = new Promise<void>((resolve) => {
    releaseExternalRemoval = resolve;
  });
  const rm = vi
    .spyOn(fs.promises, "rm")
    .mockImplementation(async (...args: Parameters<typeof fs.promises.rm>) => {
      const [target] = args;
      if (target === sourceRoot) {
        throw new Error("simulated source cleanup failure");
      }
      if (target === externalRoot) {
        await externalRemovalCanFinish;
      }
      await realRm(...args);
    });

  let settled = false;
  const cleanupResult = populatePrebuildPackages().then(
    () => {
      settled = true;
      return null;
    },
    (error: unknown) => {
      settled = true;
      return error;
    },
  );
  await waitFor(() => rm.mock.calls.length === 3);
  await sleep(10);
  expect(settled).toBe(false);

  releaseExternalRemoval();
  const error = await cleanupResult;
  expect(error).toBeInstanceOf(Error);
  expect((error as Error).message).toBe(
    "Failed to clear package caches during Lambda initialization",
  );
  expect(fs.existsSync(externalRoot)).toBe(false);
  expect(fs.existsSync(buildRoot)).toBe(false);
});

test("local cache bounds preserve active source packages and retire released packages", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const routes = Object.fromEntries(
    Array.from({ length: 18 }, (_, index) => [
      `/source-${index}.zip`,
      sourceZip,
    ]),
  );
  const server = await startPackageServer(routes);

  try {
    const sourcePackage = (index: number) =>
      makeSourceOnlyPackage(
        `${server.baseUrl}/source-${index}.zip`,
        sha256(sourceZip),
        `source-package-${index}`,
      );
    const activeLease = await acquireSourcePackage(sourcePackage(0));
    const activeModule = path.join(
      activeLease.package.dir,
      "modules/actions/example.js",
    );

    for (let index = 1; index <= 9; index += 1) {
      const lease = await acquireSourcePackage(sourcePackage(index));
      await lease.release();
    }

    expect(fs.statSync(activeModule).isFile()).toBe(true);
    expect(availableSourcePackages.has("source-package-0")).toBe(true);
    expect(getPackageCacheStats().retainedSourcePackages).toBeLessThanOrEqual(
      8,
    );

    await activeLease.release();
    for (let index = 10; index < 18; index += 1) {
      const lease = await acquireSourcePackage(sourcePackage(index));
      await lease.release();
    }

    expect(availableSourcePackages.has("source-package-0")).toBe(false);
    expect(fs.existsSync(activeLease.package.dir)).toBe(false);
    expect(getPackageCacheStats()).toMatchObject({
      retainedSourcePackages: 8,
      activeSourceOwners: 0,
      sourceRetirements: 10,
    });
  } finally {
    await server.close();
  }
});

test("resident ownership protects source and external packages under pressure", async () => {
  const externalZip = makeExternalDepsZip();
  const sourceZips = Array.from({ length: 18 }, (_, index) =>
    makeSourcePackageZip(`external-package-${index}`),
  );
  const routes: Record<string, Route> = {};
  for (let index = 0; index < sourceZips.length; index += 1) {
    routes[`/source-${index}.zip`] = sourceZips[index];
    routes[`/external-${index}.zip`] = externalZip;
  }
  const server = await startPackageServer(routes);
  const sourcePackage = (index: number) =>
    makeSourcePackage(
      `${server.baseUrl}/source-${index}.zip`,
      sha256(sourceZips[index]),
      `${server.baseUrl}/external-${index}.zip`,
      sha256(externalZip),
      `deprecated-wrapper-key-${index}`,
      `source-package-${index}`,
      `external-package-${index}`,
    );

  try {
    const residentLease = await acquireSourcePackage(
      sourcePackage(0),
      "resident",
    );
    let residentReleased = false;
    try {
      const residentSourceDir = residentLease.package.dir;
      const residentExternal = availableExternalPackages.get(
        "external-package-0",
      );
      if (residentExternal === undefined) {
        throw new Error("Expected resident external package to be cached");
      }
      const residentExternalDir = residentExternal.dir;

      for (let index = 1; index <= 9; index += 1) {
        const lease = await acquireSourcePackage(sourcePackage(index));
        await lease.release();
      }

      expect(availableSourcePackages.get("source-package-0")).toBe(
        residentLease.package,
      );
      expect(availableExternalPackages.get("external-package-0")).toBe(
        residentExternal,
      );
      expect(residentExternal.sourceOwners).toBe(1);
      expect(
        fs
          .statSync(path.join(residentSourceDir, "node_modules/example"))
          .isDirectory(),
      ).toBe(true);
      expect(getPackageCacheStats()).toMatchObject({
        retainedSourcePackages: 8,
        retainedExternalPackages: 8,
        residentSourceOwners: 1,
        activeSourceOwners: 0,
      });
      expect(getPackageCacheStats().retainedSourceBytes).toBeGreaterThanOrEqual(
        residentLease.package.retainedBytes,
      );
      expect(
        getPackageCacheStats().retainedExternalBytes,
      ).toBeGreaterThanOrEqual(residentExternal.retainedBytes);

      await residentLease.release();
      residentReleased = true;
      for (let index = 10; index < sourceZips.length; index += 1) {
        const lease = await acquireSourcePackage(sourcePackage(index));
        await lease.release();
      }

      expect(availableSourcePackages.has("source-package-0")).toBe(false);
      expect(availableExternalPackages.has("external-package-0")).toBe(false);
      expect(fs.existsSync(residentSourceDir)).toBe(false);
      expect(fs.existsSync(residentExternalDir)).toBe(false);
    } finally {
      if (!residentReleased) {
        await residentLease.release();
      }
    }
  } finally {
    await server.close();
  }
});

test("imported source package count survives disk cache retirement", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const routes = Object.fromEntries(
    Array.from({ length: 9 }, (_, index) => [
      `/source-${index}.zip`,
      sourceZip,
    ]),
  );
  const server = await startPackageServer(routes);

  try {
    const firstLease = await acquireSourcePackage(
      makeSourceOnlyPackage(
        `${server.baseUrl}/source-0.zip`,
        sha256(sourceZip),
        "source-package-0",
      ),
    );
    expect(getPackageCacheStats().importedSourcePackages).toBe(0);
    recordSourcePackageImport(firstLease.package.dir);
    recordSourcePackageImport(firstLease.package.dir);
    expect(getPackageCacheStats().importedSourcePackages).toBe(1);
    await firstLease.release();

    for (let index = 1; index < 9; index += 1) {
      const lease = await acquireSourcePackage(
        makeSourceOnlyPackage(
          `${server.baseUrl}/source-${index}.zip`,
          sha256(sourceZip),
          `source-package-${index}`,
        ),
      );
      recordSourcePackageImport(lease.package.dir);
      await lease.release();
    }

    expect(getPackageCacheStats()).toMatchObject({
      importedSourcePackages: 9,
      retainedSourcePackages: 8,
    });
  } finally {
    await server.close();
  }
});

test("byte bounds retire oversized source and external packages after release", async () => {
  const sourceZip = makeSourcePackageZip();
  const sourceOnlyZip = makeSourcePackageZip(null);
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/source-only.zip": sourceOnlyZip,
    "/external.zip": externalZip,
  });

  try {
    const sourceOnlyLease = await acquireSourcePackage(
      makeSourceOnlyPackage(
        `${server.baseUrl}/source-only.zip`,
        sha256(sourceOnlyZip),
        "source-byte-package",
      ),
    );
    const sourceOnlyDir = sourceOnlyLease.package.dir;
    sourceOnlyLease.package.retainedBytes = 512 * 1024 * 1024 + 1;
    await sourceOnlyLease.release();
    expect(availableSourcePackages.has("source-byte-package")).toBe(false);
    expect(fs.existsSync(sourceOnlyDir)).toBe(false);

    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );
    const lease = await acquireSourcePackage(sourcePackage);
    const externalPackage = availableExternalPackages.get(
      "external-package-key",
    );
    if (externalPackage === undefined) {
      throw new Error("Expected external package to be cached");
    }
    const sourceDir = lease.package.dir;
    const externalDir = externalPackage.dir;

    // Avoid allocating multi-gigabyte fixtures while exercising the real byte
    // accounting and paired source/external retirement path.
    externalPackage.retainedBytes = 2 * 1024 * 1024 * 1024 + 1;

    expect(fs.existsSync(sourceDir)).toBe(true);
    expect(fs.existsSync(externalDir)).toBe(true);
    await lease.release();

    expect(availableSourcePackages.has("source-package-key")).toBe(false);
    expect(availableExternalPackages.has("external-package-key")).toBe(false);
    expect(fs.existsSync(sourceDir)).toBe(false);
    expect(fs.existsSync(externalDir)).toBe(false);
  } finally {
    await server.close();
  }
});

test("cache enforcement waits for every selected retirement after failure", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const lease = await acquireSourcePackage(
      makeSourcePackage(
        `${server.baseUrl}/source.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external.zip`,
        sha256(externalZip),
      ),
    );
    const externalPackage = availableExternalPackages.get(
      "external-package-key",
    );
    if (externalPackage === undefined) {
      throw new Error("Expected external package to be cached");
    }
    externalPackage.retainedBytes = 2 * 1024 * 1024 * 1024 + 1;

    let finishExternalRemoval!: () => void;
    const externalRemoval = new Promise<void>((resolve) => {
      finishExternalRemoval = resolve;
    });
    let markExternalRemovalStarted!: () => void;
    const externalRemovalStarted = new Promise<void>((resolve) => {
      markExternalRemovalStarted = resolve;
    });
    const realRm = fs.promises.rm;
    vi.spyOn(fs.promises, "rm").mockImplementation(
      async (...args: Parameters<typeof fs.promises.rm>) => {
        const [target] = args;
        if (target === lease.package.dir) {
          throw new Error("simulated source retirement failure");
        }
        if (target === externalPackage.dir) {
          markExternalRemovalStarted();
          await externalRemoval;
        }
        await realRm(...args);
      },
    );

    let releaseSettled = false;
    const releaseResult = lease.release().then(
      () => undefined,
      (error: unknown) => error,
    );
    void releaseResult.then(() => {
      releaseSettled = true;
    });
    await externalRemovalStarted;
    await Promise.resolve();
    const settledBeforeEveryRemoval = releaseSettled;
    finishExternalRemoval();
    const error = await releaseResult;
    expect(settledBeforeEveryRemoval).toBe(false);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe("Failed to clean retired packages");
    expect(fs.existsSync(externalPackage.dir)).toBe(false);
  } finally {
    await server.close();
  }
});

test("lease acquisition retries when a cache hit retires before ownership", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const routes = Object.fromEntries(
    Array.from({ length: 9 }, (_, index) => [
      `/source-${index}.zip`,
      sourceZip,
    ]),
  );
  const server = await startPackageServer(routes);

  try {
    const sourcePackages = Array.from({ length: 9 }, (_, index) =>
      makeSourceOnlyPackage(
        `${server.baseUrl}/source-${index}.zip`,
        sha256(sourceZip),
        `source-package-${index}`,
      ),
    );
    const leases = await Promise.all(
      sourcePackages.map((sourcePackage) =>
        acquireSourcePackage(sourcePackage),
      ),
    );

    // Starting the acquire and releasing the last owner in the same turn forces
    // the cache-hit continuation to race LRU retirement.
    const reacquiredPromise = acquireSourcePackage(sourcePackages[0]);
    await leases[0].release();
    const reacquired = await reacquiredPromise;

    expect(availableSourcePackages.get("source-package-0")).toBe(
      reacquired.package,
    );
    expect(fs.existsSync(reacquired.package.dir)).toBe(true);
    expect(server.requestCounts.get("/source-0.zip")).toBe(2);

    await Promise.all([
      reacquired.release(),
      ...leases.slice(1).map((lease) => lease.release()),
    ]);
  } finally {
    await server.close();
  }
});

test("source publication retries when an external hit retires before ownership", async () => {
  const externalZip = makeExternalDepsZip();
  const linkedSourceZip = makeSourcePackageZip("external-package-key");
  const sourceOnlyZip = makeSourcePackageZip(null);
  const server = await startPackageServer({
    "/failed-source.zip": { status: 500 },
    "/pending-source.zip": linkedSourceZip,
    "/source-only.zip": sourceOnlyZip,
    "/external.zip": externalZip,
  });

  try {
    const failedSource = makeSourcePackage(
      `${server.baseUrl}/failed-source.zip`,
      sha256(linkedSourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "failed-wrapper-key",
      "failed-source-package",
    );
    await expect(maybeDownloadAndLinkPackages(failedSource)).rejects.toThrow(
      "Failed to fetch package",
    );
    const retiredExternal = availableExternalPackages.get(
      "external-package-key",
    );
    if (retiredExternal === undefined) {
      throw new Error("Expected the successful external download to be cached");
    }

    const sourceOnlyLease = await acquireSourcePackage(
      makeSourceOnlyPackage(
        `${server.baseUrl}/source-only.zip`,
        sha256(sourceOnlyZip),
        "source-only-package",
      ),
    );
    // Starting publication and releasing the unrelated lease in the same turn
    // makes cache enforcement retire the hit before its owner can resume.
    retiredExternal.retainedBytes = 2 * 1024 * 1024 * 1024 + 1;
    const pendingPublication = maybeDownloadAndLinkPackages(
      makeSourcePackage(
        `${server.baseUrl}/pending-source.zip`,
        sha256(linkedSourceZip),
        `${server.baseUrl}/external.zip`,
        sha256(externalZip),
        "pending-wrapper-key",
        "pending-source-package",
      ),
    );
    await sourceOnlyLease.release();
    const published = await pendingPublication;

    const replacementExternal = availableExternalPackages.get(
      "external-package-key",
    );
    expect(replacementExternal).not.toBe(retiredExternal);
    expect(replacementExternal?.sourceOwners).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(2);
    expect(
      fs
        .statSync(path.join(published.dir, "node_modules/example"))
        .isDirectory(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

test("source publication owns a cached external package before linking", async () => {
  const externalZip = makeExternalDepsZip();
  const externalKey = "external-package-0";
  const pendingSourceZip = makeSourcePackageZip(externalKey);
  const routes: Record<string, Route> = {
    "/failed-source.zip": { status: 500 },
    "/pending-source.zip": { body: pendingSourceZip, delayMs: 1_000 },
    "/external-0.zip": externalZip,
  };
  const protectedSourceZips = Array.from({ length: 8 }, (_, offset) => {
    const index = offset + 1;
    const sourceZip = makeSourcePackageZip(`external-package-${index}`);
    routes[`/source-${index}.zip`] = sourceZip;
    routes[`/external-${index}.zip`] = externalZip;
    return sourceZip;
  });
  const server = await startPackageServer(routes);

  try {
    const protectedPackages = protectedSourceZips.map((sourceZip, offset) => {
      const index = offset + 1;
      return makeSourcePackage(
        `${server.baseUrl}/source-${index}.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external-${index}.zip`,
        sha256(externalZip),
        `deprecated-wrapper-key-${index}`,
        `source-package-${index}`,
        `external-package-${index}`,
      );
    });
    const failedSource = makeSourcePackage(
      `${server.baseUrl}/failed-source.zip`,
      sha256(pendingSourceZip),
      `${server.baseUrl}/external-0.zip`,
      sha256(externalZip),
      "failed-wrapper-key",
      "failed-source-package",
      externalKey,
    );
    await expect(maybeDownloadAndLinkPackages(failedSource)).rejects.toThrow(
      "Failed to fetch package",
    );

    const externalHitsBefore = getPackageCacheStats().externalHits;
    const pendingSource = makeSourcePackage(
      `${server.baseUrl}/pending-source.zip`,
      sha256(pendingSourceZip),
      `${server.baseUrl}/external-0.zip`,
      sha256(externalZip),
      "pending-wrapper-key",
      "pending-source-package",
      externalKey,
    );
    const pendingPublication = maybeDownloadAndLinkPackages(pendingSource);
    await waitFor(
      () => getPackageCacheStats().externalHits > externalHitsBefore,
    );

    const protectedLeases = await Promise.all(
      protectedPackages.map((sourcePackage) =>
        acquireSourcePackage(sourcePackage),
      ),
    );
    const published = await pendingPublication;

    expect(
      fs
        .statSync(path.join(published.dir, "node_modules/example"))
        .isDirectory(),
    ).toBe(true);
    expect(availableExternalPackages.has(externalKey)).toBe(true);

    await Promise.all(protectedLeases.map((lease) => lease.release()));
  } finally {
    await server.close();
  }
});

test("failed sources keep successful external downloads within cache bounds", async () => {
  const externalZip = makeExternalDepsZip();
  const routes: Record<string, Route> = {};
  const sourceZips = Array.from({ length: 10 }, (_, index) => {
    const sourceZip = makeSourcePackageZip(`external-package-${index}`);
    routes[`/source-${index}.zip`] = { status: 500 };
    routes[`/external-${index}.zip`] = externalZip;
    return sourceZip;
  });
  const server = await startPackageServer(routes);

  try {
    const sourcePackages = sourceZips.map((sourceZip, index) =>
      makeSourcePackage(
        `${server.baseUrl}/source-${index}.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external-${index}.zip`,
        sha256(externalZip),
        `deprecated-wrapper-key-${index}`,
        `source-package-${index}`,
        `external-package-${index}`,
      ),
    );
    for (let index = 0; index < sourcePackages.length; index += 1) {
      await expect(
        maybeDownloadAndLinkPackages(sourcePackages[index]),
      ).rejects.toThrow("Failed to fetch package");
    }

    expect(getPackageCacheStats()).toMatchObject({
      retainedSourcePackages: 0,
      retainedExternalPackages: 8,
      sourceFailedPublications: 10,
      externalRetirements: 2,
    });
  } finally {
    await server.close();
  }
});

test("failed source cleanup still enforces external cache bounds", async () => {
  const externalZip = makeExternalDepsZip();
  const routes: Record<string, Route> = {};
  const sourceZips = Array.from({ length: 10 }, (_, index) => {
    const sourceZip = makeSourcePackageZip(`metadata-external-${index}`);
    routes[`/source-${index}.zip`] = sourceZip;
    routes[`/external-${index}.zip`] = externalZip;
    return sourceZip;
  });
  const server = await startPackageServer(routes);
  const realRm = fs.promises.rm;
  const sourceStagingPrefix = `${path.join(tmpdir!, "source")}${path.sep}.`;
  vi.spyOn(fs.promises, "rm").mockImplementation(
    async (...args: Parameters<typeof fs.promises.rm>) => {
      const [target] = args;
      if (
        typeof target === "string" &&
        target.startsWith(sourceStagingPrefix)
      ) {
        throw new Error("simulated source cleanup failure");
      }
      await realRm(...args);
    },
  );

  try {
    for (let index = 0; index < sourceZips.length; index += 1) {
      const sourcePackage = makeSourcePackage(
        `${server.baseUrl}/source-${index}.zip`,
        sha256(sourceZips[index]),
        `${server.baseUrl}/external-${index}.zip`,
        sha256(externalZip),
        `deprecated-wrapper-key-${index}`,
        `source-package-${index}`,
        `external-package-${index}`,
      );
      await expect(maybeDownloadAndLinkPackages(sourcePackage)).rejects.toThrow(
        "Failed to clean failed source package publication",
      );
    }

    expect(getPackageCacheStats()).toMatchObject({
      retainedSourcePackages: 0,
      retainedExternalPackages: 8,
      sourceFailedPublications: 10,
      externalRetirements: 2,
    });
  } finally {
    await server.close();
  }
});

test("source package final directory is absent until publication", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": { body: sourceZip, delayMs: 200 },
    "/external.zip": { body: externalZip, delayMs: 0 },
  });

  try {
    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    const localPromise = maybeDownloadAndLinkPackages(sourcePackage);
    const sourceRoot = path.join(tmpdir!, "source");
    await waitFor(
      () => fs.existsSync(sourceRoot) && fs.readdirSync(sourceRoot).length > 0,
    );

    expect(
      fs.readdirSync(sourceRoot).every((entry) => entry.startsWith(".")),
    ).toBe(true);

    const local = await localPromise;
    expect(path.dirname(local.dir)).toBe(sourceRoot);
    expect(path.basename(local.dir)).not.toMatch(/^\./);
  } finally {
    await server.close();
  }
});

test("different source packages share an external dependency download", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source-a.zip": sourceZip,
    "/source-b.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const sourcePackageA = makeSourcePackage(
      `${server.baseUrl}/source-a.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-a",
      "source-package-key-a",
    );
    const sourcePackageB = makeSourcePackage(
      `${server.baseUrl}/source-b.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-b",
      "source-package-key-b",
    );

    const [localA, localB] = await Promise.all([
      maybeDownloadAndLinkPackages(sourcePackageA),
      maybeDownloadAndLinkPackages(sourcePackageB),
    ]);

    expect(localA.dir).not.toBe(localB.dir);
    expect(server.requestCounts.get("/source-a.zip")).toBe(1);
    expect(server.requestCounts.get("/source-b.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
    expect(
      fs.statSync(path.join(localA.dir, "node_modules/example")).isDirectory(),
    ).toBe(true);
    expect(
      fs.statSync(path.join(localB.dir, "node_modules/example")).isDirectory(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

test("a cross-key cache miss preserves published source and external packages", async () => {
  const sourceZipA = makeSourcePackageZip("external-package-key-a");
  const sourceZipB = makeSourcePackageZip("external-package-key-b");
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source-a.zip": sourceZipA,
    "/source-b.zip": sourceZipB,
    "/external-a.zip": externalZip,
    "/external-b.zip": externalZip,
  });

  try {
    const sourcePackageA = makeSourcePackage(
      `${server.baseUrl}/source-a.zip`,
      sha256(sourceZipA),
      `${server.baseUrl}/external-a.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-a",
      "source-package-key-a",
      "external-package-key-a",
    );
    const sourcePackageB = makeSourcePackage(
      `${server.baseUrl}/source-b.zip`,
      sha256(sourceZipB),
      `${server.baseUrl}/external-b.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-b",
      "source-package-key-b",
      "external-package-key-b",
    );

    const localA = await maybeDownloadAndLinkPackages(sourcePackageA);
    await maybeDownloadAndLinkPackages(sourcePackageB);

    expect(
      fs.statSync(path.join(localA.dir, "modules/actions/example.js")).isFile(),
    ).toBe(true);
    expect(
      fs.statSync(path.join(localA.dir, "node_modules/example")).isDirectory(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

test("source cache uses the bundled source package key", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const firstRequest = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-a",
    );
    const secondRequest = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-b",
    );

    const firstLocal = await maybeDownloadAndLinkPackages(firstRequest);
    const secondLocal = await maybeDownloadAndLinkPackages(secondRequest);

    expect(secondLocal.dir).toBe(firstLocal.dir);
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
  } finally {
    await server.close();
  }
});

test("same source key rejects a different archive checksum", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": { body: sourceZip, delayMs: 100 },
    "/external.zip": externalZip,
  });

  try {
    const matchingRequest = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );
    const mismatchedRequest = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(Buffer.from("different source archive")),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    const matchingPromise = maybeDownloadAndLinkPackages(matchingRequest);
    await expect(
      maybeDownloadAndLinkPackages(mismatchedRequest),
    ).rejects.toThrow(
      "Package checksum does not match cached package identity",
    );
    const local = await matchingPromise;

    await expect(
      maybeDownloadAndLinkPackages(mismatchedRequest),
    ).rejects.toThrow(
      "Package checksum does not match cached package identity",
    );
    expect(fs.existsSync(local.dir)).toBe(true);
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
  } finally {
    await server.close();
  }
});

test("same-key waiters reject mismatched external dependencies", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": { body: sourceZip, delayMs: 100 },
    "/external.zip": externalZip,
  });

  try {
    const matchingRequest = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );
    const mismatchedRequest = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key",
      "source-package-key",
      "different-external-package-key",
    );

    const matchingPromise = maybeDownloadAndLinkPackages(matchingRequest);
    const mismatchedPromise = maybeDownloadAndLinkPackages(mismatchedRequest);

    await expect(matchingPromise).resolves.toBeDefined();
    await expect(mismatchedPromise).rejects.toThrow(
      "Source package external dependencies do not match package metadata",
    );
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
  } finally {
    await server.close();
  }
});

test("same external key rejects a different archive checksum", async () => {
  const sourceZipA = makeSourcePackageZip();
  const sourceZipB = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source-a.zip": sourceZipA,
    "/source-b.zip": sourceZipB,
    "/external.zip": externalZip,
  });

  try {
    const sourcePackageA = makeSourcePackage(
      `${server.baseUrl}/source-a.zip`,
      sha256(sourceZipA),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-a",
      "source-package-key-a",
    );
    const sourcePackageB = makeSourcePackage(
      `${server.baseUrl}/source-b.zip`,
      sha256(sourceZipB),
      `${server.baseUrl}/external.zip`,
      sha256(Buffer.from("different external archive")),
      "deprecated-wrapper-key-b",
      "source-package-key-b",
    );

    const localA = await maybeDownloadAndLinkPackages(sourcePackageA);
    const mismatchedCachedSource = makeSourcePackage(
      `${server.baseUrl}/source-a.zip`,
      sha256(sourceZipA),
      `${server.baseUrl}/external.zip`,
      sha256(Buffer.from("different external archive")),
      "deprecated-wrapper-key-a",
      "source-package-key-a",
    );
    await expect(
      maybeDownloadAndLinkPackages(mismatchedCachedSource),
    ).rejects.toThrow(
      "Package checksum does not match cached package identity",
    );
    await expect(maybeDownloadAndLinkPackages(sourcePackageB)).rejects.toThrow(
      "Package checksum does not match cached package identity",
    );

    expect(fs.existsSync(localA.dir)).toBe(true);
    expect(availableSourcePackages.has("source-package-key-b")).toBe(false);
    expect(server.requestCounts.get("/source-a.zip")).toBe(1);
    expect(server.requestCounts.get("/source-b.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
  } finally {
    await server.close();
  }
});

test("retired package identities use new Node module-cache paths", async () => {
  const externalZip = makeExternalDepsZip();
  const routes: Record<string, Route> = {};
  const sourceZips = Array.from({ length: 9 }, (_, index) => {
    const sourceZip = makeSourcePackageZip(`external-package-${index}`);
    routes[`/source-${index}.zip`] = sourceZip;
    routes[`/external-${index}.zip`] = externalZip;
    return sourceZip;
  });
  const changedExternalZip = makeExternalDepsZip(2);
  const changedExternalSourceZip = makeSourcePackageZip("external-package-0");
  const changedSourceZip = makeSourcePackageZip("external-package-0", 2);
  routes["/changed-external-source.zip"] = changedExternalSourceZip;
  routes["/changed-source.zip"] = changedSourceZip;
  routes["/changed-external.zip"] = changedExternalZip;
  const server = await startPackageServer(routes);

  try {
    const sourcePackages = sourceZips.map((sourceZip, index) =>
      makeSourcePackage(
        `${server.baseUrl}/source-${index}.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external-${index}.zip`,
        sha256(externalZip),
        `deprecated-wrapper-key-${index}`,
        `source-package-${index}`,
        `external-package-${index}`,
      ),
    );
    let retiredSourceDir: string | undefined;
    let retiredExternalDir: string | undefined;
    for (const sourcePackage of sourcePackages) {
      const lease = await acquireSourcePackage(sourcePackage);
      if (sourcePackage === sourcePackages[0]) {
        retiredSourceDir = lease.package.dir;
        retiredExternalDir =
          availableExternalPackages.get("external-package-0")?.dir;
      }
      await lease.release();
    }
    if (retiredSourceDir === undefined || retiredExternalDir === undefined) {
      throw new Error("Expected the first package paths to be captured");
    }

    expect(availableSourcePackages.has("source-package-0")).toBe(false);
    expect(availableExternalPackages.has("external-package-0")).toBe(false);

    const changedExternal = makeSourcePackage(
      `${server.baseUrl}/changed-external-source.zip`,
      sha256(changedExternalSourceZip),
      `${server.baseUrl}/changed-external.zip`,
      sha256(changedExternalZip),
      "changed-external-wrapper",
      "changed-external-source-package",
      "external-package-0",
    );
    const changedExternalLease = await acquireSourcePackage(changedExternal);
    const changedExternalDir =
      availableExternalPackages.get("external-package-0")?.dir;
    if (changedExternalDir === undefined) {
      throw new Error("Expected changed external package to be cached");
    }
    expect(changedExternalDir).not.toBe(retiredExternalDir);
    expect(
      fs.readFileSync(
        path.join(changedExternalDir, "node_modules/example/index.js"),
        "utf8",
      ),
    ).toContain("module.exports = 2");
    await changedExternalLease.release();

    const changedSource = makeSourcePackage(
      `${server.baseUrl}/changed-source.zip`,
      sha256(changedSourceZip),
      `${server.baseUrl}/changed-external.zip`,
      sha256(changedExternalZip),
      "changed-source-wrapper",
      "source-package-0",
      "external-package-0",
    );
    const changedSourceLease = await acquireSourcePackage(changedSource);
    expect(changedSourceLease.package.dir).not.toBe(retiredSourceDir);
    expect(
      fs.readFileSync(
        path.join(changedSourceLease.package.dir, "modules/actions/example.js"),
        "utf8",
      ),
    ).toContain("export const value = 2");
    await changedSourceLease.release();

    expect(server.requestCounts.get("/changed-external.zip")).toBe(1);
    expect(server.requestCounts.get("/changed-source.zip")).toBe(1);
  } finally {
    await server.close();
  }
});

test("a cached dependency mismatch does not remove the published package", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const sourceOnlyPackage = makeSourceOnlyPackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
    );
    const local = await maybeDownloadAndLinkPackages(sourceOnlyPackage);
    const inconsistentPackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    await expect(
      maybeDownloadAndLinkPackages(inconsistentPackage),
    ).rejects.toThrow(
      "Source package external dependencies do not match package metadata",
    );

    expect(fs.existsSync(local.dir)).toBe(true);
    expect(availableSourcePackages.get("source-package-key")?.dir).toBe(
      local.dir,
    );
    expect(server.requestCounts.get("/source.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBeUndefined();
  } finally {
    await server.close();
  }
});

test("package keys cannot cross cache roots or delete another package", async () => {
  const externalPackageKeyA = "external-package-key-a";
  const sourcePackageKeyA = "source-package-key-a";
  const crossingSourceKey = `../external_deps/${externalPackageKeyA}`;
  const crossingExternalKey = `../source/${sourcePackageKeyA}`;
  const sourceZipA = makeSourcePackageZip(externalPackageKeyA);
  const sourceZipB = makeSourcePackageZip(crossingExternalKey);
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source-a.zip": sourceZipA,
    "/source-b.zip": sourceZipB,
    "/external-a.zip": externalZip,
    "/external-b.zip": externalZip,
  });

  try {
    const sourcePackageA = makeSourcePackage(
      `${server.baseUrl}/source-a.zip`,
      sha256(sourceZipA),
      `${server.baseUrl}/external-a.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-a",
      sourcePackageKeyA,
      externalPackageKeyA,
    );
    const sourcePackageB = makeSourcePackage(
      `${server.baseUrl}/source-b.zip`,
      sha256(sourceZipB),
      `${server.baseUrl}/external-b.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-b",
      crossingSourceKey,
      crossingExternalKey,
    );

    const localA = await maybeDownloadAndLinkPackages(sourcePackageA);
    const localB = await maybeDownloadAndLinkPackages(sourcePackageB);
    const externalB = availableExternalPackages.get(crossingExternalKey);
    if (externalB === undefined) {
      throw new Error("Expected crossing-key external package to be cached");
    }

    expect(path.dirname(localB.dir)).toBe(path.join(tmpdir!, "source"));
    expect(path.dirname(externalB.dir)).toBe(
      path.join(tmpdir!, "external_deps"),
    );
    expect(
      fs.statSync(path.join(localA.dir, "modules/actions/example.js")).isFile(),
    ).toBe(true);
    expect(
      fs.statSync(path.join(localA.dir, "node_modules/example")).isDirectory(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

test("failed external deps download cannot publish abandoned source over retry", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source.zip": (requestNumber) => ({
      body: sourceZip,
      delayMs: requestNumber === 1 ? 100 : 10,
    }),
    "/external.zip": (requestNumber) =>
      requestNumber === 1
        ? { status: 500, body: Buffer.from("temporary failure"), delayMs: 0 }
        : { body: externalZip, delayMs: 0 },
  });

  try {
    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    await expect(maybeDownloadAndLinkPackages(sourcePackage)).rejects.toThrow(
      "Failed to fetch package",
    );
    expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);
    expect(
      await fs.promises.readdir(path.join(tmpdir!, "external_deps")),
    ).toEqual([]);

    const local = await maybeDownloadAndLinkPackages(sourcePackage);
    await sleep(150);

    expect(server.requestCounts.get("/source.zip")).toBe(2);
    expect(server.requestCounts.get("/external.zip")).toBe(2);
    expect(
      fs.statSync(path.join(local.dir, "modules/actions/example.js")).isFile(),
    ).toBe(true);
    expect(
      fs.lstatSync(path.join(local.dir, "node_modules")).isSymbolicLink(),
    ).toBe(true);
    expect(
      fs.statSync(path.join(local.dir, "node_modules/example")).isDirectory(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

test("failed source download cleans staging and reuses successful external deps", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const sourceRoute = "/source.zip?token=signed-secret";
  const server = await startPackageServer({
    [sourceRoute]: (requestNumber) =>
      requestNumber === 1
        ? { status: 500, body: Buffer.from("temporary failure"), delayMs: 0 }
        : { body: sourceZip, delayMs: 0 },
    "/external.zip": { body: externalZip, delayMs: 0 },
  });

  try {
    const sourcePackage = makeSourcePackage(
      `${server.baseUrl}${sourceRoute}`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
    );

    const failedDownload = maybeDownloadAndLinkPackages(sourcePackage);
    await expect(failedDownload).rejects.toThrow("Failed to fetch package");
    await expect(failedDownload).rejects.not.toThrow("signed-secret");
    expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);
    expect(
      await fs.promises.readdir(path.join(tmpdir!, "external_deps")),
    ).toHaveLength(1);

    const local = await maybeDownloadAndLinkPackages(sourcePackage);

    expect(server.requestCounts.get(sourceRoute)).toBe(2);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
    expect(
      fs.statSync(path.join(local.dir, "modules/actions/example.js")).isFile(),
    ).toBe(true);
    expect(
      fs.statSync(path.join(local.dir, "node_modules/example")).isDirectory(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

test("invalid package URLs do not disclose signed query strings", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const sourcePackage = makeSourceOnlyPackage(
    "https://[invalid?token=signed-secret",
    sha256(sourceZip),
  );

  const failedDownload = maybeDownloadAndLinkPackages(sourcePackage);
  await expect(failedDownload).rejects.toThrow("Invalid package URL");
  await expect(failedDownload).rejects.not.toThrow("signed-secret");
  expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);
});

test("oversized package downloads fail before buffering the response", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const sourceUri = "https://packages.invalid/source.zip?token=signed-secret";
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(sourceZip, {
      headers: { "Content-Length": "90000000" },
    }),
  );
  const sourcePackage = makeSourceOnlyPackage(sourceUri, sha256(sourceZip));

  const failedDownload = maybeDownloadAndLinkPackages(sourcePackage);
  await expect(failedDownload).rejects.toThrow(
    "Package archive exceeds the size limit",
  );
  await expect(failedDownload).rejects.not.toThrow("signed-secret");
  expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);
});

test("oversized declared extraction fails before archive allocation", async () => {
  const sourceZip = withFirstDeclaredUncompressedSize(
    makeSourcePackageZip(null),
    230_000_000,
  );
  const server = await startPackageServer({ "/source.zip": sourceZip });

  try {
    const sourcePackage = makeSourceOnlyPackage(
      `${server.baseUrl}/source.zip`,
      sha256(sourceZip),
    );

    await expect(maybeDownloadAndLinkPackages(sourcePackage)).rejects.toThrow(
      "Package archive exceeds the extracted size limit",
    );
    expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);
  } finally {
    await server.close();
  }
});

test("malformed or CRC-invalid entries fail inside the package boundary", async () => {
  const malformedSourceZip = corruptZipEntry(
    makeSourcePackageZip(null),
    "modules/actions/example.js",
  );
  const crcInvalidSourceZip = invalidateZipEntryCrc(
    makeSourcePackageZip(null),
    "modules/actions/example.js",
  );
  const server = await startPackageServer({
    "/malformed.zip": malformedSourceZip,
    "/crc-invalid.zip": crcInvalidSourceZip,
  });

  try {
    for (const [name, sourceZip] of [
      ["malformed", malformedSourceZip],
      ["crc-invalid", crcInvalidSourceZip],
    ] as const) {
      const sourcePackage = makeSourceOnlyPackage(
        `${server.baseUrl}/${name}.zip`,
        sha256(sourceZip),
        name,
      );

      await expect(maybeDownloadAndLinkPackages(sourcePackage)).rejects.toThrow(
        "Failed to extract package archive",
      );
      expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual(
        [],
      );
    }
  } finally {
    await server.close();
  }
});

test("local package read failures do not disclose paths or query strings", async () => {
  const sourceZip = makeSourcePackageZip(null);
  const packageUrl = pathToFileURL(
    path.join(tmpdir!, "private-package-path.zip"),
  );
  packageUrl.searchParams.set("token", "signed-secret");
  const sourcePackage = makeSourceOnlyPackage(
    packageUrl.href,
    sha256(sourceZip),
  );

  const failedDownload = maybeDownloadAndLinkPackages(sourcePackage);
  await expect(failedDownload).rejects.toThrow(
    "Failed while downloading package",
  );
  await expect(failedDownload).rejects.not.toThrow("private-package-path");
  await expect(failedDownload).rejects.not.toThrow("signed-secret");
  expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);

  await fs.promises.writeFile(packageUrl, sourceZip);
  const local = await maybeDownloadAndLinkPackages(sourcePackage);
  expect(
    fs.statSync(path.join(local.dir, "modules/actions/example.js")).isFile(),
  ).toBe(true);
});

test("stalled response body times out, cleans staging, and permits retry", async () => {
  vi.useFakeTimers();
  const sourceZip = makeSourcePackageZip(null);
  const sourceUri = "https://packages.invalid/source.zip?token=signed-secret";
  let requestCount = 0;
  let markFetchStarted!: () => void;
  const fetchStarted = new Promise<void>((resolve) => {
    markFetchStarted = resolve;
  });
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
    requestCount += 1;
    if (requestCount > 1) {
      return new Response(sourceZip);
    }

    const signal = init?.signal;
    if (signal === undefined || signal === null) {
      throw new Error("Expected package fetch to have an abort signal");
    }
    let bodyController: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        bodyController = controller;
      },
    });
    signal.addEventListener(
      "abort",
      () => {
        bodyController.error(new DOMException("aborted", "AbortError"));
      },
      { once: true },
    );
    markFetchStarted();
    return new Response(body);
  });

  const sourcePackage = makeSourceOnlyPackage(sourceUri, sha256(sourceZip));
  const failedDownload = maybeDownloadAndLinkPackages(sourcePackage);
  await fetchStarted;
  await vi.advanceTimersByTimeAsync(120_000);

  await expect(failedDownload).rejects.toThrow(
    "Timed out downloading package after 120000ms",
  );
  await expect(failedDownload).rejects.not.toThrow("signed-secret");
  expect(await fs.promises.readdir(path.join(tmpdir!, "source"))).toEqual([]);

  const local = await maybeDownloadAndLinkPackages(sourcePackage);
  expect(requestCount).toBe(2);
  expect(
    fs.statSync(path.join(local.dir, "modules/actions/example.js")).isFile(),
  ).toBe(true);
});

test.each([
  ["bundled dependency chunk", "modules/_deps/chunk.js"],
  ["source map", "modules/actions/example.js.map"],
  ["package json ESM marker", "package.json"],
])(
  "current source cache hits do not scan the published %s path",
  async (_, filePath) => {
    const sourceZip = makeSourcePackageZip();
    const externalZip = makeExternalDepsZip();
    const server = await startPackageServer({
      "/source.zip": sourceZip,
      "/external.zip": externalZip,
    });

    try {
      const sourcePackage = makeSourcePackage(
        `${server.baseUrl}/source.zip`,
        sha256(sourceZip),
        `${server.baseUrl}/external.zip`,
        sha256(externalZip),
      );

      const firstLocal = await maybeDownloadAndLinkPackages(sourcePackage);
      await fs.promises.rm(path.join(firstLocal.dir, filePath));

      const reused = await maybeDownloadAndLinkPackages(sourcePackage);

      expect(reused).toBe(firstLocal);
      expect(fs.existsSync(firstLocal.dir)).toBe(true);
      expect(server.requestCounts.get("/source.zip")).toBe(1);
      expect(server.requestCounts.get("/external.zip")).toBe(1);
      expect(
        fs
          .statSync(path.join(firstLocal.dir, "modules/actions/example.js"))
          .isFile(),
      ).toBe(true);
      expect(
        fs
          .statSync(path.join(firstLocal.dir, "node_modules/example"))
          .isDirectory(),
      ).toBe(true);
    } finally {
      await server.close();
    }
  },
);

test("new source validation detects a missing cached external tree", async () => {
  const sourceZip = makeSourcePackageZip();
  const externalZip = makeExternalDepsZip();
  const server = await startPackageServer({
    "/source-a.zip": sourceZip,
    "/source-b.zip": sourceZip,
    "/external.zip": externalZip,
  });

  try {
    const sourcePackageA = makeSourcePackage(
      `${server.baseUrl}/source-a.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-a",
      "source-package-key-a",
    );
    const sourcePackageB = makeSourcePackage(
      `${server.baseUrl}/source-b.zip`,
      sha256(sourceZip),
      `${server.baseUrl}/external.zip`,
      sha256(externalZip),
      "deprecated-wrapper-key-b",
      "source-package-key-b",
    );
    const localA = await maybeDownloadAndLinkPackages(sourcePackageA);
    const externalPackage = availableExternalPackages.get(
      "external-package-key",
    );
    if (externalPackage === undefined) {
      throw new Error("Expected external package to be cached");
    }
    await fs.promises.rm(path.join(externalPackage.dir, "node_modules"), {
      recursive: true,
    });

    await expect(maybeDownloadAndLinkPackages(sourcePackageB)).rejects.toThrow(
      "Incomplete source package",
    );

    expect(fs.existsSync(externalPackage.dir)).toBe(true);
    expect(fs.existsSync(localA.dir)).toBe(true);
    expect(server.requestCounts.get("/source-a.zip")).toBe(1);
    expect(server.requestCounts.get("/source-b.zip")).toBe(1);
    expect(server.requestCounts.get("/external.zip")).toBe(1);
    expect(
      fs.statSync(path.join(localA.dir, "modules/actions/example.js")).isFile(),
    ).toBe(true);
  } finally {
    await server.close();
  }
});

function makeSourcePackage(
  sourceUri: string,
  sourceSha256: string,
  externalUri: string,
  externalSha256: string,
  deprecatedKey = "source-package-key",
  bundledSourceKey = "source-package-key",
  externalPackageKey = "external-package-key",
): SourcePackage {
  const bundledSource = {
    uri: sourceUri,
    key: bundledSourceKey,
    sha256: sourceSha256,
  };
  return {
    ...bundledSource,
    key: deprecatedKey,
    bundled_source: bundledSource,
    external_deps: {
      uri: externalUri,
      key: externalPackageKey,
      sha256: externalSha256,
    },
  };
}

function makeSourceOnlyPackage(
  sourceUri: string,
  sourceSha256: string,
  sourcePackageKey = "source-package-key",
): SourcePackage {
  const bundledSource = {
    uri: sourceUri,
    key: sourcePackageKey,
    sha256: sourceSha256,
  };
  return {
    ...bundledSource,
    bundled_source: bundledSource,
    external_deps: null,
  };
}

function makeSourcePackageZip(
  externalDepsStorageKey: string | null = "external-package-key",
  moduleValue = 1,
  actionEnvironment = "node",
  moduleEnvironments: [string, string][] | undefined = undefined,
  applicationModule = `import "../_deps/chunk.js";\nexport const value = ${moduleValue};\n`,
): Buffer {
  moduleEnvironments ??= [
    ["_deps/chunk.js", "node"],
    ["actions/example.js", actionEnvironment],
  ];
  const zip = new AdmZip();
  zip.addFile("modules/", Buffer.alloc(0));
  zip.addFile("modules/actions/", Buffer.alloc(0));
  zip.addFile("modules/_deps/", Buffer.alloc(0));
  zip.addFile(
    "metadata.json",
    Buffer.from(
      JSON.stringify({
        modulePaths: [
          "_deps/chunk.js",
          "actions/example.js",
          "actions/example.js.map",
        ],
        moduleEnvironments,
        externalDepsStorageKey: externalDepsStorageKey ?? undefined,
      }),
    ),
  );
  zip.addFile("modules/actions/example.js", Buffer.from(applicationModule));
  zip.addFile(
    "modules/_deps/chunk.js",
    Buffer.from("export const chunk = 1;\n"),
  );
  zip.addFile(
    "modules/actions/example.js.map",
    Buffer.from('{"version":3,"sources":["example.ts"],"mappings":""}'),
  );
  return zip.toBuffer();
}

function makeSourceOnlyPackageZip(moduleCount: number): Buffer {
  const modulePaths = Array.from(
    { length: moduleCount },
    (_, index) => `actions/module_${index}.js`,
  );
  const zip = new AdmZip();
  zip.addFile("modules/", Buffer.alloc(0));
  zip.addFile("modules/actions/", Buffer.alloc(0));
  zip.addFile(
    "metadata.json",
    Buffer.from(
      JSON.stringify({
        modulePaths,
        moduleEnvironments: modulePaths.map((modulePath) => [
          modulePath,
          "node",
        ]),
      }),
    ),
  );
  for (const [index, modulePath] of modulePaths.entries()) {
    zip.addFile(
      `modules/${modulePath}`,
      Buffer.from(`export const value = ${index};\n`),
    );
  }
  return zip.toBuffer();
}

function makeExternalDepsZip(moduleValue = 1): Buffer {
  const zip = new AdmZip();
  zip.addFile("node_modules/", Buffer.alloc(0));
  zip.addFile("node_modules/example/", Buffer.alloc(0));
  zip.addFile(
    "node_modules/example/index.js",
    Buffer.from(`module.exports = ${moduleValue};\n`),
  );
  return zip.toBuffer();
}

function sha256(buffer: Buffer): string {
  return createHash("sha256").update(buffer).digest("base64url");
}

function withFirstDeclaredUncompressedSize(
  zipBuffer: Buffer,
  size: number,
): Buffer {
  const result = Buffer.from(zipBuffer);
  const centralDirectoryHeader = result.indexOf(
    Buffer.from([0x50, 0x4b, 0x01, 0x02]),
  );
  if (centralDirectoryHeader === -1) {
    throw new Error("Test ZIP has no central-directory entry");
  }
  result.writeUInt32LE(size, centralDirectoryHeader + 24);
  return result;
}

function corruptZipEntry(zipBuffer: Buffer, entryName: string): Buffer {
  const result = Buffer.from(zipBuffer);
  const entry = new AdmZip(result).getEntry(entryName);
  if (entry === null) {
    throw new Error("Test ZIP entry is missing");
  }
  const localHeaderOffset = entry.header.offset;
  const fileNameLength = result.readUInt16LE(localHeaderOffset + 26);
  const extraLength = result.readUInt16LE(localHeaderOffset + 28);
  const dataOffset = localHeaderOffset + 30 + fileNameLength + extraLength;
  if (entry.header.compressedSize === 0) {
    throw new Error("Test ZIP entry has no compressed data");
  }
  result[dataOffset] ^= 0xff;
  return result;
}

function invalidateZipEntryCrc(zipBuffer: Buffer, entryName: string): Buffer {
  const result = Buffer.from(zipBuffer);
  const encodedName = Buffer.from(entryName);
  let nameOffset = -1;
  for (;;) {
    nameOffset = result.indexOf(encodedName, nameOffset + 1);
    if (nameOffset === -1) {
      throw new Error("Test ZIP central-directory entry is missing");
    }
    const centralHeaderOffset = nameOffset - 46;
    if (
      centralHeaderOffset >= 0 &&
      result.readUInt32LE(centralHeaderOffset) === 0x02014b50
    ) {
      const crcOffset = centralHeaderOffset + 16;
      result.writeUInt32LE(
        (result.readUInt32LE(crcOffset) ^ 1) >>> 0,
        crcOffset,
      );
      return result;
    }
  }
}

type RouteResponse = {
  body?: Buffer;
  delayMs?: number;
  status?: number;
};

type Route =
  | Buffer
  | RouteResponse
  | ((requestNumber: number) => RouteResponse);

async function startPackageServer(routes: Record<string, Route>): Promise<{
  baseUrl: string;
  requestCounts: Map<string, number>;
  close: () => Promise<void>;
}> {
  const requestCounts = new Map<string, number>();
  const server = http.createServer((req, res) => {
    const url = req.url ?? "";
    const requestNumber = (requestCounts.get(url) ?? 0) + 1;
    requestCounts.set(url, requestNumber);
    const route = routes[url];
    if (route === undefined) {
      res.writeHead(404);
      res.end();
      return;
    }
    const response =
      typeof route === "function"
        ? route(requestNumber)
        : Buffer.isBuffer(route)
          ? { body: route }
          : route;
    setTimeout(() => {
      res.writeHead(response.status ?? 200, {
        "Content-Type": "application/zip",
      });
      res.end(response.body ?? Buffer.alloc(0));
    }, response.delayMs ?? 25);
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (typeof address !== "object" || address === null) {
    throw new Error("Test package server did not bind to a TCP port");
  }

  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    requestCounts,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
  };
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await sleep(10);
  }
  throw new Error("Condition was not met before timeout");
}
