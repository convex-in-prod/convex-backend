import { existsSync, mkdirSync, realpathSync, writeFileSync } from "node:fs";
import path from "node:path";
import { expect, test, vi } from "vitest";
import type { Context } from "../../bundler/context.js";
import { nodeFs, RecordingFs, withTmpDir } from "../../bundler/fs.js";
import {
  typeCheckFunctions,
  typeCheckFunctionsInMode,
  type TypecheckResult,
} from "./typecheck.js";
import { spawnAsync } from "./utils/utils.js";

function testContext(): Context {
  return {
    fs: nodeFs,
    deprecationMessagePrinted: false,
    crash: async ({ printedMessage }) => {
      throw new Error(printedMessage ?? "CLI crashed without a message");
    },
    registerCleanup: () => "test-cleanup",
    removeCleanup: () => async () => {},
    bigBrainAuth: () => null,
    _updateBigBrainAuth: () => {},
  };
}

test("tsrs launcher retains file observation and diagnostic retry behavior", async () => {
  await withTmpDir(async (tmpDir) => {
    const originalDirectory = process.cwd();
    const recordingFs = new RecordingFs(false);
    const registerPath = vi.spyOn(recordingFs, "registerPath");
    // The child resolves cwd to its real path, including macOS's symlinked temp directory.
    const sourcePath = path.join(
      realpathSync(tmpDir.path),
      "convex",
      "check.ts",
    );
    mkdirSync(path.join(tmpDir.path, "convex"));
    writeFileSync(
      path.join(tmpDir.path, "convex", "tsconfig.json"),
      JSON.stringify({
        compilerOptions: { noEmit: false, types: [], lib: ["es2020"] },
      }),
    );
    writeFileSync(sourcePath, "const value: number = 1;");
    try {
      process.chdir(tmpDir.path);
      const ctx = testContext();
      ctx.fs = recordingFs;
      await typeCheckFunctionsInMode(ctx, "enable", "convex");
      expect(existsSync(sourcePath.replace(/\.ts$/, ".js"))).toBe(false);
      expect(registerPath.mock.calls.some(([p]) => p === sourcePath)).toBe(
        true,
      );
      expect(recordingFs.finalize()).not.toBe("invalidated");

      writeFileSync(sourcePath, 'const value: number = "invalid";');
      const retryFs = new RecordingFs(false);
      ctx.fs = retryFs;
      let failed = false;
      await typeCheckFunctions(ctx, "convex", async (result, _log, retry) => {
        expect(result).toBe("typecheckFailed");
        expect(retry).toBeDefined();
        await expect(retry!()).rejects.toMatchObject({ status: 2 });
        expect(retryFs.finalize()).not.toBe("invalidated");
        // A corrected source between passes retains the existing concurrent-change recovery.
        writeFileSync(sourcePath, "const value: number = 1;");
        await expect(retry!()).resolves.toBe("success");
        failed = true;
      });
      expect(failed).toBe(true);
      expect(retryFs.finalize()).toBe("invalidated");
    } finally {
      process.chdir(originalDirectory);
      registerPath.mockRestore();
    }
  });
});

test("typecheck modes skip only missing configuration or an explicitly disabled check", async () => {
  await withTmpDir(async (tmpDir) => {
    const ctx = testContext();
    await expect(
      typeCheckFunctionsInMode(ctx, "try", tmpDir.path),
    ).resolves.toBeUndefined();
    await expect(
      typeCheckFunctionsInMode(ctx, "enable", tmpDir.path),
    ).rejects.toThrow();

    writeFileSync(
      path.join(tmpDir.path, "tsconfig.json"),
      JSON.stringify({ compilerOptions: { pretty: true } }),
    );
    await expect(
      typeCheckFunctionsInMode(ctx, "try", tmpDir.path),
    ).resolves.toBeUndefined();

    writeFileSync(
      path.join(tmpDir.path, "check.ts"),
      'const value: number = "invalid";',
    );
    await expect(
      typeCheckFunctionsInMode(ctx, "disable", tmpDir.path),
    ).resolves.toBeUndefined();
    await expect(
      typeCheckFunctionsInMode(ctx, "try", tmpDir.path),
    ).rejects.toThrow();
  });
});

test("diagnostic subprocess failure waits for its output pipes to close", async () => {
  await withTmpDir(async (tmpDir) => {
    const marker = path.join(tmpDir.path, "diagnostics-complete");
    // A descendant keeps the diagnostic pipes open after the launcher exits.
    const lateWriter = `
      setTimeout(() => {
        process.stderr.write("final diagnostic\\n");
        require("node:fs").writeFileSync(process.argv[1], "done");
      }, 200);
    `;
    const launcher = `
      const { spawn } = require("node:child_process");
      spawn(process.execPath, ["-e", process.argv[1], process.argv[2]], {
        stdio: ["ignore", 1, 2],
      }).unref();
      process.exit(2);
    `;
    await expect(
      spawnAsync(
        testContext(),
        process.execPath,
        ["-e", launcher, lateWriter, marker],
        { stdio: "inherit" },
      ),
    ).rejects.toMatchObject({ status: 2 });
    expect(existsSync(marker)).toBe(true);
  });
});

test("subprocess output preserves UTF-8 across pipe chunks", async () => {
  const writer = `
    const bytes = Buffer.from("check-\u{1f600}.ts\\n");
    const split = Buffer.byteLength("check-") + 1;
    process.stdout.write(bytes.subarray(0, split));
    process.stderr.write(bytes.subarray(0, split));
    setTimeout(() => {
      process.stdout.write(bytes.subarray(split));
      process.stderr.write(bytes.subarray(split));
    }, 100);
  `;
  const result = await spawnAsync(testContext(), process.execPath, [
    "-e",
    writer,
  ]);
  expect(result.status).toBe(0);
  expect(result.stdout).toBe("check-\u{1f600}.ts\n");
  expect(result.stderr).toBe("check-\u{1f600}.ts\n");
});

test("a no-inputs diagnostic does not suppress other compiler errors", async () => {
  await withTmpDir(async (tmpDir) => {
    const compiler = path.join(tmpDir.path, "diagnostic-compiler");
    writeFileSync(path.join(tmpDir.path, "tsconfig.json"), "{}");
    // Exercise the pinned package's launcher with a deterministic diagnostic stream.
    writeFileSync(
      compiler,
      `#!/usr/bin/env node
console.log("error TS18003: No inputs were found.");
console.log("error TS5023: Unknown compiler option 'invalid'.");
process.exitCode = 2;
`,
      { mode: 0o755 },
    );
    vi.stubEnv("TSRS_BINARY", compiler);
    try {
      let result: TypecheckResult | undefined;
      await typeCheckFunctions(testContext(), tmpDir.path, async (value) => {
        result = value;
      });
      expect(result).toBe("typecheckFailed");
    } finally {
      vi.unstubAllEnvs();
    }
  });
});
