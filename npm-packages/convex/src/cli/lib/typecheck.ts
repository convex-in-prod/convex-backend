import { chalkStderr } from "chalk";
import path from "path";
import { performance } from "perf_hooks";
import { Context } from "../../bundler/context.js";
import {
  changeSpinner,
  logError,
  logFailure,
  logMessage,
  showSpinner,
} from "../../bundler/log.js";
import * as Sentry from "@sentry/node";
import { formatDuration, spawnAsync } from "./utils/utils.js";
import { WatchContext } from "./watch.js";
import { createRequire } from "node:module";

export type TypecheckResult = "cantTypeCheck" | "success" | "typecheckFailed";

export type TypeCheckMode = "enable" | "try" | "disable";

const SLOW_TYPECHECK_THRESHOLD_MS = 10_000;
const SLOW_TYPECHECK_DOCS_URL =
  "https://docs.convex.dev/cli/troubleshooting/typecheck-performance";

type TypecheckResultHandler = (
  result: TypecheckResult,
  logSpecificError?: () => void,
  // If given, we run it to print out errors.
  // We expect it to throw or resolve to "success"
  // if a concurrent change invalidated the error result.
  runOnError?: () => Promise<"success">,
) => Promise<void>;

/**
 * Conditionally run a typecheck function and interpret the result.
 *
 * If typeCheckMode === "disable", never run the typecheck function.
 * If typeCheckMode === "enable", run the typecheck and crash if typechecking
 * fails or the project has no tsconfig.
 * If typeCheckMode === "try", try and run the typecheck. crash if typechecking
 * fails but skip projects without a tsconfig.
 */
export async function typeCheckFunctionsInMode(
  ctx: Context,
  typeCheckMode: TypeCheckMode,
  functionsDir: string,
): Promise<void> {
  if (typeCheckMode === "disable") {
    return;
  }
  const typecheckStart = performance.now();
  const slowTypecheckTimeout = setTimeout(() => {
    changeSpinner(
      `Still running TypeScript... see ${SLOW_TYPECHECK_DOCS_URL} to troubleshoot.`,
    );
  }, SLOW_TYPECHECK_THRESHOLD_MS);
  try {
    await typeCheckFunctions(
      ctx,
      functionsDir,
      async (result, logSpecificError, runOnError) => {
        if (
          (result === "cantTypeCheck" && typeCheckMode === "enable") ||
          result === "typecheckFailed"
        ) {
          logSpecificError?.();
          logError(
            chalkStderr.gray(
              "To ignore failing typecheck, use `--typecheck=disable`.",
            ),
          );
          try {
            const result = await runOnError?.();
            // Concurrent change invalidated the error, don't fail
            if (result === "success") {
              return;
            }
          } catch {
            // As expected, `runOnError` threw
          }
          await ctx.crash({
            exitCode: 1,
            errorType: "invalid filesystem data",
            printedMessage: null,
          });
        }
      },
    );
  } finally {
    clearTimeout(slowTypecheckTimeout);
  }
  maybeLogSlowTypecheckSuggestion(ctx, performance.now() - typecheckStart);
}

// Runs TypeScript compiler to typecheck Convex query and mutation functions.
export async function typeCheckFunctions(
  ctx: Context,
  functionsDir: string,
  handleResult: TypecheckResultHandler,
): Promise<void> {
  const tsconfig = path.join(functionsDir, "tsconfig.json");
  if (!ctx.fs.exists(tsconfig)) {
    return handleResult("cantTypeCheck", () => {
      logError(`No TypeScript configuration found at ${tsconfig}.`);
      logError("Run `npx convex codegen --init` to create one.");
    });
  }
  // Resolve the compiler shipped with this CLI, including pnpm's nested layout. The
  // published CLI is CommonJS; source and ESM consumers use import.meta.url instead.
  const require = createRequire(
    typeof __filename === "string" ? __filename : import.meta.url,
  );
  const compilerPath = path.join(
    path.dirname(require.resolve("tsrs/package.json")),
    "bin",
    "tsrs",
  );
  await runTypecheck(
    ctx,
    compilerPath,
    // Typechecking must not emit files even when a project enables TypeScript emit.
    ["--project", functionsDir, "--noEmit"],
    handleResult,
  );
}

async function runTypecheck(
  ctx: Context,
  compilerPath: string,
  compilerArgs: string[],
  handleResult: TypecheckResultHandler,
) {
  // Run `tsrs` once and have it print out the files it touched. This output won't
  // be very useful if there's an error, but we'll run it again to get a nice
  // user-facing error in this exceptional case.
  // The `--listFiles` command prints out files touched on success or error.
  const result = await spawnAsync(ctx, process.execPath, [
    compilerPath,
    ...compilerArgs,
    // Keep the diagnostic/file-list protocol independent of project formatting.
    "--pretty",
    "false",
    "--listFiles",
  ]);
  if (result.status === null) {
    return handleResult("typecheckFailed", () => {
      logFailure(`TypeScript typecheck could not complete.`);
      if (result.error) {
        logError(chalkStderr.red(`${result.error.toString()}`));
      }
    });
  }
  // Okay, we may have failed `tsrs` but at least it returned. Try to parse its
  // output to discover which files it touched.
  const filesTouched = result.stdout
    .split("\n")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  let anyPathsFound = false;
  for (const fileTouched of filesTouched) {
    const absPath = path.resolve(fileTouched);
    let st;
    try {
      st = ctx.fs.stat(absPath);
      anyPathsFound = true;
    } catch {
      // Just move on if we have a bogus path from `tsrs`. We'll log below if
      // we fail to stat *any* of the paths emitted by `tsrs`.
      // TODO: Switch to using their JS API so we can get machine readable output.
      continue;
    }
    ctx.fs.registerPath(absPath, st);
  }
  if (filesTouched.length > 0 && !anyPathsFound) {
    const err = new Error(
      `Failed to stat any files emitted by tsrs (received ${filesTouched.length})`,
    );
    Sentry.captureException(err);
  }

  if (!result.error && result.status === 0) {
    return handleResult("success");
  }

  // An empty project is valid, but TS18003 must not hide later configuration
  // diagnostics or a launcher failure on stderr.
  if (
    filesTouched.length === 1 &&
    filesTouched[0].startsWith("error TS18003:") &&
    result.stderr.trim() === ""
  ) {
    return handleResult("success");
  }

  // At this point we know that `tsrs` failed. Rerun it without `--listFiles`
  // and with stderr redirected to have it print out a nice error.
  return handleResult(
    "typecheckFailed",
    () => {
      logFailure(`TypeScript typecheck via \`tsrs\` failed.`);
    },
    async () => {
      showSpinner("Collecting TypeScript errors");
      await spawnAsync(
        ctx,
        process.execPath,
        [compilerPath, ...compilerArgs, "--pretty", "true"],
        {
          stdio: "inherit",
        },
      );
      // If this passes, we had a concurrent file change that'll overlap with
      // our observations in the first run. Invalidate our context's filesystem
      // but allow the rest of the system to observe the success.
      ctx.fs.invalidate();
      return "success";
    },
  );
}

function maybeLogSlowTypecheckSuggestion(ctx: Context, durationMs: number) {
  if (!(ctx instanceof WatchContext)) {
    return;
  }
  if (!ctx.isFirstPush) {
    return;
  }
  if (durationMs <= SLOW_TYPECHECK_THRESHOLD_MS) {
    return;
  }
  const formattedDuration = formatDuration(durationMs);
  logMessage(
    chalkStderr.gray(
      `Typechecking took ${formattedDuration}. For faster iteration, see ${SLOW_TYPECHECK_DOCS_URL} to troubleshoot typecheck performance.`,
    ),
  );
}
