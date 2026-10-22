import { chalkStderr } from "chalk";
import { functionsDir, ensureHasConvexDependency } from "./lib/utils/utils.js";
import { Command } from "@commander-js/extra-typings";
import { readProjectConfig } from "./lib/config.js";
import { typeCheckFunctions } from "./lib/typecheck.js";
import { oneoffContext } from "../bundler/context.js";
import { logFinishedStep, logMessage } from "../bundler/log.js";

// Experimental (it's going to fail sometimes) TypeScript type checking.
// Includes a separate command to help users debug their TypeScript configs.

export const typecheck = new Command("typecheck")
  .description(
    "Run TypeScript typechecking on your Convex functions with tsrs.",
  )
  .allowExcessArguments(false)
  .action(async () => {
    const ctx = await oneoffContext({
      url: undefined,
      adminKey: undefined,
      envFile: undefined,
    });
    // Read configuration without bundling so compiler diagnostics remain reachable.
    const { configPath, projectConfig } = await readProjectConfig(ctx);
    await ensureHasConvexDependency(ctx, "typecheck");
    await typeCheckFunctions(
      ctx,
      functionsDir(configPath, projectConfig),
      async (typecheckResult, logSpecificError, runOnError) => {
        logSpecificError?.();
        if (typecheckResult === "typecheckFailed") {
          logMessage(chalkStderr.gray("Typecheck failed"));
          try {
            await runOnError?.();
            // If runOnError doesn't throw then it worked the second time.
            // No errors to report, but it's still a failure.
          } catch {
            // As expected, `runOnError` threw
          }
          return await ctx.crash({
            exitCode: 1,
            errorType: "invalid filesystem data",
            printedMessage: null,
          });
        } else if (typecheckResult === "cantTypeCheck") {
          logMessage(
            chalkStderr.gray(
              "Unable to typecheck; is convex/tsconfig.json present?",
            ),
          );
          return await ctx.crash({
            exitCode: 1,
            errorType: "invalid filesystem data",
            printedMessage: null,
          });
        } else {
          logFinishedStep("Typecheck passed.");
          return await ctx.flushAndExit(0);
        }
      },
    );
  });
