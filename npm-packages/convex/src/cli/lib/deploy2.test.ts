import { brotliDecompressSync } from "node:zlib";
import { afterEach, expect, test, vi } from "vitest";
import type { Context } from "../../bundler/context.js";
import { nodeFs } from "../../bundler/fs.js";
import type { ComponentDirectory } from "./components/definition/directoryStructure.js";
import {
  startPushRequest,
  type CodegenAnalysis,
  type StartPushRequest,
} from "./deployApi/startPush.js";
import { Span } from "./tracing.js";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
});

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

function pushRequest(forCodegen: boolean): StartPushRequest {
  return {
    adminKey: "test-admin-key",
    dryRun: false,
    functions: "convex/",
    appDefinition: {
      definition: null,
      dependencies: [],
      schema: null,
      changedModules: [],
      unchangedModuleHashes: [],
      udfServerVersion: "1.0.0",
    },
    componentDefinitions: [],
    nodeDependencies: [],
    forCodegen,
  };
}

const analysis = {
  "": {
    definition: {
      path: "",
      definitionType: { type: "app" },
      childComponents: [],
      httpMounts: {},
      exports: { type: "branch", branch: [] },
      envVars: [],
    },
    schema: null,
    functions: {
      "messages.js": {
        functions: [
          {
            name: "hello",
            pos: null,
            udfType: "Query",
            visibility: { kind: "public" },
            args: null,
            returns: null,
          },
        ],
        httpRoutes: null,
        cronSpecs: null,
        sourceMapped: null,
      },
    },
    udfConfig: {
      serverVersion: "1.0.0",
      importPhaseRngSeed: null,
      importPhaseUnixTimestamp: null,
    },
  },
};

const schemaChange = {
  allocatedComponentIds: {},
  schemaIds: {},
  indexDiffs: {},
};

function stubFetchResponse(body: unknown) {
  const fetchMock = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function requestPath(input: RequestInfo | URL): string {
  const url = input instanceof Request ? input.url : input.toString();
  return new URL(url).pathname;
}

function parsePushRequest(init: RequestInit | undefined): StartPushRequest {
  const body = init?.body;
  if (!Buffer.isBuffer(body)) {
    throw new Error("Expected a Brotli-compressed request buffer");
  }
  const json = brotliDecompressSync(body).toString("utf8");
  return startPushRequest.parse(JSON.parse(json));
}

const requestOptions = {
  url: "https://example.test",
  deploymentName: null,
};

test("standalone codegen evaluates both app and component requests and consumes analysis", async () => {
  const fetchMock = stubFetchResponse({ analysis, schemaChange });
  const { startOrEvaluatePush } = await import("./deploy2.js");
  let appCodegenAnalysis: CodegenAnalysis | undefined;

  for (const forCodegen of [false, true]) {
    const result = await startOrEvaluatePush(
      testContext(),
      Span.noop(),
      pushRequest(forCodegen),
      requestOptions,
      "codegen",
    );
    if (result.kind !== "codegen") {
      throw new Error("Codegen selected start_push");
    }
    expect(
      result.response.analysis[""]?.functions["messages.js"],
    ).toBeDefined();
    if (!forCodegen) {
      appCodegenAnalysis = result.response;
    }
  }

  expect(fetchMock).toHaveBeenCalledTimes(2);
  for (const [index, [input, init]] of fetchMock.mock.calls.entries()) {
    expect(requestPath(input)).toBe("/api/deploy2/evaluate_push");
    const request = parsePushRequest(init);
    expect(request.includeAnalysis).toBe(true);
    expect(request.forCodegen).toBe(index === 1);
  }

  const { componentApiTSWithTypes } =
    await import("../codegen_templates/component_api.js");
  const rootComponent: ComponentDirectory = {
    isRoot: true,
    path: "/project/convex",
    definitionPath: "/project/convex/convex.config.ts",
    isRootWithoutConfig: false,
  };
  if (appCodegenAnalysis === undefined) {
    throw new Error("App codegen did not return analysis");
  }
  const generated = await componentApiTSWithTypes(
    testContext(),
    appCodegenAnalysis,
    rootComponent,
    rootComponent,
    new Map([[rootComponent.path, rootComponent]]),
    { staticApi: true, useComponentApiImports: false },
  );
  expect(generated).toContain('"hello":');
});

test("standalone codegen rejects a legacy evaluate response without falling back", async () => {
  const fetchMock = stubFetchResponse({ schemaChange });
  const { startOrEvaluatePush } = await import("./deploy2.js");

  await expect(
    startOrEvaluatePush(
      testContext(),
      Span.noop(),
      pushRequest(false),
      requestOptions,
      "codegen",
    ),
  ).rejects.toThrow(
    "backend version does not support non-committing code generation",
  );

  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(requestPath(fetchMock.mock.calls[0][0])).toBe(
    "/api/deploy2/evaluate_push",
  );
});

test("normal push mode still selects start_push without requesting analysis", async () => {
  const fetchMock = stubFetchResponse({
    environmentVariables: {},
    externalDepsId: null,
    componentDefinitionPackages: {},
    appAuth: [],
    analysis,
    app: {
      definitionPath: "",
      componentPath: "",
      args: {},
      childComponents: {},
      httpRoutes: { httpModuleRoutes: null, mounts: [] },
      exports: {},
    },
    schemaChange,
  });
  const { startOrEvaluatePush } = await import("./deploy2.js");

  const result = await startOrEvaluatePush(
    testContext(),
    Span.noop(),
    pushRequest(false),
    requestOptions,
    "startPush",
  );

  expect(result.kind).toBe("startPush");
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const [input, init] = fetchMock.mock.calls[0];
  expect(requestPath(input)).toBe("/api/deploy2/start_push");
  expect(parsePushRequest(init).includeAnalysis).toBeUndefined();
});
