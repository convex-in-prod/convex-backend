import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const dockerfile = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "Dockerfile.backend"),
  "utf8",
);

test("optional source authority helper shares Cargo selection and explicit gate features", () => {
  const selection = dockerfile.slice(
    dockerfile.indexOf("feature_args=()"),
    dockerfile.indexOf("# Build selected native dependency packages"),
  );
  const commands = dockerfile.slice(
    dockerfile.indexOf("# The pinned nightly Cargo"),
    dockerfile.indexOf('test -x "$artifact_dir/convex-local-backend"'),
  );
  for (const parallel of ["", "3"]) {
    for (const enabled of ["false", "true"]) {
      for (const feature of [
        "",
        "jemalloc",
        "static-hermes-wasmtime-gate",
        "local_backend/static-hermes-wasmtime-gate",
        "jemalloc,local_backend/static-hermes-wasmtime-gate",
      ]) {
        if (enabled === "true" && !feature.includes("static-hermes-wasmtime-gate")) {
          continue;
        }
        // Execute only argument construction with stubbed commands; no Cargo, copy or build runs.
        const result = spawnSync(
          "bash",
          [
            "-c",
            `
set -euo pipefail
cargo() { printf '%s\\0' "$@"; printf '\\n'; }
cp() { :; }
cargo_profile_args=()
artifact_dir=/unused-artifacts
${selection}
${commands}
`,
          ],
          {
            encoding: "utf8",
            env: {
              ...process.env,
              BUILD_SOURCE_PACKAGE_PREACTIVATION_AUTHORITY: enabled,
              LOCAL_BACKEND_FEATURES: feature,
              TARGETARCH: "amd64",
              CARGO_BUILD_PROFILE: "release",
              CARGO_TARGET_DIR: "/unused-target",
              RUSTC_PARALLEL_FRONTEND_THREADS: parallel,
            },
          },
        );
        assert.equal(result.status, 0, result.stderr);
        const calls = result.stdout
          .trimEnd()
          .split("\n")
          .map((line) => line.split("\0").filter(Boolean));
        assert.equal(calls.length, parallel === "" ? 1 : 2);
        const backend = calls[parallel === "" ? 0 : 1];
        assert(backend.includes("convex-local-backend"));
        assert.deepEqual(
          backend.flatMap((arg, index) => arg === "--features" ? [backend[index + 1]] : []),
          [
            ...(feature === "" ? [] : [feature]),
            ...(enabled === "true" && parallel === ""
              ? ["application/static-hermes-wasmtime-gate"]
              : []),
          ],
        );
        if (enabled === "false") {
          assert.deepEqual(
            calls[0].flatMap((arg, index) => arg === "-p" ? [calls[0][index + 1]] : []),
            parallel === "" ? ["local_backend", "keybroker"] : ["keybroker"],
          );
          assert.deepEqual(
            calls[0].flatMap((arg, index) => arg === "--bin" ? [calls[0][index + 1]] : []),
            parallel === "" ? ["convex-local-backend", "generate_key"] : ["generate_key"],
          );
        }
        const helperCalls = calls.filter((args) =>
          args.includes("source_package_preactivation_authority"),
        );
        assert.equal(helperCalls.length, enabled === "true" ? 1 : 0);
        if (enabled === "true") {
          const helper = helperCalls[0];
          assert(helper.includes("application/static-hermes-wasmtime-gate"));
          assert(helper.includes("application"));
          assert(helper.includes("local_backend"));
          assert(helper.includes(feature));
          assert(helper.includes("generate_key"));
          assert(helper.includes("--artifact-dir"));
          assert.equal(
            helper.includes("convex-local-backend"),
            parallel === "",
          );
        }
      }
    }
  }
  for (const [enabled, feature, message] of [
    ["yes", "static-hermes-wasmtime-gate", "must be true or false"],
    ["true", "", "requires static-hermes-wasmtime-gate"],
  ]) {
    const result = spawnSync(
      "bash",
      ["-c", `set -euo pipefail\n${selection}`],
      {
        encoding: "utf8",
        env: {
          ...process.env,
          TARGETARCH: "amd64",
          BUILD_SOURCE_PACKAGE_PREACTIVATION_AUTHORITY: enabled,
          LOCAL_BACKEND_FEATURES: feature,
        },
      },
    );
    assert.equal(result.status, 1);
    assert(result.stderr.includes(message));
  }
  assert.match(
    dockerfile,
    /^ARG BUILD_SOURCE_PACKAGE_PREACTIVATION_AUTHORITY=false$/mu,
  );
  assert.match(
    dockerfile,
    /test -x "\$artifact_dir\/source_package_preactivation_authority"/u,
  );
  assert.match(
    dockerfile,
    /COPY --from=build \/convex\/backend-artifacts\/ \.\//u,
  );
});

test("prebuilds selected native dependencies with an independent Cargo jobserver", () => {
  assert.match(dockerfile, /^ARG NATIVE_PREBUILD_JOBS=1$/mu);
  assert.match(dockerfile, /^ARG NATIVE_PREBUILD_PACKAGES=$/mu);
  assert.match(
    dockerfile,
    /CMAKE_BUILD_PARALLEL_LEVEL=\$\{NATIVE_PREBUILD_JOBS\}/u,
  );
  assert.match(dockerfile, /MAKEFLAGS="-j\$\{NATIVE_PREBUILD_JOBS\}"/u);
  assert.match(dockerfile, /CMAKE_C_COMPILER_LAUNCHER=sccache/u);
  assert.match(dockerfile, /CMAKE_CXX_COMPILER_LAUNCHER=sccache/u);
  assert.match(dockerfile, /NATIVE_PREBUILD_JOBS must be a positive integer/u);
  assert.match(
    dockerfile,
    /native_prebuild_package_args\+=\(--package "\$package_spec"\)/u,
  );
  assert.match(
    dockerfile,
    /--jobs "\$NATIVE_PREBUILD_JOBS"[\s\S]*"\$\{native_prebuild_package_args\[@\]\}"/u,
  );
  assert.doesNotMatch(dockerfile, /librocksdb-sys/u);
  assert.match(
    dockerfile,
    /id=convex-backend-sccache-\$\{TARGETARCH\},target=\/sccache,sharing=locked/u,
  );
  assert.match(dockerfile, /^ARG SCCACHE_CACHE_SIZE=$/mu);
  assert.match(
    dockerfile,
    /if \[\[ -n "\$SCCACHE_CACHE_SIZE" \]\]; then\s+export SCCACHE_CACHE_SIZE\s+else\s+unset SCCACHE_CACHE_SIZE/u,
  );
});

test("bounds dependency cooking with the requested Cargo job count", () => {
  const dependencyStageName = dockerfile.indexOf(" AS build-dependencies");
  const dependencyStage = dockerfile.slice(
    dockerfile.lastIndexOf("FROM ", dependencyStageName),
    dockerfile.indexOf("FROM build-dependencies AS build"),
  );
  assert.match(dependencyStage, /^ARG CARGO_BUILD_JOBS=8$/mu);
  assert.match(
    dependencyStage,
    /CARGO_BUILD_JOBS must be a positive integer/u,
  );
  assert.match(
    dependencyStage,
    /cargo chef cook\s+\\\s+--jobs "\$CARGO_BUILD_JOBS"/u,
  );
});

test("can serialize local backend codegen without collapsing codegen units", () => {
  assert.match(dockerfile, /^ARG SERIALIZE_LOCAL_BACKEND_CODEGEN=false$/mu);
  assert.match(
    dockerfile,
    /profile\.release\.package\.local_backend\.rustflags=\["-Zno-parallel-backend"\]/u,
  );
  assert.match(dockerfile, /profile-rustflags/u);
  assert.match(
    dockerfile,
    /profile\.release\.package\.local_backend\.codegen-units=\$local_backend_codegen_units/u,
  );
  assert.doesNotMatch(
    dockerfile,
    /export CARGO_PROFILE_RELEASE_PACKAGE_LOCAL_BACKEND_CODEGEN_UNITS/u,
  );
  assert.match(
    dockerfile,
    /SERIALIZE_LOCAL_BACKEND_CODEGEN requires the release Cargo profile/u,
  );
  assert.match(
    dockerfile,
    /SERIALIZE_LOCAL_BACKEND_CODEGEN must be true or false/u,
  );
});

test("rejects the Static Hermes gate on unsupported image architectures", () => {
  assert.match(
    dockerfile,
    /\$\{LOCAL_BACKEND_FEATURES\/\/,\/ \}/u,
  );
  assert.match(
    dockerfile,
    /static-hermes-wasmtime-gate\|local_backend\/static-hermes-wasmtime-gate/u,
  );
  assert.match(
    dockerfile,
    /static_hermes_wasmtime_gate_selected[\s\S]*"\$TARGETARCH" != amd64/u,
  );
  assert.match(
    dockerfile,
    /static-hermes-wasmtime-gate requires Docker target architecture amd64/u,
  );
});
