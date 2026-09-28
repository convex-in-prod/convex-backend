import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { access, mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const cargoRunner = join(scriptDirectory, "run_cargo.sh");

async function setupFakeCargo(t) {
  const root = await mkdtemp(join(tmpdir(), "run-cargo-access-stamp-test-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const binDirectory = join(root, "bin");
  const buildDirectory = join(root, "build");
  await Promise.all([mkdir(binDirectory), mkdir(buildDirectory)]);
  const cargo = join(binDirectory, "cargo");
  await writeFile(
    cargo,
    `#!/bin/sh
if [ "$1" = metadata ]; then
  shift
  metadata_manifest_path=
  metadata_config_seen=0
  metadata_frozen_seen=0
  metadata_locked_seen=0
  metadata_offline_seen=0
  metadata_target_config_seen=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --target-dir|--target-dir=*)
        echo "metadata received unsupported --target-dir" >&2
        exit 44
        ;;
      --manifest-path)
        metadata_manifest_path=$2
        shift 2
        ;;
      --manifest-path=*)
        metadata_manifest_path=\${1#--manifest-path=}
        shift
        ;;
      --config)
        if [ "$2" = "$FAKE_EXPECTED_METADATA_CONFIG" ]; then
          metadata_config_seen=1
        fi
        if [ "$2" = "$FAKE_EXPECTED_METADATA_TARGET_CONFIG" ]; then
          metadata_target_config_seen=1
        fi
        shift 2
        ;;
      --config=*)
        metadata_config=\${1#--config=}
        if [ "$metadata_config" = "$FAKE_EXPECTED_METADATA_CONFIG" ]; then
          metadata_config_seen=1
        fi
        if [ "$metadata_config" = "$FAKE_EXPECTED_METADATA_TARGET_CONFIG" ]; then
          metadata_target_config_seen=1
        fi
        shift
        ;;
      --frozen)
        metadata_frozen_seen=1
        shift
        ;;
      --locked)
        metadata_locked_seen=1
        shift
        ;;
      --offline)
        metadata_offline_seen=1
        shift
        ;;
      *)
        shift
        ;;
    esac
  done
  if [ -n "$FAKE_EXPECTED_METADATA_MANIFEST_PATH" ] && [ "$metadata_manifest_path" != "$FAKE_EXPECTED_METADATA_MANIFEST_PATH" ]; then
    echo "metadata manifest path was not propagated" >&2
    exit 41
  fi
  if [ -n "$FAKE_EXPECTED_METADATA_CONFIG" ] && [ "$metadata_config_seen" -ne 1 ]; then
    echo "metadata config was not propagated" >&2
    exit 42
  fi
  if [ -n "$FAKE_EXPECTED_METADATA_TARGET_CONFIG" ] && [ "$metadata_target_config_seen" -ne 1 ]; then
    echo "metadata target directory config was not propagated" >&2
    exit 43
  fi
  if [ -n "$FAKE_EXPECTED_METADATA_CONSTRAINTS" ] &&
     { [ "$metadata_frozen_seen" -ne 1 ] || [ "$metadata_locked_seen" -ne 1 ] || [ "$metadata_offline_seen" -ne 1 ]; }; then
    echo "metadata lock and network constraints were not propagated" >&2
    exit 45
  fi
  printf '{"build_directory":"%s"}\\n' "$FAKE_METADATA_BUILD_DIRECTORY"
  exit 0
fi
exit "\${FAKE_CARGO_EXIT:-0}"
`,
    { mode: 0o700 },
  );
  return { binDirectory, buildDirectory, root };
}

function runCargo({
  binDirectory,
  buildDirectory,
  cargoArguments = ["build"],
  exitCode = "0",
  expectedMetadataConfig = "",
  expectedMetadataConstraints = "",
  expectedMetadataManifestPath = "",
  expectedMetadataTargetConfig = "",
  metadataBuildDirectory = buildDirectory,
}) {
  return spawnSync(cargoRunner, cargoArguments, {
    cwd: scriptDirectory,
    encoding: "utf8",
    env: {
      ...process.env,
      FAKE_BUILD_DIRECTORY: buildDirectory,
      FAKE_CARGO_EXIT: exitCode,
      FAKE_EXPECTED_METADATA_CONFIG: expectedMetadataConfig,
      FAKE_EXPECTED_METADATA_CONSTRAINTS: expectedMetadataConstraints,
      FAKE_EXPECTED_METADATA_MANIFEST_PATH: expectedMetadataManifestPath,
      FAKE_EXPECTED_METADATA_TARGET_CONFIG: expectedMetadataTargetConfig,
      FAKE_METADATA_BUILD_DIRECTORY: metadataBuildDirectory,
      PATH: `${binDirectory}${delimiter}${process.env.PATH}`,
      PROTOC: "/bin/true",
    },
  });
}

test("stamps the resolved build directory after a successful Cargo command", async (t) => {
  const fixture = await setupFakeCargo(t);

  const result = runCargo(fixture);

  assert.equal(result.status, 0, result.stderr);
  await assert.doesNotReject(() =>
    access(join(fixture.buildDirectory, ".cargo-build-last-access")),
  );
});

test("does not stamp a build directory after Cargo fails", async (t) => {
  const fixture = await setupFakeCargo(t);

  const result = runCargo({ ...fixture, exitCode: "17" });

  assert.equal(result.status, 17, result.stderr);
  await assert.rejects(() =>
    access(join(fixture.buildDirectory, ".cargo-build-last-access")),
  );
});

test("propagates build context before stamping an explicit target directory", async (t) => {
  const fixture = await setupFakeCargo(t);
  const targetDirectory = join(fixture.root, "explicit-target");
  const buildDirectory = join(fixture.root, "explicit-build");
  const manifestPath = join(fixture.root, "workspace", "Cargo.toml");
  const config = `build.build-dir="${buildDirectory}"`;
  await Promise.all([mkdir(targetDirectory), mkdir(buildDirectory)]);

  const result = runCargo({
    ...fixture,
    cargoArguments: [
      "build",
      "--target-dir",
      targetDirectory,
      "--manifest-path",
      manifestPath,
      "--config",
      config,
      "--frozen",
      "--locked",
      "--offline",
    ],
    expectedMetadataConfig: config,
    expectedMetadataConstraints: "yes",
    expectedMetadataManifestPath: manifestPath,
    expectedMetadataTargetConfig: `build.target-dir="${targetDirectory}"`,
    metadataBuildDirectory: buildDirectory,
  });

  assert.equal(result.status, 0, result.stderr);
  await assert.doesNotReject(() =>
    access(join(buildDirectory, ".cargo-build-last-access")),
  );
  await assert.rejects(() =>
    access(join(fixture.buildDirectory, ".cargo-build-last-access")),
  );
});
