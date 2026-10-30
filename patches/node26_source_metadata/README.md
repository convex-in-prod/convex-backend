# Node.js 26 source-package metadata

This patch lets a backend accept and read `nodeVersion: "26"` in source-package
metadata. It adds Node.js 26 to the existing version parser, serializer, and
supported-version check. Existing version values keep their meaning; no data
migration is required to install the patch.

The patch does not install Node.js or select an executable. A self-hosted
deployment uses its installed Node runtime for local actions. The maintained
[local executor](../local_node_executor_resilience/README.md) selects `node`
from `PATH`; its deployment-wide version pin and startup check are separate
from this metadata change. Applications cannot select different local binaries
through source-package metadata.

Install a backend that recognizes the new value before deploying functions
whose source metadata declares Node.js 26. An older backend cannot read source
records containing the new value, so replacing that backend is not a transparent
rollback after those records have been written.

This small patch can be dropped when upstream recognizes the same value.
Deployments using the [Wasm patch](../static_hermes_wasm_udfs/README.md) also need
its matching source-identity handling; those integrations stay in the Wasm patch.
