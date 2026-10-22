# Native TypeScript checking with tsrs

The Node executor build uses `tsrs@0.18.0-ts7.1.0-dev.20260929` for its no-emit
typecheck. esbuild still produces both runtime bundles. Its TypeScript
configuration uses bundler module resolution to match that build instead of the
removed Node 10 resolver. Cargo watches this configuration as well as the
package manifest and sources before invoking the filtered Turbo build.

The CLI uses its pinned tsrs dependency for all application typechecks. It no
longer exposes a compiler selector in convex.json or the typecheck command.
Existing typecheck enable, try and disable modes remain, as do watched-file
registration and diagnostic retries. The standalone SDK carries the same
behavior and dependency. No separate application installation of tsrs is needed.
The enable mode requires a tsconfig; try skips projects without one; disable
skips checking. A missing or unusable installed compiler fails a configured
check in both enable and try modes. Application checks always pass `--noEmit`.

This is a build-tool patch with no backend wire-protocol dependency. Older
convex-js releases that use tsc remain compatible: the backend receives the same
JavaScript and deployment requests. It does not require clients to upgrade. tsrs
0.18 supports Linux x64/arm64 and macOS arm64 and implements TypeScript 7.1
development semantics. Unsupported hosts cannot run the new CLI's application
typechecks. The SDK also uses tsrs for source checking and declaration emission.
Public and internal declarations are emitted once each and copied into the
matching ESM/CommonJS layouts, with distinct package markers and unchanged map
depths. esbuild remains the SDK JavaScript emitter.

The maintained self-hosted build paths use tsrs as follows:

- System UDFs are checked without emitting an unused ES5 tree. The Rust build
  bundles their original sources through esbuild; dashboard imports also refer
  to those sources. The package no longer advertises nonexistent dist
  entrypoints, and Turbo caches only the check. Cargo also watches the sources,
  manifests and TypeScript configurations of the embedded id-encoding and
  udf-syscall-ffi packages so their changes reach the bundler.
- The UDF runtime is checked against its existing DOM interface subset. Browser
  Event Timing's interaction count is not part of this server runtime contract.
- id-encoding, udf-syscall-ffi and shapes use tsrs for ESM/declaration emission.
  esbuild transforms those emitted JavaScript files to CommonJS. Declarations
  and maps retain both package layouts; CommonJS still targets ES2015.
- The design system and analytics package use tsrs for emission. The design
  system retains tsc-alias for rewriting source aliases; that tool is not a
  checker. Its rewrite configuration excludes React type-resolution overrides,
  so emitted JavaScript and declarations keep ordinary React package imports.
- dashboard-common uses tsrs for checking. The self-hosted dashboard runs Next
  route-type generation and tsrs before Next builds assets, disabling only
  Next's duplicate tsc check. The generated `.next/types` validators are
  included explicitly because ordinary source globs exclude hidden directories.
  Modern check targets retain assignment-style class fields; removed baseUrl
  lookup is expressed with equivalent relative paths.
- The platform client's explicit source check uses tsrs. Its zshy package
  builder still uses the TypeScript JavaScript API and its declaration/import
  transforms. It is not a replaceable tsc command. Cloud-only dashboard recipes,
  demos and TypeScript-version compatibility fixtures are outside this
  migration.

TypeScript remains installed where API consumers such as zshy, Next,
documentation or lint tooling need it. This does not give the CLI a checker
fallback.

Reverting this patch restores the previous TypeScript build recipes and CLI
compiler selection. It does not require a running service restart.
