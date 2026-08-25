# local_backend

## OpenAPI specs

HTTP handlers annotated with `#[utoipa::path(...)]` feed three OpenAPI doc roots
in `src/router.rs`, each with a checked-in spec:

| Doc root          | Served at                     | Checked-in spec                                                    |
| ----------------- | ----------------------------- | ------------------------------------------------------------------ |
| `PlatformApiDoc`  | `/api/v1/openapi.json`        | `npm-packages/@convex-dev/platform/deployment-openapi.json`        |
| `PublicApiDoc`    | `/api/public_openapi.json`    | `npm-packages/@convex-dev/platform/public-deployment-openapi.json` |
| `DashboardApiDoc` | `/api/dashboard_openapi.json` | `npm-packages/dashboard/dashboard-deployment-openapi.json`         |

After changing anything that feeds the specs — a route, its
`#[utoipa::path(...)]` annotation, or a docstring baked into a description —
regenerate the affected checked-in JSON specs and TypeScript clients alongside
the Rust change.

For the public API spec, run this command from the repository root:

```sh
UPDATE_API_SPECS=1 scripts/run_cargo.sh test -p local_backend --lib test_public_api_spec_matches
```

Without `UPDATE_API_SPECS`, the same test checks that the generated public spec
matches the checked-in file. This checkout does not define the
`just generate-api-specs` recipe referenced by some package scripts.
