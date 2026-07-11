# TypeScript monorepo

## Development workflow

When doing changes in `/npm-packages/<package>`:

```sh
# After each modification, use the package's formatter script when it has one.
# For a package that depends on Prettier but has no formatter script:
cd npm-packages/<package>
npm exec prettier -- --write <changed-files>

# When the change is ready
# Run the package's lint script when package.json defines one.
cd npm-packages/<package>
npm run lint
npm exec prettier -- --check <changed-files>
cd ../..
just turbo run build --filter=<package>...

# To run a specific test file
cd npm-packages/<package>/
npm run test -- <file>
```

## Dependencies management

This project uses pnpm workspaces to manage dependencies.

After modifying the dependencies of a package, run `just update-js`.

## Code organization

The public docs live at https://docs.convex.dev/ and the Convex Cloud dashboard
at https://dashboard.convex.dev/.
