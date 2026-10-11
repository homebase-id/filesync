# Releasing Homebase Filesync

How installers are made: `docs/spec/packaging.md`. The workflow: `.github/workflows/release.yml`.

## A release

1. Set the workspace version (`Cargo.toml`, `[workspace.package] version`), commit, push to
   `main`, and wait for CI.
2. Optionally, run the Release workflow by hand first: it builds and tests everything without
   releasing.
3. Tag and push: `git tag v0.1.0 && git push origin v0.1.0`. The workflow builds, packages and
   tests; then the `release` job waits in the `release-signing` environment for its reviewer.
4. Once approved, it makes a **draft** GitHub release with every file, `SHA256SUMS` and build
   provenance attestations. Check it, then publish it by hand. Approving and publishing are the
   two people of a release.

## What runs where (DevOps' rules)

- `build`, `package`, `dmg`, `rpm` and the tests hold **no secrets**. Their packages carry a
  throwaway Conveyor key made for each run (the bleeding channel's feed; self-signed for Windows
  and macOS): they are for testing.
- Only `release` runs in the **`release-signing` environment**: tags `v*` only, a required
  reviewer, no self-review. Its secrets are environment secrets, never organization secrets.
- Every action is pinned to a commit SHA; Conveyor is pinned by version and SHA-256; the `.rpm`
  is built with Fedora's own `rpmbuild` in Fedora.

## Signing (to do: waiting for DevOps)

DevOps provides, as environment secrets of `release-signing`, keys for filesync alone (not
chat-kmp's, which were in GitHub during the breach):

| What | For | Variables (`packaging/conveyor-production.conf`) |
|---|---|---|
| A Conveyor root key | the update feeds (Sparkle, the apt repository, the `.appinstaller`) | `CONVEYOR_SIGNING_KEY` |
| An Apple Developer ID Application certificate | the Mac app and the `.dmg` | `APPLE_P12_PATH` (staged from the secret), `APPLE_SIGNING_KEY_PASS` |
| An App Store Connect API key (lowest role that notarizes) | notarization | `APPSTORE_API_ISSUER_ID`, `APPSTORE_API_KEY_ID`, `APPLE_NOTARIZATION_KEY` |
| An Azure Trusted Signing certificate profile, through OIDC federated on `repo:homebase-id/filesync:environment:release-signing` | the MSIX and the EXEs | `AZURE_ACCESS_TOKEN` (fetched in the job), `AZURE_SIGNING_PROFILE` |

Then the `release` job runs Conveyor with `conveyor-production.conf` (`make copied-site`: it
signs, notarizes and uploads the site to `homebase-id/filesync-release-production`, as chat-kmp
does), and signs and notarizes the `.dmg` files. Someone creates the release repos first
(`filesync-release-production`, `filesync-release-bleeding`). DevOps may move signing off GitHub
later (their own host, `rcodesign` and `jsign`); the build jobs stay the same.

## Checking a release

- `sha256sum -c SHA256SUMS`
- `gh attestation verify <file> --repo homebase-id/filesync`
