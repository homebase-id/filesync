# Releasing Homebase Filesync

How installers are made: `docs/spec/packaging.md`. The workflow: `.github/workflows/release.yml`.

## A release

1. Set the workspace version (`Cargo.toml`, `[workspace.package] version`), commit, push to
   `main`, and wait for CI.
2. Optionally, run the Release workflow by hand first: it builds and tests everything without
   releasing.
3. Tag and push: `git tag v0.1.0 && git push origin v0.1.0`. The workflow builds, packages and
   tests; then the `release` job (in the `release-signing` environment, which only `v*` tags
   reach) makes a **draft** GitHub release with every file, `SHA256SUMS` and build provenance
   attestations.
4. Check the draft, then publish it by hand: the one human step of a release.

## What runs where (DevOps' rules)

- `build`, `package`, `dmg`, `rpm` and the tests hold **no secrets**. Their packages carry a
  throwaway Conveyor key made for each run (the bleeding channel's feed; self-signed for Windows
  and macOS): they are for testing.
- Only `release` runs in the **`release-signing` environment**: tags `v*` only (no required
  reviewer, so the team can release; DevOps, 2026-10-11). Its secrets are environment secrets,
  never organization secrets.
- The repository allows only GitHub's own actions and `Swatinem/rust-cache`, pinned by SHA; the
  default token is read-only, so each job asks for the permissions it needs.
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

The secrets go in the signing step's own `env:` (not the job's, where the other steps would see
them); the `.p12` is staged in `$RUNNER_TEMP` and removed in an `if: always()` step; checksums
and attestations are made after signing. The Conveyor root key is generated off GitHub, its
backup in the DevOps vault. A repository ruleset for `v*` tags (only maintainers create, move or
delete them) backs the environment's tag rule, which checks only the name.

Then, as DevOps proposed:

1. **Sign once, in the tag's `release` job.** Conveyor `make site` with
   `conveyor-production.conf` (it signs and notarizes), and the `.dmg` files signed and
   notarized. The whole signed site, update feeds included, goes onto the draft with
   `SHA256SUMS` and the attestations.
2. **Publish what was looked at.** A second workflow, on `release: published`, in its own
   environment (`release-publish`, `v*` tags only) that holds no signing keys, only a GitHub
   App's key for the release repo:
   - it downloads the draft's files;
   - it checks them (`sha256sum -c SHA256SUMS`, `gh attestation verify`);
   - it uploads them unchanged to `filesync-release-production`.

   It never runs Conveyor again: signing again would give other bytes than the ones published.
   Publishing the draft is then the human check for the update feed too, not only for the
   GitHub release, and the signing keys and the upload credential never meet in one job. Someone creates the release repos first
(`filesync-release-production`, `filesync-release-bleeding`). DevOps may move signing off GitHub
later (their own host, `rcodesign` and `jsign`); the build jobs stay the same.

## Checking a release

- `sha256sum -c SHA256SUMS`
- `gh attestation verify <file> --repo homebase-id/filesync`
