# Releasing Homebase Filesync

How installers are made: `docs/spec/packaging.md`. The workflow: `.github/workflows/release.yml`.

## A release

1. Set the workspace version (`Cargo.toml`, `[workspace.package] version`), commit, push to
   `main`, and wait for CI.
2. Optionally, run the Release workflow by hand first: it builds and tests everything without
   releasing. To test the signed packages too, tag a release candidate (`v0.1.0-rc.1`): it makes
   a draft marked pre-release, which publishing never puts on the update site.
3. Tag and push: `git tag v0.1.0 && git push origin v0.1.0`. The workflow builds, packages and
   tests, signs (`sign`, in the `release-signing` environment, which only `v*` tags reach), and
   makes a **draft** GitHub release with every file, `SHA256SUMS` and build provenance
   attestations.
4. Check the draft, then publish it by hand: the one human step of a release. Publishing runs
   `publish.yml`, which puts exactly those files on the update site: from then on, installed
   copies update to it.

## What runs where (DevOps' rules)

- `build`, `package`, `dmg`, `rpm` and the tests hold **no secrets**. Their packages carry a
  throwaway Conveyor key made for each run (the bleeding channel's feed; self-signed for Windows
  and macOS): they are for testing.
- Only `sign` runs in the **`release-signing` environment**, and only `publish.yml` in
  **`release-publish`**: tags `v*` only (no required reviewer, so the team can release; DevOps,
  2026-10-11). Their secrets are environment secrets, never organization secrets, and the
  signing keys and the upload credential are never in one job.
- The repository allows only GitHub's own actions and `Swatinem/rust-cache`, each pinned to a
  commit SHA; the default token is read-only, so each job asks for the permissions it needs.
  Conveyor is pinned by version and SHA-256, the Fedora image that builds the `.rpm` by digest.

## Signing

Keys for filesync alone (not chat-kmp's, which were in GitHub during the breach), set up by
DevOps (their PR #227). In the `release-signing` environment:

| What | For | Secrets (`secrets.*`) and variables (`vars.*`) |
|---|---|---|
| A Conveyor root key, made off GitHub (backup in the DevOps vault): **set** | the update feeds (Sparkle, the apt repository, the `.appinstaller`), and the self-signed certificates until the ones below exist | `CONVEYOR_SIGNING_KEY` |
| An Apple Developer ID Application certificate (.p12 with the G2 intermediate) | the Mac app and the `.dmg` | `APPLE_SIGNING_KEY` (base64: decoded to `$RUNNER_TEMP`, `APPLE_P12_PATH`), `APPLE_SIGNING_KEY_PASS` |
| An App Store Connect API key (lowest role that notarizes) | notarization | `APPLE_NOTARIZATION_KEY` (the .p8), `APPSTORE_API_KEY_ID`, `APPSTORE_API_ISSUER_ID` |
| An Azure Trusted Signing certificate profile | the MSIX and the EXEs | vars `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SIGNING_PROFILE`; the token by OIDC in the job |

The Apple and Azure ones aren't set yet (2026-10-11): until they are, the Mac app and the
Windows packages are self-signed (the release notes say how to open them), and each joins by
itself once its secrets exist: the `sign` job adds `packaging/conveyor-apple.conf` and
`conveyor-azure.conf` to `conveyor-production.conf` when it finds them. To do then: sign and
notarize the `.dmg` files themselves too (the app inside is signed by Conveyor). Packages
signed by the self-signed certificates can't update to ones signed by the real ones (another
publisher): those installs are reinstalled once.

The Azure token: there is no `azure/login` (not an allowed action), so the job asks GitHub for an
OIDC token
(`ACTIONS_ID_TOKEN_REQUEST_URL`, audience `api://AzureADTokenExchange`), exchanges it with `curl`
at `https://login.microsoftonline.com/$AZURE_TENANT_ID/oauth2/v2.0/token` (client credentials,
a JWT-bearer client assertion, scope `https://codesigning.azure.net/.default`), and masks it
(`::add-mask::`) first thing. The federated subject is
`repo:homebase-id/filesync:environment:release-signing`.

The secrets go in the signing step's own `env:` (not the job's, where the other steps would see
them); the `.p12` is staged in `$RUNNER_TEMP` and removed in an `if: always()` step. A repository
ruleset for `v*` tags (only maintainers create, move or delete them) would back the environment's
tag rule, which checks only the name.

As DevOps proposed:

1. **Sign once, on the tag (`sign`).** Conveyor `make site` with `conveyor-production.conf`. The
   whole signed site, update feeds included, goes onto the draft (`release`) with the disk
   images, the `.rpm` files, `SHA256SUMS` and the attestations, made after signing.
2. **Publish what was looked at (`publish.yml`, on `release: published`).** In its own
   environment `release-publish` (`v*` tags only), which holds no signing keys, only a GitHub
   App's (secret `RELEASE_APP_PRIVATE_KEY`, variable `RELEASE_APP_ID`; app
   `homebase-filesync-release-pub`, installed on the two update-site repos only; a token from
   `actions/create-github-app-token`, limited to `filesync-release-production` with
   `contents: write`):
   - it downloads the draft's files;
   - it checks them (`sha256sum -c SHA256SUMS`, `gh attestation verify`);
   - it uploads them unchanged as the newest release of `homebase-id/filesync-release-production`,
     the update site (`releases/latest/download`).

   It never runs Conveyor again: signing again would give other bytes than the ones published.
   Publishing the draft is then the human check for the update feed too, not only for the
   GitHub release, and the signing keys and the upload credential never meet in one job.

The update sites are `homebase-id/filesync-release-production` and `-bleeding` (public, Actions
off). DevOps may move signing off GitHub later (their own host, `rcodesign` and `jsign`); the
build jobs stay the same.

## Links that always get the newest release

Every release also carries copies without the version in their names (the `release` job), so
these links keep working from one release to the next (chat's setup screen uses them). They
start working with the first published final release, since pre-releases never reach the update site:

| OS | https://github.com/homebase-id/filesync-release-production/releases/latest/download/… |
|---|---|
| macOS | `Homebase-Filesync-mac-aarch64.dmg`, `Homebase-Filesync-mac-amd64.dmg` |
| Windows | `homebase-filesync.exe` (Conveyor's installer: it picks the MSIX; needs the signed package) |
| Ubuntu, Debian | `homebase-filesync_amd64.deb`, `homebase-filesync_arm64.deb` |
| Others | the releases page: https://github.com/homebase-id/filesync-release-production/releases/latest |

## Checking a release

- `sha256sum -c SHA256SUMS`
- `gh attestation verify <file> --repo homebase-id/filesync`
