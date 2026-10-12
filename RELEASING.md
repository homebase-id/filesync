# Releasing Homebase Filesync

How installers are made: `docs/spec/packaging.md`. The workflow: `.github/workflows/release.yml`.

## A release

1. Set the workspace version (`Cargo.toml`, `[workspace.package] version`), commit, push to
   `main`, and wait for CI.
2. Optionally, run the Release workflow by hand first: it builds and tests everything without
   releasing. To test the signed packages too, tag a release candidate (`v0.1.0-rc.1`): it makes
   a draft marked pre-release, which publishing never puts on the update site.
3. Tag and push: `git tag -m "v0.1.0" v0.1.0 && git push origin v0.1.0`. The tag is signed
   (`tag.gpgsign`), so it needs a message: without `-m`, git stops with "no tag message?".
   The workflow builds, packages and tests, signs (`sign` and `sign-dmg`, in the
   `release-signing` environment, which only `v*` tags reach), and makes a **draft** GitHub
   release with every file, `SHA256SUMS` and build provenance attestations.
4. Check the draft, then publish it: the one human step of a release. Publishing runs
   `publish.yml`, which puts exactly those files on the update site: from then on, installed
   copies update to it. The user may publish with the button or ask a Claude session to,
   which runs `gh release edit <tag> --draft=false` (it triggers `publish.yml` the same way),
   but only on the user's explicit go for that release, given after the session's checks of the
   draft. A passing build is not a go.

## What runs where (DevOps' rules)

- `build`, `package`, `dmg`, `rpm` and the tests hold **no secrets**. Their packages carry a
  throwaway Conveyor key made for each run (the bleeding channel's feed; self-signed for Windows
  and macOS): they are for testing.
- Only `sign` and `sign-dmg` run in the **`release-signing` environment** (the second on a
  Mac, for the disk images), and only `publish.yml` in
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
| A Conveyor root key, made off GitHub (backup in the DevOps vault) | the update feeds (Sparkle, the apt repository, the `.appinstaller`) | `CONVEYOR_SIGNING_KEY` |
| An Apple Developer ID Application certificate (.p12 with the G2 intermediate; "Developer ID Application: Vidence, LLC (Apps) (JNJ85M8JM6)", until 2031-09-17) | the Mac app (`sign`) and the `.dmg` files (`sign-dmg`) | `APPLE_SIGNING_KEY` (base64), `APPLE_SIGNING_KEY_PASS` |
| An App Store Connect API key (lowest role that notarizes) | notarization | `APPLE_NOTARIZATION_KEY` (the .p8), `APPSTORE_API_KEY_ID`, `APPSTORE_API_ISSUER_ID` |
| An Azure Trusted Signing certificate profile | the MSIX and the EXEs | vars `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SIGNING_ENDPOINT`, `AZURE_SIGNING_ACCOUNT`, `AZURE_SIGNING_PROFILE` (Conveyor's alias is `<account>/<profile>`); the token by OIDC in the job |

All are set (2026-10-12; first fully signed: v0.1.0-rc.6). `sign` refuses to start if any is
missing: a tag is never signed by halves. It signs with `conveyor-production.conf`,
`packaging/conveyor-apple.conf` and `conveyor-azure.conf`. `sign-dmg` signs, notarizes and
staples the disk images on a Mac, then checks them and the app inside as Gatekeeper does;
`test-signed-msix` installs each signed MSIX (Intel and ARM) on a fresh Windows runner without
trusting any certificate first and runs `signtool verify` on every package. Installs of the
self-signed v0.1.0-rc.1 can't update to signed ones (another publisher): they are reinstalled
once.

The .p12 as DevOps made it doesn't suit every tool, so `packaging/apple-p12.sh` writes what
each needs (the key piped, never in the clear on disk): for Conveyor the certificate alone and
a .p12 of it and the key (Conveyor refuses one with the G2 intermediate); for macOS's keychain
the .p12 again in the older format `security import` reads (3DES, SHA-1 MAC; it can't read
OpenSSL 3's AES and SHA-256).

The Azure token: there is no `azure/login` (not an allowed action), so the job asks GitHub for an
OIDC token
(`ACTIONS_ID_TOKEN_REQUEST_URL`, audience `api://AzureADTokenExchange`), exchanges it with `curl`
at `https://login.microsoftonline.com/$AZURE_TENANT_ID/oauth2/v2.0/token` (client credentials,
a JWT-bearer client assertion, scope `https://codesigning.azure.net/.default`), and masks it
(`::add-mask::`) first thing; Azure's error, if it refuses, is shown. The repository uses
immutable OIDC subjects (`use_immutable_subject`, don't change it), so the federated subject
is `repo:homebase-id@80376795/filesync@1410799080:environment:release-signing`. The Azure app's
only role is Trusted Signing Certificate Profile Signer on the profile: it sees no
subscription, so nothing here may need one.

The secrets go in the signing step's own `env:` (not the job's, where the other steps would see
them); the keys are staged in a `mkdir -m 700` folder in `$RUNNER_TEMP` (`sign-dmg`: imported
into a keychain of its own there, put on the search list where `codesign` looks) and removed
in an `if: always()` step. Anyone with write access may push a `v*` tag, with no reviewer (the
user's decision, 2026-10-12), but only a commit on `main` is signed: the `version` job refuses
a tag on any other commit, so what gets signed has been through a pull request. (The
environment's tag rule checks only the name; the tag's own signature isn't checked.)

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
