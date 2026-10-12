#!/bin/bash
# Writes DevOps' Developer ID .p12 (APPLE_SIGNING_KEY, base64: the certificate, its key and
# Apple's G2 intermediate; its password in APPLE_SIGNING_KEY_PASS) into <dir> (mode 700) in the
# form each tool takes (the release workflow):
#   conveyor <dir>   developerid.cer, the certificate alone (DER), and developerid-only.p12, it
#                    and the key: Conveyor refuses a certificate file or a .p12 with the
#                    intermediate in it
#   keychain <dir>   developerid-mac.p12, the whole .p12 in the older format (3DES, SHA-1 MAC)
#                    that macOS's `security import` reads; it fails on OpenSSL 3's default
#                    (AES, SHA-256 MAC) with "MAC verification failed"
# The key goes from one openssl to the next through pipes, never in the clear on disk.
# OPENSSL names an OpenSSL 3 (on macOS Homebrew's: the system's is LibreSSL).
set -euo pipefail

if [[ $# -ne 2 || ! $1 =~ ^(conveyor|keychain)$ ]]; then
  echo "usage: $0 conveyor|keychain <dir>" >&2
  exit 2
fi
if [[ -z "${APPLE_SIGNING_KEY:-}" || -z "${APPLE_SIGNING_KEY_PASS:-}" ]]; then
  echo "::error::APPLE_SIGNING_KEY and APPLE_SIGNING_KEY_PASS must be set" >&2
  exit 1
fi
openssl=${OPENSSL:-openssl}
dir=$2
p12="$dir/developerid.p12"
# Wrapped or on one line: base64 tools differ on which they take, so the line breaks go first.
printf '%s' "$APPLE_SIGNING_KEY" | tr -d '\r\n ' | "$openssl" base64 -d -A > "$p12"

legacy=()
"$openssl" pkcs12 -in "$p12" -passin env:APPLE_SIGNING_KEY_PASS -noout 2> /dev/null ||
  legacy=(-legacy)
read_p12() {
  "$openssl" pkcs12 "${legacy[@]}" -in "$p12" -passin env:APPLE_SIGNING_KEY_PASS "$@"
}
# A .p12 of the key and `certs` (a file; openssl reads it twice, so not a plain pipe).
export_p12() {
  local certs=$1 out=$2
  shift 2
  "$openssl" pkcs12 -export -inkey <(read_p12 -nocerts -nodes) -in "$certs" \
    -passout env:APPLE_SIGNING_KEY_PASS -out "$out" "$@"
}

case $1 in
  conveyor)
    read_p12 -clcerts -nokeys | "$openssl" x509 -outform der -out "$dir/developerid.cer"
    "$openssl" x509 -inform der -in "$dir/developerid.cer" -noout -subject
    export_p12 <(read_p12 -clcerts -nokeys) "$dir/developerid-only.p12" -name developerid
    ;;
  keychain)
    export_p12 <(read_p12 -nokeys) "$dir/developerid-mac.p12" \
      -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES -macalg sha1
    ;;
esac
