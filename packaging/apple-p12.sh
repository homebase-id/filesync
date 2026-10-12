#!/bin/bash
# Turns DevOps' Developer ID .p12 (the certificate, its key and Apple's G2 intermediate; its
# password in APPLE_SIGNING_KEY_PASS) into what each tool takes (the release workflow):
#   conveyor <p12> <dir>   <dir>/developerid.cer, the certificate alone (DER), and
#                          <dir>/developerid-only.p12, it and the key: Conveyor refuses a
#                          certificate file or a .p12 with the intermediate in it
#   keychain <p12> <out>   the whole .p12 again in the older format (3DES, SHA-1 MAC) that
#                          macOS's `security import` reads; it fails on OpenSSL 3's default
#                          (AES, SHA-256 MAC) with "MAC verification failed"
# The key goes from one openssl to the next through a pipe, never in the clear on disk.
# OPENSSL names an OpenSSL 3 (on macOS Homebrew's: the system's is LibreSSL).
set -euo pipefail

openssl=${OPENSSL:-openssl}
p12=$2
legacy=()
"$openssl" pkcs12 -in "$p12" -passin env:APPLE_SIGNING_KEY_PASS -noout 2> /dev/null ||
  legacy=(-legacy)
read_p12() {
  "$openssl" pkcs12 "${legacy[@]}" -in "$p12" -passin env:APPLE_SIGNING_KEY_PASS "$@"
}

case $1 in
  conveyor)
    dir=$3
    read_p12 -clcerts -nokeys > "$dir/developerid.pem"
    "$openssl" x509 -in "$dir/developerid.pem" -noout -subject
    "$openssl" x509 -in "$dir/developerid.pem" -outform der -out "$dir/developerid.cer"
    "$openssl" pkcs12 -export -inkey <(read_p12 -nocerts -nodes) -in "$dir/developerid.pem" \
      -name developerid -passout env:APPLE_SIGNING_KEY_PASS -out "$dir/developerid-only.p12"
    ;;
  keychain)
    # Key and certificates from two pipes: openssl reads -in twice, a pipe only once.
    "$openssl" pkcs12 -export -inkey <(read_p12 -nocerts -nodes) -in <(read_p12 -nokeys) \
      -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES -macalg sha1 \
      -passout env:APPLE_SIGNING_KEY_PASS -out "$3"
    ;;
  *)
    echo "usage: $0 conveyor <p12> <dir> | keychain <p12> <out>" >&2
    exit 2
    ;;
esac
