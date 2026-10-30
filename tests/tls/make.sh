#!/bin/sh
# Test-only certificates for tests/test_remote_tls.py. Never use these keys for anything else.
# Each directory holds tls.key, tls.crt and ca.crt, so it works as a secret_mount_point.
# Regenerate with OpenSSL 3 (macOS's LibreSSL writes EC keys OpenSSL 3 rejects):
#   OPENSSL=/opt/homebrew/opt/openssl@3/bin/openssl sh tests/tls/make.sh
set -e
cd "$(dirname "$0")"
OPENSSL=${OPENSSL:-openssl}
DAYS=36500

ca() {  # dir cn: a self-signed CA, kept as dir/ca.key and dir/ca.crt
  mkdir -p "$1"
  "$OPENSSL" req -x509 -sha256 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$1/ca.key" -out "$1/ca.crt" -subj "/CN=$2" -days $DAYS 2>/dev/null
}

leaf() {  # dir ca_dir subject [san]: a certificate signed by ca_dir
  mkdir -p "$1"
  "$OPENSSL" req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$1/tls.key" -out "$1/tls.csr" -subj "$3" 2>/dev/null
  if [ -n "$4" ]; then printf 'subjectAltName=%s\n' "$4" > "$1/ext"; else : > "$1/ext"; fi
  "$OPENSSL" x509 -req -sha256 -in "$1/tls.csr" -CA "$2/ca.crt" -CAkey "$2/ca.key" \
    -set_serial "0x$("$OPENSSL" rand -hex 8)" -out "$1/tls.crt" -days $DAYS \
    -extfile "$1/ext" 2>/dev/null
  [ "$1" = "$2" ] || cp "$2/ca.crt" "$1/ca.crt"
  rm "$1/tls.csr" "$1/ext"
}

ca ca ohra-test-ca
leaf node-a ca /CN=ohra-node DNS:ohra-node,DNS:ohra.local
leaf node-b ca /CN=ohra-node DNS:ohra-node,DNS:ohra.local
leaf other-name ca /CN=someone-else DNS:someone-else
leaf cn-only ca /CN=ohra-node
leaf san-only ca / DNS:ohra-node
ca foreign foreign-ca
leaf foreign foreign /CN=ohra-node DNS:ohra-node,DNS:ohra.local
