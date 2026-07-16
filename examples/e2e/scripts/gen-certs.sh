#!/bin/sh
# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
#
# Generates a throwaway self-signed CA and a broker (server) certificate into
# the shared /certs volume at stack start-up. NEVER committed: the certs live
# only in the compose volume and are recreated on every `down -v` + up. The
# broker cert's SAN covers the compose service name "redpanda" (plus localhost)
# so TLS hostname verification passes without any client-side opt-out.
set -eu

CERT_DIR="${CERT_DIR:-/certs}"
DAYS=3650
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

# Idempotent: if a broker cert is already present, do nothing (a re-run against
# a warm volume must not rotate the CA out from under a running broker).
if [ -f server-cert.pem ] && [ -f ca-cert.pem ]; then
  echo "certs already present in $CERT_DIR, skipping generation"
  exit 0
fi

echo "generating self-signed CA..."
openssl req -new -x509 -nodes -days "$DAYS" \
  -keyout ca-key.pem -out ca-cert.pem \
  -subj "/CN=llingr-kafka-example-CA/O=llingr/C=GB"

echo "generating broker (server) certificate (SAN: redpanda, localhost)..."
openssl genrsa -out server-key.pem 2048

cat > server.cnf <<'EOF'
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no
[dn]
CN = redpanda
[v3_req]
subjectAltName = @alt
[alt]
DNS.1 = redpanda
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -new -key server-key.pem -out server.csr -config server.cnf
openssl x509 -req -in server.csr \
  -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-cert.pem -days "$DAYS" \
  -extensions v3_req -extfile server.cnf

# The broker process runs as a non-root uid in the redpanda image; make the
# key world-readable inside this throwaway volume so it can read it.
chmod 0644 server-key.pem ca-key.pem
rm -f server.csr server.cnf ca-cert.srl

echo "certs generated in $CERT_DIR:"
ls -1 "$CERT_DIR"
