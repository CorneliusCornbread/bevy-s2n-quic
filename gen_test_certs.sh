#!/usr/bin/env bash
set -euo pipefail

EXAMPLES_DIR="$(dirname "$0")/examples"
CERTS_DIR="$EXAMPLES_DIR/certs"

mkdir -p "$CERTS_DIR"

CERT_FILE="$CERTS_DIR/cert.pem"
KEY_FILE="$CERTS_DIR/key.pem"

echo "Generating self-signed certificate for testing in $CERTS_DIR..."

# Generate private key
openssl genrsa -out "$KEY_FILE" 2048

# Generate self-signed certificate. rustls only checks the subject alternative
# names, and rejects CA certificates used as a server certificate, so the
# certificate lists its names as SANs and isn't a CA.
openssl req -new -x509 -key "$KEY_FILE" -out "$CERT_FILE" -days 3650 \
    -subj "/C=US/ST=Test/L=Test/O=Test/OU=Dev/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth"

echo "Created:"
echo "  - $CERT_FILE"
echo "  - $KEY_FILE"
