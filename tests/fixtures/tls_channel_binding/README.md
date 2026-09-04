# TLS channel-binding certificate fixtures

These static self-signed certificates were generated with OpenSSL 3.5.7. They
exercise complete RSA/SHA-256 and ECDSA-P256/SHA-256 X.509 structures without
making the tests depend on runtime certificate generation.

```bash
mkdir -p tmp/channel-binding-certs

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -set_serial 2601 -subj '/CN=FlowIO Channel Binding RSA Test' \
  -keyout tmp/channel-binding-certs/rsa.key.pem \
  -out tmp/channel-binding-certs/rsa.cert.pem
openssl x509 -in tmp/channel-binding-certs/rsa.cert.pem -outform DER \
  -out tmp/channel-binding-certs/rsa.der

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -sha256 -days 3650 -set_serial 2602 \
  -subj '/CN=FlowIO Channel Binding ECDSA Test' \
  -keyout tmp/channel-binding-certs/ecdsa.key.pem \
  -out tmp/channel-binding-certs/ecdsa.cert.pem
openssl x509 -in tmp/channel-binding-certs/ecdsa.cert.pem -outform DER \
  -out tmp/channel-binding-certs/ecdsa.der

od -An -v -tx1 -w32 tmp/channel-binding-certs/rsa.der
od -An -v -tx1 -w32 tmp/channel-binding-certs/ecdsa.der
```

The rendered DER SHA-256 checksums are:

- RSA: `f2958b127702934ac73ab07325c2afa6e0be2acc9797cb7294ed371820a042cd`
- ECDSA: `0f464a28caf499697bff5226f3d10c6a04b9528320eba25d9f1974b32480cd2c`
