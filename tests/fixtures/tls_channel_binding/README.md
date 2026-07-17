# TLS channel-binding certificate fixtures

These static self-signed certificates were generated with OpenSSL 3.5.6. They
exercise complete RSA/SHA-256 and ECDSA-P256/SHA-256 X.509 structures without
making the tests depend on runtime certificate generation.

```bash
mkdir -p tmp/slice26-certs

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -set_serial 2601 -subj '/CN=FlowIO Slice 26 RSA Test' \
  -keyout tmp/slice26-certs/rsa.key.pem \
  -out tmp/slice26-certs/rsa.cert.pem
openssl x509 -in tmp/slice26-certs/rsa.cert.pem -outform DER \
  -out tmp/slice26-certs/rsa.der

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -sha256 -days 3650 -set_serial 2602 \
  -subj '/CN=FlowIO Slice 26 ECDSA Test' \
  -keyout tmp/slice26-certs/ecdsa.key.pem \
  -out tmp/slice26-certs/ecdsa.cert.pem
openssl x509 -in tmp/slice26-certs/ecdsa.cert.pem -outform DER \
  -out tmp/slice26-certs/ecdsa.der

od -An -v -tx1 -w32 tmp/slice26-certs/rsa.der
od -An -v -tx1 -w32 tmp/slice26-certs/ecdsa.der
```

The rendered DER SHA-256 checksums are:

- RSA: `b3adbd2b1cafe60ea73e480b684e17974930f5971f2f3f2034fb373d71582aa7`
- ECDSA: `4d8cdccda40e20443acd5cc33746ea6d836aa33b21921f72e7316fd7cea9ac64`
