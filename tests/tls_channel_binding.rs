use flowio::net::tls::tls_server_end_point;
use rustls::pki_types::alg_id::{ED25519, RSA_PKCS1_SHA256, RSA_PKCS1_SHA384, RSA_PKCS1_SHA512};
use sha2::{Digest, Sha256, Sha384, Sha512};

const RSA_PKCS1_SHA1: &[u8] = &[
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x05, 0x05, 0x00,
];
const ECDSA_SHA1: &[u8] = &[0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01];

fn encode_sequence(elements: &[&[u8]]) -> Vec<u8> {
    let body_len = elements.iter().map(|element| element.len()).sum::<usize>();

    let mut out = Vec::with_capacity(body_len + 4);
    out.push(0x30);
    push_der_len(&mut out, body_len);
    for element in elements {
        out.extend_from_slice(element);
    }
    out
}

/// Emits a DER definite length, using short form below 128 and long form for
/// larger lengths.
fn push_der_len(out: &mut Vec<u8>, len: usize) {
    if len < 128 {
        out.push(len as u8);
        return;
    }

    let len_bytes = len.to_be_bytes();
    let first_nonzero = len_bytes
        .iter()
        .position(|byte| *byte != 0)
        .expect("nonzero long-form length");
    let encoded = &len_bytes[first_nonzero..];
    out.push(0x80 | encoded.len() as u8);
    out.extend_from_slice(encoded);
}

/// RFC 5929 maps MD5/SHA-1 certificate signatures to SHA-256 for
/// tls-server-end-point; the SHA-1 cases expecting SHA-256 are deliberate.
#[test]
fn tls_server_end_point_uses_signature_hash_mapping() {
    let sha256_cert = certificate_with_algorithm(RSA_PKCS1_SHA256.as_ref());
    assert_eq!(
        tls_server_end_point(&sha256_cert),
        Some(Sha256::digest(&sha256_cert).to_vec())
    );

    let sha1_cert = certificate_with_algorithm(RSA_PKCS1_SHA1);
    assert_eq!(
        tls_server_end_point(&sha1_cert),
        Some(Sha256::digest(&sha1_cert).to_vec())
    );

    let ecdsa_sha1_cert = certificate_with_algorithm(ECDSA_SHA1);
    assert_eq!(
        tls_server_end_point(&ecdsa_sha1_cert),
        Some(Sha256::digest(&ecdsa_sha1_cert).to_vec())
    );

    let sha384_cert = certificate_with_algorithm(RSA_PKCS1_SHA384.as_ref());
    assert_eq!(
        tls_server_end_point(&sha384_cert),
        Some(Sha384::digest(&sha384_cert).to_vec())
    );

    let sha512_cert = certificate_with_algorithm(RSA_PKCS1_SHA512.as_ref());
    assert_eq!(
        tls_server_end_point(&sha512_cert),
        Some(Sha512::digest(&sha512_cert).to_vec())
    );
}

#[test]
fn tls_server_end_point_rejects_hashless_signature_algorithms() {
    assert_eq!(
        tls_server_end_point(&certificate_with_algorithm(ED25519.as_ref())),
        None
    );
}

#[test]
fn tls_server_end_point_rejects_malformed_certificate_der() {
    assert_eq!(tls_server_end_point(&[0x30, 0x01, 0xff]), None);
}

#[test]
fn tls_server_end_point_accepts_long_form_der_lengths() {
    let padding = vec![0u8; 130];
    let first = encode_sequence(&[&padding]);
    let third = [0x03, 0x01, 0x00];
    let algorithm = encode_sequence(&[RSA_PKCS1_SHA256.as_ref()]);
    let cert = encode_sequence(&[&first, &algorithm, &third]);

    assert_eq!(
        tls_server_end_point(&cert),
        Some(Sha256::digest(&cert).to_vec())
    );
}

/// Minimal stub certificate DER where only signatureAlgorithm is meaningful
/// for tls_server_end_point.
fn certificate_with_algorithm(algorithm_oid: &[u8]) -> Vec<u8> {
    let first = [0x30, 0x00];
    let third = [0x03, 0x01, 0x00];
    let algorithm = encode_sequence(&[algorithm_oid]);
    encode_sequence(&[&first, &algorithm, &third])
}
