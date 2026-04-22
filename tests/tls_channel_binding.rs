use flowio::net::tls::tls_server_end_point;
use rustls::pki_types::alg_id::{ED25519, RSA_PKCS1_SHA256, RSA_PKCS1_SHA384, RSA_PKCS1_SHA512};
use sha2::{Digest, Sha256, Sha384, Sha512};

fn encode_sequence(elements: &[&[u8]]) -> Vec<u8> {
    let body_len = elements.iter().map(|element| element.len()).sum::<usize>();
    assert!(body_len < 128, "test helper only supports short-form DER");

    let mut out = Vec::with_capacity(body_len + 2);
    out.push(0x30);
    out.push(body_len as u8);
    for element in elements {
        out.extend_from_slice(element);
    }
    out
}

#[test]
fn tls_server_end_point_uses_signature_hash_mapping() {
    let sha256_cert = certificate_with_algorithm(RSA_PKCS1_SHA256.as_ref());
    assert_eq!(
        tls_server_end_point(&sha256_cert),
        Some(Sha256::digest(&sha256_cert).to_vec())
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

fn certificate_with_algorithm(algorithm_oid: &[u8]) -> Vec<u8> {
    let first = [0x30, 0x00];
    let third = [0x03, 0x01, 0x00];
    let algorithm = encode_sequence(&[algorithm_oid]);
    encode_sequence(&[&first, &algorithm, &third])
}
