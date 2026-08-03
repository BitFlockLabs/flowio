use flowio::net::tls::tls_server_end_point;
use rustls::pki_types::alg_id::{ED25519, RSA_PKCS1_SHA256, RSA_PKCS1_SHA384, RSA_PKCS1_SHA512};
use sha2::{Digest, Sha256, Sha384, Sha512};

const RSA_PKCS1_SHA1: &[u8] = &[
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x05, 0x05, 0x00,
];
const ECDSA_SHA1: &[u8] = &[0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01];
const UNKNOWN_SIGNATURE_ALGORITHM: &[u8] = &[0x06, 0x03, 0x2a, 0x03, 0x04];

const RSA_SHA256_CERT_HEX: &str = include_str!("fixtures/tls_channel_binding/rsa_sha256.der.hex");
const ECDSA_SHA256_CERT_HEX: &str =
    include_str!("fixtures/tls_channel_binding/ecdsa_sha256.der.hex");

fn encode_tlv(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 4);
    out.push(tag);
    push_der_len(&mut out, body.len());
    out.extend_from_slice(body);
    out
}

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
    assert_eq!(
        tls_server_end_point(&certificate_with_algorithm(UNKNOWN_SIGNATURE_ALGORITHM)),
        None
    );
}

#[test]
fn tls_server_end_point_rejects_malformed_certificate_der() {
    let malformed = [
        ("invalid outer child", vec![0x30, 0x01, 0xff]),
        ("indefinite outer length", vec![0x30, 0x80]),
        ("missing long-form length", vec![0x30, 0x81]),
        ("truncated long-form length", vec![0x30, 0x82, 0x01]),
        (
            "excessive length-octet count",
            vec![0x30, 0x89, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "truncated short-form child",
            encode_sequence(&[&[0x30, 0x02, 0x00]]),
        ),
        (
            "truncated long-form child",
            encode_sequence(&[&[0x30, 0x81]]),
        ),
    ];

    for (name, certificate) in malformed {
        assert_eq!(
            tls_server_end_point(&certificate),
            None,
            "malformed case accepted: {name}"
        );
    }
}

#[test]
fn tls_server_end_point_requires_exact_certificate_children() {
    let first = encode_sequence(&[]);
    let algorithm = encode_sequence(&[RSA_PKCS1_SHA256.as_ref()]);
    let signature = [0x03, 0x01, 0x00];
    let wrong_first = encode_tlv(0x02, &[0x00]);
    let wrong_second = encode_tlv(0x04, &[]);
    let wrong_third = encode_tlv(0x04, &[0x00]);
    let extra_sequence = encode_sequence(&[]);
    let extra_signature = [0x03, 0x01, 0x00];

    let malformed = [
        (
            "wrong first child",
            encode_sequence(&[&wrong_first, &algorithm, &signature]),
        ),
        (
            "missing first child",
            encode_sequence(&[&algorithm, &signature]),
        ),
        (
            "extra first child",
            encode_sequence(&[&extra_sequence, &first, &algorithm, &signature]),
        ),
        (
            "wrong second child",
            encode_sequence(&[&first, &wrong_second, &signature]),
        ),
        (
            "missing second child",
            encode_sequence(&[&first, &signature]),
        ),
        (
            "extra second child",
            encode_sequence(&[&first, &extra_sequence, &algorithm, &signature]),
        ),
        (
            "wrong third child",
            encode_sequence(&[&first, &algorithm, &wrong_third]),
        ),
        (
            "missing third child",
            encode_sequence(&[&first, &algorithm]),
        ),
        (
            "extra third child",
            encode_sequence(&[&first, &algorithm, &extra_signature, &signature]),
        ),
    ];

    for (name, certificate) in malformed {
        assert_eq!(
            tls_server_end_point(&certificate),
            None,
            "structurally invalid case accepted: {name}"
        );
    }
}

#[test]
fn tls_server_end_point_rejects_trailing_bytes() {
    let first = encode_sequence(&[]);
    let algorithm = encode_sequence(&[RSA_PKCS1_SHA256.as_ref()]);
    let signature = [0x03, 0x01, 0x00];
    let trailing = [0x05, 0x00];

    let trailing_inside_outer = encode_sequence(&[&first, &algorithm, &signature, &trailing]);
    assert_eq!(tls_server_end_point(&trailing_inside_outer), None);

    let mut trailing_after_outer = encode_sequence(&[&first, &algorithm, &signature]);
    trailing_after_outer.extend_from_slice(&trailing);
    assert_eq!(tls_server_end_point(&trailing_after_outer), None);
}

#[test]
fn tls_server_end_point_accepts_long_form_der_lengths() {
    for body_len in [128, 130] {
        let padding = vec![0u8; body_len];
        let first = encode_sequence(&[&padding]);
        let third = [0x03, 0x01, 0x00];
        let algorithm = encode_sequence(&[RSA_PKCS1_SHA256.as_ref()]);
        let cert = encode_sequence(&[&first, &algorithm, &third]);

        assert_eq!(
            tls_server_end_point(&cert),
            Some(Sha256::digest(&cert).to_vec()),
            "minimal long-form length {body_len} was rejected"
        );
    }
}

#[test]
fn tls_server_end_point_rejects_noncanonical_der_lengths() {
    let canonical = certificate_with_algorithm(RSA_PKCS1_SHA256.as_ref());
    assert_eq!(canonical[0], 0x30);
    assert!(canonical[1] < 0x80);

    let mut nonminimal_outer = Vec::with_capacity(canonical.len() + 1);
    nonminimal_outer.extend_from_slice(&[0x30, 0x81, canonical[1]]);
    nonminimal_outer.extend_from_slice(&canonical[2..]);
    assert_eq!(tls_server_end_point(&nonminimal_outer), None);

    let mut zero_padded_first = Vec::with_capacity(132);
    zero_padded_first.extend_from_slice(&[0x30, 0x82, 0x00, 0x80]);
    zero_padded_first.resize(132, 0);
    let algorithm = encode_sequence(&[RSA_PKCS1_SHA256.as_ref()]);
    let signature = [0x03, 0x01, 0x00];
    let zero_padded_child = encode_sequence(&[&zero_padded_first, &algorithm, &signature]);
    assert_eq!(tls_server_end_point(&zero_padded_child), None);
}

#[test]
fn tls_server_end_point_accepts_static_rsa_and_ecdsa_certificate_corpus() {
    let rsa = decode_hex_fixture(RSA_SHA256_CERT_HEX);
    assert_eq!(
        tls_server_end_point(&rsa),
        Some(Sha256::digest(&rsa).to_vec())
    );

    let ecdsa = decode_hex_fixture(ECDSA_SHA256_CERT_HEX);
    assert_eq!(
        tls_server_end_point(&ecdsa),
        Some(Sha256::digest(&ecdsa).to_vec())
    );
}

fn decode_hex_fixture(input: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len() / 2);
    let mut high_nibble = None;

    for byte in input.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => panic!("non-hex byte in certificate fixture"),
        };

        if let Some(high) = high_nibble.take() {
            output.push((high << 4) | nibble);
        } else {
            high_nibble = Some(nibble);
        }
    }

    assert!(high_nibble.is_none(), "odd-length certificate fixture");
    output
}

/// Minimal stub certificate DER where only signatureAlgorithm is meaningful
/// for tls_server_end_point.
fn certificate_with_algorithm(algorithm_oid: &[u8]) -> Vec<u8> {
    let first = [0x30, 0x00];
    let third = [0x03, 0x01, 0x00];
    let algorithm = encode_sequence(&[algorithm_oid]);
    encode_sequence(&[&first, &algorithm, &third])
}
