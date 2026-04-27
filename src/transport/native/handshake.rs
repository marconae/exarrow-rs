use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use num_bigint::BigUint;

use crate::error::TransportError;

use super::attributes::{AttributeSet, AttributeValue};
use super::constants::*;
use super::encryption::ChaCha20Encryptor;
use super::framing::{MessageHeader, SerialCounter};

/// Result of building the authentication message.
pub struct AuthMessage {
    pub wire_bytes: Vec<u8>,
    pub send_key: Vec<u8>,
    pub recv_key: Vec<u8>,
}

/// Build the initial login packet sent when first connecting.
///
/// Format: [MAGIC 4B] [msg_len 4B] [protocol_version 4B] [change_date 4B] [attributes...]
///
/// The Exasol C++ SDK writes these fields through `exaBswap32`, but that helper only swaps on
/// big-endian hosts. On the little-endian platforms we support, the login packet stays LE.
pub fn build_login_packet(username: &str) -> Vec<u8> {
    let mut attrs = AttributeSet::new();
    attrs.add(ATTR_USERNAME, AttributeValue::String(username.to_owned()));
    attrs.add(
        ATTR_CLIENTNAME,
        AttributeValue::String("exarrow-rs".to_owned()),
    );
    attrs.add(
        ATTR_DRIVERNAME,
        AttributeValue::String("exarrow-rs".to_owned()),
    );
    attrs.add(
        ATTR_CLIENTOS,
        AttributeValue::String(std::env::consts::OS.to_owned()),
    );
    attrs.add(
        ATTR_CLIENTVERSION,
        AttributeValue::String(env!("CARGO_PKG_VERSION").to_owned()),
    );

    let attr_bytes = attrs.serialize();
    // msg_len covers protocol_version(4) + change_date(4) + attributes
    let msg_len: u32 = 8 + attr_bytes.len() as u32;

    let mut buf = Vec::with_capacity(8 + msg_len as usize);
    buf.extend_from_slice(&LOGIN_MAGIC.to_le_bytes());
    buf.extend_from_slice(&msg_len.to_le_bytes());
    buf.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    buf.extend_from_slice(&CHANGE_DATE.to_le_bytes());
    buf.extend_from_slice(&attr_bytes);
    buf
}

/// Parse the server's login response to extract session attributes.
///
/// The server sends: [total_size: u32 LE] followed by attribute data.
/// Returns the parsed attributes containing public key, session info, etc.
pub fn parse_login_response(data: &[u8]) -> Result<AttributeSet, TransportError> {
    if data.len() < 4 {
        return Err(TransportError::ProtocolError(
            "Login response too short".into(),
        ));
    }
    let total_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + total_size {
        return Err(TransportError::ProtocolError(format!(
            "Login response truncated: expected {} bytes, got {}",
            4 + total_size,
            data.len()
        )));
    }
    let attr_data = &data[4..4 + total_size];
    // Count attributes by scanning the data
    let count = count_attributes(attr_data);
    super::attributes::parse_attributes(attr_data, count)
}

/// Build the second-phase authentication message containing encrypted password and keys.
///
/// Sends CMD_SET_ATTRIBUTES with the RSA-encrypted password and ChaCha20 keys.
/// Uses Exasol's custom RSA encoding: interleave data with random phrase, then
/// encrypt in 64-byte blocks via raw RSA (no PKCS#1 padding).
pub fn build_auth_message(
    password: &str,
    public_key_data: &[u8],
    random_phrase: &[u8],
    serial: &SerialCounter,
    use_chacha20: bool,
) -> Result<AuthMessage, TransportError> {
    let (n, e) = parse_rsa_public_key(public_key_data)?;

    // Pad password: [password bytes][0x00][random padding] to match phrase length
    let pwd_bytes = password.as_bytes();
    let padded_pwd = if pwd_bytes.len() + 2 < random_phrase.len() {
        let padding_len = random_phrase.len() - pwd_bytes.len() - 2;
        let rng = SystemRandom::new();
        let mut padding = vec![0u8; padding_len];
        rng.fill(&mut padding)
            .map_err(|_| TransportError::ProtocolError("RNG failure".into()))?;
        let mut padded = Vec::with_capacity(random_phrase.len() - 1);
        padded.extend_from_slice(pwd_bytes);
        padded.push(0x00); // null terminator
        padded.extend_from_slice(&padding);
        padded
    } else {
        pwd_bytes.to_vec()
    };

    let encrypted_password = exasol_encode_pwd(&padded_pwd, random_phrase, &n, &e)?;

    let mut attrs = AttributeSet::new();
    attrs.add(
        ATTR_ENCODED_PASSWORD,
        AttributeValue::Binary(encrypted_password),
    );

    let (send_key, recv_key) = if use_chacha20 {
        let (sk, rk) = ChaCha20Encryptor::generate_keys();
        let encrypted_send_key = exasol_encode_pwd(&sk, random_phrase, &n, &e)?;
        let encrypted_recv_key = exasol_encode_pwd(&rk, random_phrase, &n, &e)?;
        attrs.add(
            ATTR_CLIENT_SEND_KEY,
            AttributeValue::Binary(encrypted_send_key),
        );
        attrs.add(
            ATTR_CLIENT_RECEIVE_KEY,
            AttributeValue::Binary(encrypted_recv_key),
        );
        attrs.add(
            ATTR_CLIENT_KEYS_LEN,
            AttributeValue::Int32(CHACHA20_KEY_LEN as i32),
        );
        (sk, rk)
    } else {
        (Vec::new(), Vec::new())
    };

    let attr_bytes = attrs.serialize();
    let header = MessageHeader::new(
        CMD_SET_ATTRIBUTES,
        serial.next(),
        attrs.num_attributes(),
        attr_bytes.len() as u32,
        0,
    );
    let header_bytes = header.serialize();

    let mut message = Vec::with_capacity(HEADER_SIZE + attr_bytes.len());
    message.extend_from_slice(&header_bytes);
    message.extend_from_slice(&attr_bytes);

    Ok(AuthMessage {
        wire_bytes: message,
        send_key,
        recv_key,
    })
}

/// Parse an RSA public key into (n, e).
///
/// Supports two formats:
/// 1. **PKCS#1 DER** (starts with 0x30): standard ASN.1 SEQUENCE(INTEGER, INTEGER)
/// 2. **Raw Exasol native format**: `[exponent: k/2 bytes BE] [modulus: k/2 bytes BE]`
///    where the exponent is zero-padded to half the total key size.
fn parse_rsa_public_key(key_data: &[u8]) -> Result<(BigUint, BigUint), TransportError> {
    if key_data.first() == Some(&0x30) {
        // PKCS#1 DER format
        parse_rsa_public_key_pkcs1_der(key_data)
    } else {
        // Raw Exasol native format: first half = exponent (BE, zero-padded), second half = modulus (BE)
        if key_data.len() < 4 || !key_data.len().is_multiple_of(2) {
            return Err(TransportError::ProtocolError(format!(
                "Invalid raw RSA key: unexpected length {}",
                key_data.len()
            )));
        }
        let half = key_data.len() / 2;
        let e = BigUint::from_bytes_be(&key_data[..half]);
        let n = BigUint::from_bytes_be(&key_data[half..]);
        if n.bits() == 0 || e.bits() == 0 {
            return Err(TransportError::ProtocolError(
                "Invalid raw RSA key: zero modulus or exponent".into(),
            ));
        }
        Ok((n, e))
    }
}

fn parse_rsa_public_key_pkcs1_der(der: &[u8]) -> Result<(BigUint, BigUint), TransportError> {
    let mut pos = 0;

    if der.get(pos) != Some(&0x30) {
        return Err(TransportError::ProtocolError(
            "Invalid DER: expected SEQUENCE".into(),
        ));
    }
    pos += 1;

    let (seq_len, len_bytes) = read_der_length(&der[pos..])?;
    pos += len_bytes;

    let seq_end = pos + seq_len;
    if seq_end > der.len() {
        return Err(TransportError::ProtocolError(
            "Invalid DER: truncated".into(),
        ));
    }

    let (n_bytes, consumed) = read_der_integer(&der[pos..seq_end])?;
    pos += consumed;

    let (e_bytes, consumed) = read_der_integer(&der[pos..seq_end])?;
    pos += consumed;

    if pos != seq_end {
        return Err(TransportError::ProtocolError(
            "Invalid DER: trailing bytes".into(),
        ));
    }

    Ok((
        BigUint::from_bytes_be(n_bytes),
        BigUint::from_bytes_be(e_bytes),
    ))
}

fn read_der_length(data: &[u8]) -> Result<(usize, usize), TransportError> {
    if data.is_empty() {
        return Err(TransportError::ProtocolError(
            "Invalid DER: truncated".into(),
        ));
    }
    let first = data[0];
    if first < 0x80 {
        Ok((first as usize, 1))
    } else {
        let num_bytes = (first & 0x7F) as usize;
        if num_bytes == 0 || num_bytes > 3 || data.len() < 1 + num_bytes {
            return Err(TransportError::ProtocolError(
                "Invalid DER: bad length".into(),
            ));
        }
        let mut len: usize = 0;
        for &b in &data[1..1 + num_bytes] {
            len = (len << 8) | (b as usize);
        }
        Ok((len, 1 + num_bytes))
    }
}

fn read_der_integer(data: &[u8]) -> Result<(&[u8], usize), TransportError> {
    if data.is_empty() || data[0] != 0x02 {
        return Err(TransportError::ProtocolError(
            "Invalid DER: expected INTEGER".into(),
        ));
    }
    let (int_len, len_bytes) = read_der_length(&data[1..])?;
    let header_len = 1 + len_bytes;
    if data.len() < header_len + int_len {
        return Err(TransportError::ProtocolError(
            "Invalid DER: truncated integer".into(),
        ));
    }
    let mut value = &data[header_len..header_len + int_len];
    if value.len() > 1 && value[0] == 0x00 {
        value = &value[1..];
    }
    Ok((value, header_len + int_len))
}

/// Exasol's custom RSA password encoding.
///
/// 1. Null-terminate the data
/// 2. Interleave data bytes with phrase bytes: [data[0], phrase[0], data[1], phrase[1], ...]
/// 3. Encrypt in 64-byte blocks → 128-byte output blocks (raw RSA: plaintext^e mod N)
fn exasol_encode_pwd(
    data: &[u8],
    phrase: &[u8],
    n: &BigUint,
    e: &BigUint,
) -> Result<Vec<u8>, TransportError> {
    // Null-terminate the data
    let mut pwd = Vec::with_capacity(data.len() + 1);
    pwd.extend_from_slice(data);
    pwd.push(0x00);
    let pwd_len = pwd.len();
    let phrase_len = phrase.len();

    // Calculate interleaved plaintext size
    let mut encoded_output_len = if pwd_len > phrase_len {
        pwd_len * 2
    } else {
        phrase_len * 2
    };

    // Round up to multiple of RSA_KEY_LENGTH (128)
    let rsa_key_len = 128usize; // Protocol::RSA_KEY_LENGTH
    if encoded_output_len % rsa_key_len != 0 {
        encoded_output_len += rsa_key_len - (encoded_output_len % rsa_key_len);
    }

    // Interleave password and phrase bytes
    let interleaved_len = encoded_output_len; // before doubling
    let mut interleaved = vec![0u8; interleaved_len];
    let iterations = encoded_output_len / 2; // number of pairs (before the *2 in C++)

    // The C++ code does: encodedOutputLen *= 2; for(i=0; i<encodedOutputLen/4; i++)
    // which means iterations = encodedOutputLen/2 (after rounding, before doubling)
    // Each iteration writes 2 bytes: tmp[i*2] = pwd[i%pwdLen], tmp[i*2+1] = phrase[i%phraseLen]
    for i in 0..iterations {
        interleaved[i * 2] = pwd[i % pwd_len];
        interleaved[i * 2 + 1] = phrase[i % phrase_len];
    }

    // Now encoded_output_len doubles for the RSA output
    let rsa_output_len = encoded_output_len * 2;
    let block_input_size = rsa_key_len / 2; // 64 bytes
    let num_blocks = rsa_output_len / rsa_key_len;
    let mut encrypted = vec![0u8; rsa_output_len];

    for i in 0..num_blocks {
        let input_start = i * block_input_size;
        let input_end = input_start + block_input_size;

        // Read 64 bytes as a big-endian integer
        let block_data = if input_end <= interleaved.len() {
            &interleaved[input_start..input_end]
        } else {
            // Pad with zeros if we go past the interleaved data
            break;
        };

        let plaintext = BigUint::from_bytes_be(block_data);

        // Raw RSA: ciphertext = plaintext^e mod N
        let ciphertext = plaintext.modpow(e, n);

        // Export as 128-byte big-endian, zero-padded
        let c_bytes = ciphertext.to_bytes_be();
        let output_start = i * rsa_key_len;
        let offset = rsa_key_len - c_bytes.len();
        encrypted[output_start + offset..output_start + offset + c_bytes.len()]
            .copy_from_slice(&c_bytes);
    }

    Ok(encrypted)
}

/// Count the number of attributes in a binary attribute buffer.
///
/// Scans forward through the buffer counting each attribute entry.
fn count_attributes(data: &[u8]) -> u32 {
    let mut count = 0u32;
    let mut offset = 0;
    while offset + 2 <= data.len() {
        let id = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;
        match attribute_wire_size(id) {
            WireSize::Bool => {
                if offset >= data.len() {
                    break;
                }
                offset += 1;
            }
            WireSize::I32 => {
                if offset + 4 > data.len() {
                    break;
                }
                offset += 4;
            }
            WireSize::I64 => {
                if offset + 8 > data.len() {
                    break;
                }
                offset += 8;
            }
            WireSize::LengthPrefixed => {
                if offset + 4 > data.len() {
                    break;
                }
                let len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + len > data.len() {
                    break;
                }
                offset += len;
            }
        }
        count += 1;
    }
    count
}

enum WireSize {
    Bool,
    I32,
    I64,
    LengthPrefixed,
}

fn attribute_wire_size(id: u16) -> WireSize {
    match id {
        ATTR_AUTOCOMMIT
        | ATTR_TSUTC_ENABLED
        | ATTR_ENCRYPTION_REQUIRED
        | ATTR_SNAPSHOT_TRANSACTIONS_ENABLED
        | ATTR_TRANSACTION_STATE => WireSize::Bool,
        ATTR_PROTOCOL_VERSION
        | ATTR_QUERY_TIMEOUT
        | ATTR_QUERY_CACHE_ACCESS
        | ATTR_CLIENT_KEYS_LEN => WireSize::I32,
        ATTR_SESSIONID
        | ATTR_DATA_MESSAGE_SIZE
        | ATTR_RESULT_SET_HANDLE
        | ATTR_STATEMENT_HANDLE
        | ATTR_NUM_ROWS => WireSize::I64,
        _ => WireSize::LengthPrefixed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_packet_starts_with_magic() {
        let packet = build_login_packet("sys");
        let magic = u32::from_le_bytes([packet[0], packet[1], packet[2], packet[3]]);
        assert_eq!(magic, LOGIN_MAGIC);
    }

    #[test]
    fn login_packet_contains_protocol_version() {
        let packet = build_login_packet("sys");
        let version = u32::from_le_bytes([packet[8], packet[9], packet[10], packet[11]]);
        assert_eq!(version, PROTOCOL_VERSION);
    }

    #[test]
    fn login_packet_contains_change_date() {
        let packet = build_login_packet("sys");
        let date = u32::from_le_bytes([packet[12], packet[13], packet[14], packet[15]]);
        assert_eq!(date, CHANGE_DATE);
    }

    #[test]
    fn login_packet_msg_len_is_consistent() {
        let packet = build_login_packet("test_user");
        let msg_len = u32::from_le_bytes([packet[4], packet[5], packet[6], packet[7]]) as usize;
        // Total packet = 8 (magic + msg_len) + msg_len
        assert_eq!(packet.len(), 8 + msg_len);
    }

    #[test]
    fn count_attributes_empty() {
        assert_eq!(count_attributes(&[]), 0);
    }

    #[test]
    fn count_attributes_roundtrip() {
        let mut attrs = AttributeSet::new();
        attrs.add(ATTR_USERNAME, AttributeValue::String("sys".into()));
        attrs.add(ATTR_AUTOCOMMIT, AttributeValue::Bool(true));
        attrs.add(ATTR_PROTOCOL_VERSION, AttributeValue::Int32(21));
        let bytes = attrs.serialize();
        assert_eq!(count_attributes(&bytes), 3);
    }
}
