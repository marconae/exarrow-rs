use std::sync::atomic::{AtomicU32, Ordering};

use super::constants::HEADER_SIZE;
use crate::error::TransportError;

/// The 21-byte binary message header for the native protocol.
///
/// All multi-byte fields are in native (little-endian) byte order. The C++ SDK's
/// `exaBswap32` helper is a no-op on little-endian platforms (`#ifdef WORDS_BIGENDIAN`),
/// so the wire format is LE on all x86/x64 Exasol deployments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    /// Payload size after the header.
    pub message_length: u32,
    /// Command code (e.g. CMD_EXECUTE, CMD_FETCH2).
    pub command: u8,
    /// Monotonically increasing message counter.
    pub serial: u32,
    /// Number of attributes following the header.
    pub num_attributes: u32,
    /// Total byte length of attribute data.
    pub attribute_data_len: u32,
    /// Number of data parts in the payload.
    pub num_result_parts: u32,
}

impl MessageHeader {
    pub fn new(
        command: u8,
        serial: u32,
        num_attributes: u32,
        attribute_data_len: u32,
        num_result_parts: u32,
    ) -> Self {
        let message_length = attribute_data_len;
        Self {
            message_length,
            command,
            serial,
            num_attributes,
            attribute_data_len,
            num_result_parts,
        }
    }

    /// Serialize the header into a 21-byte array.
    pub fn serialize(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.message_length.to_le_bytes());
        buf[4] = self.command;
        buf[5..9].copy_from_slice(&self.serial.to_le_bytes());
        buf[9..13].copy_from_slice(&self.num_attributes.to_le_bytes());
        buf[13..17].copy_from_slice(&self.attribute_data_len.to_le_bytes());
        buf[17..21].copy_from_slice(&self.num_result_parts.to_le_bytes());
        buf
    }

    /// Parse a 21-byte array into a `MessageHeader`.
    pub fn parse(data: &[u8; HEADER_SIZE]) -> Result<Self, TransportError> {
        let message_length = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let command = data[4];
        let serial = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
        let num_attributes = u32::from_le_bytes([data[9], data[10], data[11], data[12]]);
        let attribute_data_len = u32::from_le_bytes([data[13], data[14], data[15], data[16]]);
        let num_result_parts = u32::from_le_bytes([data[17], data[18], data[19], data[20]]);

        Ok(Self {
            message_length,
            command,
            serial,
            num_attributes,
            attribute_data_len,
            num_result_parts,
        })
    }
}

/// Thread-safe monotonically increasing counter for message serial numbers.
pub struct SerialCounter {
    next: AtomicU32,
}

impl SerialCounter {
    pub fn new() -> Self {
        Self {
            next: AtomicU32::new(1),
        }
    }

    /// Return the next serial number, incrementing the internal counter.
    pub fn next(&self) -> u32 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for SerialCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = MessageHeader {
            message_length: 1024,
            command: 12,
            serial: 7,
            num_attributes: 3,
            attribute_data_len: 256,
            num_result_parts: 1,
        };
        let bytes = header.serialize();
        assert_eq!(bytes.len(), HEADER_SIZE);

        let parsed = MessageHeader::parse(&bytes).unwrap();
        assert_eq!(parsed, header);
    }

    #[test]
    fn test_header_zero_values() {
        let header = MessageHeader::new(0, 0, 0, 0, 0);
        let bytes = header.serialize();
        let parsed = MessageHeader::parse(&bytes).unwrap();
        assert_eq!(parsed.message_length, 0);
        assert_eq!(parsed.command, 0);
        assert_eq!(parsed.serial, 0);
        assert_eq!(parsed.num_attributes, 0);
        assert_eq!(parsed.attribute_data_len, 0);
        assert_eq!(parsed.num_result_parts, 0);
    }

    #[test]
    fn test_header_max_values() {
        let header = MessageHeader {
            message_length: u32::MAX,
            command: u8::MAX,
            serial: u32::MAX,
            num_attributes: u32::MAX,
            attribute_data_len: u32::MAX,
            num_result_parts: u32::MAX,
        };
        let bytes = header.serialize();
        let parsed = MessageHeader::parse(&bytes).unwrap();
        assert_eq!(parsed, header);
    }

    #[test]
    fn test_serial_counter_increments() {
        let counter = SerialCounter::new();
        assert_eq!(counter.next(), 1);
        assert_eq!(counter.next(), 2);
        assert_eq!(counter.next(), 3);
    }

    #[test]
    fn test_serial_counter_is_monotonic() {
        let counter = SerialCounter::new();
        let mut prev = counter.next();
        for _ in 0..100 {
            let curr = counter.next();
            assert!(curr > prev);
            prev = curr;
        }
    }
}
