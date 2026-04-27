use chacha20::ChaCha20;
use cipher::{KeyIvInit, StreamCipher};

use super::constants::CHACHA20_KEY_LEN;

/// Per-direction ChaCha20 cipher state.
///
/// The counter advances continuously across messages (never reset).
struct ChaCha20State {
    cipher: ChaCha20,
}

impl ChaCha20State {
    fn new(key: &[u8]) -> Self {
        let nonce = [0u8; 12];
        let cipher = ChaCha20::new_from_slices(key, &nonce).expect("key length must be 32 bytes");
        Self { cipher }
    }

    fn apply(&mut self, data: &mut [u8]) {
        self.cipher.apply_keystream(data);
    }
}

/// Bidirectional ChaCha20 encryption for the native TCP protocol.
///
/// Maintains separate cipher states for send and receive directions.
/// The cipher counter advances continuously across messages.
pub struct ChaCha20Encryptor {
    send_cipher: Option<ChaCha20State>,
    recv_cipher: Option<ChaCha20State>,
}

impl ChaCha20Encryptor {
    pub fn new() -> Self {
        Self {
            send_cipher: None,
            recv_cipher: None,
        }
    }

    /// Generate a pair of 32-byte random keys (send_key, recv_key).
    pub fn generate_keys() -> (Vec<u8>, Vec<u8>) {
        use aws_lc_rs::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        let mut send_key = vec![0u8; CHACHA20_KEY_LEN];
        let mut recv_key = vec![0u8; CHACHA20_KEY_LEN];
        rng.fill(&mut send_key).expect("RNG fill failed");
        rng.fill(&mut recv_key).expect("RNG fill failed");
        (send_key, recv_key)
    }

    /// Initialize the cipher states with the given keys.
    pub fn set_keys(&mut self, send_key: &[u8], recv_key: &[u8]) {
        self.send_cipher = Some(ChaCha20State::new(send_key));
        self.recv_cipher = Some(ChaCha20State::new(recv_key));
    }

    /// Encrypt data in place using the send cipher.
    pub fn encrypt(&mut self, data: &mut [u8]) {
        if let Some(state) = &mut self.send_cipher {
            state.apply(data);
        }
    }

    /// Decrypt data in place using the receive cipher.
    pub fn decrypt(&mut self, data: &mut [u8]) {
        if let Some(state) = &mut self.recv_cipher {
            state.apply(data);
        }
    }

    /// Returns true if encryption keys have been set.
    pub fn is_active(&self) -> bool {
        self.send_cipher.is_some() && self.recv_cipher.is_some()
    }
}

impl Default for ChaCha20Encryptor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chacha20_key_generation() {
        let (send_key, recv_key) = ChaCha20Encryptor::generate_keys();

        assert_eq!(send_key.len(), CHACHA20_KEY_LEN);
        assert_eq!(recv_key.len(), CHACHA20_KEY_LEN);

        // Keys should not be all zeros (statistically impossible).
        assert!(send_key.iter().any(|&b| b != 0));
        assert!(recv_key.iter().any(|&b| b != 0));

        // Two independently generated keys should differ.
        assert_ne!(send_key, recv_key);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let (send_key, recv_key) = ChaCha20Encryptor::generate_keys();

        // Sender encrypts with send_key, receiver decrypts with the same key.
        let mut encryptor = ChaCha20Encryptor::new();
        encryptor.set_keys(&send_key, &recv_key);

        // Receiver uses swapped keys: what sender sends, receiver receives.
        let mut decryptor = ChaCha20Encryptor::new();
        decryptor.set_keys(&recv_key, &send_key);

        let original = b"Hello, Exasol native protocol!".to_vec();
        let mut data = original.clone();

        encryptor.encrypt(&mut data);
        // Encrypted data should differ from original.
        assert_ne!(data, original);

        decryptor.decrypt(&mut data);
        // After decryption, data matches the original.
        assert_eq!(data, original);
    }

    #[test]
    fn test_stream_continuity() {
        let key = vec![0x42u8; CHACHA20_KEY_LEN];

        let mut enc = ChaCha20Encryptor::new();
        enc.set_keys(&key, &key);

        let mut dec = ChaCha20Encryptor::new();
        dec.set_keys(&key, &key);

        // Encrypt two separate messages.
        let msg1_original = b"first message".to_vec();
        let msg2_original = b"second message".to_vec();

        let mut msg1 = msg1_original.clone();
        let mut msg2 = msg2_original.clone();

        enc.encrypt(&mut msg1);
        enc.encrypt(&mut msg2);

        // Decrypt in the same order using the receive cipher.
        dec.decrypt(&mut msg1);
        dec.decrypt(&mut msg2);

        assert_eq!(msg1, msg1_original);
        assert_eq!(msg2, msg2_original);
    }

    #[test]
    fn test_inactive_before_keys_set() {
        let enc = ChaCha20Encryptor::new();
        assert!(!enc.is_active());
    }

    #[test]
    fn test_active_after_keys_set() {
        let (send_key, recv_key) = ChaCha20Encryptor::generate_keys();
        let mut enc = ChaCha20Encryptor::new();
        enc.set_keys(&send_key, &recv_key);
        assert!(enc.is_active());
    }

    #[test]
    fn test_no_op_without_keys() {
        let mut enc = ChaCha20Encryptor::new();
        let original = b"unchanged data".to_vec();
        let mut data = original.clone();

        enc.encrypt(&mut data);
        assert_eq!(data, original, "encrypt should be a no-op without keys");

        enc.decrypt(&mut data);
        assert_eq!(data, original, "decrypt should be a no-op without keys");
    }
}
