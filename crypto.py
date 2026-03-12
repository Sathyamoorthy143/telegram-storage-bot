from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import os


def encrypt_file(data: bytes) -> tuple[bytes, bytes, bytes]:
    """
    Encrypts bytes with AES-256-GCM.
    Returns (ciphertext, nonce, key)
    """
    key    = AESGCM.generate_key(bit_length=256)  # 32 bytes
    nonce  = os.urandom(12)                         # 96-bit nonce
    aesgcm = AESGCM(key)
    encrypted = aesgcm.encrypt(nonce, data, None)
    return encrypted, nonce, key


def decrypt_file(encrypted: bytes, nonce: bytes, key: bytes) -> bytes:
    """
    Decrypts AES-256-GCM ciphertext.
    Returns plaintext bytes.
    """
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, encrypted, None)
