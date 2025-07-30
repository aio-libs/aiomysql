"""
Native Python implementation of MySQL authentication methods
without requiring cryptography package.
"""

import hashlib
from functools import partial


sha1_new = partial(hashlib.new, "sha1")
SCRAMBLE_LENGTH = 20


def _my_crypt(message1, message2):
    """XOR two byte sequences"""
    result = bytearray(message1)
    for i in range(len(result)):
        result[i] ^= message2[i]
    return bytes(result)


def _xor_password(password, salt):
    """XOR password with salt for RSA encryption"""
    salt = salt[:SCRAMBLE_LENGTH]
    password_bytes = bytearray(password)
    salt_len = len(salt)
    for i in range(len(password_bytes)):
        password_bytes[i] ^= salt[i % salt_len]
    return bytes(password_bytes)


def scramble_native_password(password, message):
    """Scramble used for mysql_native_password"""
    if not password:
        return b""

    stage1 = sha1_new(password).digest()
    stage2 = sha1_new(stage1).digest()
    s = sha1_new()
    s.update(message[:SCRAMBLE_LENGTH])
    s.update(stage2)
    result = s.digest()
    return _my_crypt(result, stage1)


def scramble_caching_sha2(password, nonce):
    """Scramble algorithm used in cached_sha2_password fast path.

    XOR(SHA256(password), SHA256(SHA256(SHA256(password)), nonce))

    Note: This uses SHA256 as specified by the MySQL protocol RFC, not for
    secure password storage. This is a challenge-response mechanism where
    the actual password verification is done server-side with proper
    password hashing algorithms.
    """
    if not password:
        return b""

    # MySQL protocol specified SHA256 usage - not for password storage
    p1 = hashlib.sha256(password).digest()  # nosec B324
    p2 = hashlib.sha256(p1).digest()  # nosec B324
    p3 = hashlib.sha256(p2 + nonce).digest()  # nosec B324

    res = bytearray(p1)
    for i in range(len(p3)):
        res[i] ^= p3[i]

    return bytes(res)


# Native RSA implementation using standard library
def _bytes_to_int(data):
    """Convert bytes to integer"""
    return int.from_bytes(data, byteorder='big')


def _int_to_bytes(value, length):
    """Convert integer to bytes with specified length"""
    return value.to_bytes(length, byteorder='big')


def _parse_pem_public_key(pem_data):
    """Parse PEM public key and extract RSA parameters"""
    if isinstance(pem_data, str):
        pem_data = pem_data.encode('ascii')

    # Remove PEM headers/footers and decode base64
    import base64
    lines = pem_data.strip().split(b'\n')
    key_data = b''.join(line for line in lines
                        if not line.startswith(b'-----'))
    der_data = base64.b64decode(key_data)

    # Parse DER-encoded public key (simplified ASN.1 parsing)
    # This is a basic implementation for MySQL's RSA keys
    try:
        return _parse_der_public_key(der_data)
    except Exception:
        # Fallback: try to extract modulus and exponent from common formats
        return _extract_rsa_params_fallback(der_data)


def _parse_der_public_key(der_data):
    """Parse DER-encoded RSA public key"""
    # Very basic ASN.1 parsing for RSA public keys
    # Format: SEQUENCE { modulus INTEGER, publicExponent INTEGER }

    pos = 0

    # Skip SEQUENCE tag and length
    if der_data[pos] != 0x30:  # SEQUENCE tag
        raise ValueError("Invalid DER format")
    pos += 1

    # Skip length bytes
    length_byte = der_data[pos]
    pos += 1
    if length_byte & 0x80:
        length_bytes = length_byte & 0x7f
        pos += length_bytes

    # Skip algorithm identifier sequence (if present)
    if der_data[pos] == 0x30:
        pos += 1
        alg_len = der_data[pos]
        pos += 1
        if alg_len & 0x80:
            length_bytes = alg_len & 0x7f
            pos += length_bytes
        else:
            pos += alg_len

    # Skip BIT STRING tag and length for public key
    if der_data[pos] == 0x03:  # BIT STRING
        pos += 1
        bit_len = der_data[pos]
        pos += 1
        if bit_len & 0x80:
            length_bytes = bit_len & 0x7f
            pos += length_bytes
        pos += 1  # Skip unused bits byte

    # Parse the actual RSA key
    if der_data[pos] != 0x30:  # SEQUENCE for RSA key
        raise ValueError("Invalid RSA key format")
    pos += 1

    # Skip sequence length
    seq_len = der_data[pos]
    pos += 1
    if seq_len & 0x80:
        length_bytes = seq_len & 0x7f
        pos += length_bytes

    # Parse modulus (n)
    if der_data[pos] != 0x02:  # INTEGER tag
        raise ValueError("Expected modulus integer")
    pos += 1

    mod_len = der_data[pos]
    pos += 1
    if mod_len & 0x80:
        length_bytes = mod_len & 0x7f
        mod_len = 0
        for i in range(length_bytes):
            mod_len = (mod_len << 8) | der_data[pos]
            pos += 1

    # Skip leading zero if present
    if der_data[pos] == 0x00:
        pos += 1
        mod_len -= 1

    modulus = _bytes_to_int(der_data[pos:pos + mod_len])
    pos += mod_len

    # Parse exponent (e)
    if der_data[pos] != 0x02:  # INTEGER tag
        raise ValueError("Expected exponent integer")
    pos += 1

    exp_len = der_data[pos]
    pos += 1
    if exp_len & 0x80:
        length_bytes = exp_len & 0x7f
        exp_len = 0
        for i in range(length_bytes):
            exp_len = (exp_len << 8) | der_data[pos]
            pos += 1

    exponent = _bytes_to_int(der_data[pos:pos + exp_len])

    return modulus, exponent


def _extract_rsa_params_fallback(der_data):
    """Fallback method to extract RSA parameters"""
    # This is a more permissive parser for various key formats

    # Look for INTEGER sequences (modulus and exponent)
    integers = []
    pos = 0

    while pos < len(der_data) - 3:
        if der_data[pos] == 0x02:  # INTEGER tag
            pos += 1
            length = der_data[pos]
            pos += 1

            if length & 0x80:
                length_bytes = length & 0x7f
                if length_bytes > 4 or pos + length_bytes >= len(der_data):
                    pos += 1
                    continue
                length = 0
                for i in range(length_bytes):
                    length = (length << 8) | der_data[pos]
                    pos += 1

            if length > 0 and pos + length <= len(der_data):
                # Skip leading zero
                start_pos = pos
                if der_data[pos] == 0x00 and length > 1:
                    start_pos += 1
                    length -= 1

                if length > 16:  # Reasonable size for RSA components
                    value = _bytes_to_int(der_data[start_pos:start_pos + length])
                    integers.append(value)
                    # Also check for common exponents
                elif length <= 8 and length > 0:  # Could be exponent
                    value = _bytes_to_int(der_data[start_pos:start_pos + length])
                    if value in (3, 17, 65537):  # Common RSA exponents
                        integers.append(value)

                pos += length
            else:
                pos += 1
        else:
            pos += 1

    if len(integers) >= 2:
        # Find modulus (largest) and exponent (common values)
        modulus = max(integers)
        exponent = 65537  # Default

        for i in integers:
            if i != modulus and i in (3, 17, 65537):
                exponent = i
                break

        return modulus, exponent

    raise ValueError("Could not extract RSA parameters")


def _pkcs1_pad(message, key_size):
    """Apply PKCS#1 v1.5 padding for encryption"""
    # PKCS#1 v1.5 padding format: 0x00 || 0x02 || PS || 0x00 || M
    # where PS is random non-zero padding bytes

    import os

    message_len = len(message)
    padded_len = (key_size + 7) // 8  # Key size in bytes

    if message_len > padded_len - 11:
        raise ValueError("Message too long for key size")

    padding_len = padded_len - message_len - 3

    # Generate random non-zero padding with better entropy
    padding = bytearray()
    attempts = 0
    max_attempts = padding_len * 10

    while len(padding) < padding_len and attempts < max_attempts:
        rand_bytes = os.urandom(min(256, padding_len - len(padding)))
        for b in rand_bytes:
            if b != 0 and len(padding) < padding_len:
                padding.append(b)
        attempts += 1

    # If we couldn't generate enough random bytes, fill with safe non-zero values
    while len(padding) < padding_len:
        padding.append(0xFF)

    padded = bytes([0x00, 0x02]) + bytes(padding) + bytes([0x00]) + message
    return padded


def _mod_exp(base, exponent, modulus):
    """Compute (base^exponent) mod modulus efficiently"""
    return pow(base, exponent, modulus)


def _rsa_encrypt_native(message, modulus, exponent):
    """Encrypt message using RSA with native Python implementation"""
    # Determine key size in bits
    key_size = modulus.bit_length()

    # Apply PKCS#1 v1.5 padding
    padded_message = _pkcs1_pad(message, key_size)

    # Convert to integer
    message_int = _bytes_to_int(padded_message)

    # Perform RSA encryption: c = m^e mod n
    ciphertext_int = _mod_exp(message_int, exponent, modulus)

    # Convert back to bytes
    ciphertext_len = (key_size + 7) // 8
    return _int_to_bytes(ciphertext_int, ciphertext_len)


def sha2_rsa_encrypt_native(password, salt, public_key):
    """Encrypt password with salt and public key using native Python.

    Used for sha256_password and caching_sha2_password.
    """
    message = _xor_password(password + b"\0", salt)

    # Parse the PEM public key
    modulus, exponent = _parse_pem_public_key(public_key)

    # Encrypt using native RSA implementation
    return _rsa_encrypt_native(message, modulus, exponent)
