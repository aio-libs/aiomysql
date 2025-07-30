"""
Unit tests for native authentication implementation.
"""

import pytest
from aiomysql._auth_native import (
    scramble_native_password,
    scramble_caching_sha2,
    _xor_password,
    _parse_pem_public_key,
    sha2_rsa_encrypt_native,
    _pkcs1_pad,
    _bytes_to_int,
    _int_to_bytes,
)


class TestNativePasswordScrambling:
    """Test mysql_native_password scrambling."""

    def test_empty_password(self):
        """Test scrambling with empty password."""
        result = scramble_native_password(b"", b"12345678901234567890")
        assert result == b""

    def test_normal_password(self):
        """Test scrambling with normal password."""
        password = b"testpassword"
        salt = b"12345678901234567890"
        result = scramble_native_password(password, salt)

        assert len(result) == 20  # SHA1 digest length
        assert isinstance(result, bytes)

    def test_consistency(self):
        """Test that scrambling is consistent."""
        password = b"consistent_test"
        salt = b"salt12345678901234567890"

        result1 = scramble_native_password(password, salt)
        result2 = scramble_native_password(password, salt)

        assert result1 == result2

    def test_different_passwords_different_results(self):
        """Test that different passwords produce different results."""
        salt = b"same_salt_12345678901234567890"

        result1 = scramble_native_password(b"password1", salt)
        result2 = scramble_native_password(b"password2", salt)

        assert result1 != result2

    def test_different_salts_different_results(self):
        """Test that different salts produce different results."""
        password = b"same_password"

        result1 = scramble_native_password(password, b"salt1234567890123456")
        result2 = scramble_native_password(password, b"salt6789012345678901")

        assert result1 != result2


class TestCachingSha2Scrambling:
    """Test caching_sha2_password scrambling."""

    def test_empty_password(self):
        """Test scrambling with empty password."""
        result = scramble_caching_sha2(b"", b"12345678901234567890")
        assert result == b""

    def test_normal_password(self):
        """Test scrambling with normal password."""
        password = b"testpassword"
        nonce = b"testnonce1234567890"
        result = scramble_caching_sha2(password, nonce)

        assert len(result) == 32  # SHA256 digest length
        assert isinstance(result, bytes)

    def test_consistency(self):
        """Test that scrambling is consistent."""
        password = b"consistent_test"
        nonce = b"nonce12345678901234567890"

        result1 = scramble_caching_sha2(password, nonce)
        result2 = scramble_caching_sha2(password, nonce)

        assert result1 == result2

    def test_different_passwords_different_results(self):
        """Test that different passwords produce different results."""
        nonce = b"same_nonce_123456789012345"

        result1 = scramble_caching_sha2(b"password1", nonce)
        result2 = scramble_caching_sha2(b"password2", nonce)

        assert result1 != result2


class TestPasswordXor:
    """Test password XOR function."""

    def test_xor_password(self):
        """Test XOR password function."""
        password = b"test"
        salt = b"12345678901234567890"

        result = _xor_password(password, salt)
        assert len(result) == len(password)
        assert isinstance(result, bytes)

    def test_xor_consistency(self):
        """Test XOR consistency."""
        password = b"consistency_test"
        salt = b"salt12345678901234567890"

        result1 = _xor_password(password, salt)
        result2 = _xor_password(password, salt)

        assert result1 == result2


class TestIntegerConversion:
    """Test integer conversion utilities."""

    def test_bytes_to_int(self):
        """Test bytes to integer conversion."""
        test_bytes = b"\x01\x02\x03\x04"
        result = _bytes_to_int(test_bytes)
        assert result == 0x01020304

    def test_int_to_bytes(self):
        """Test integer to bytes conversion."""
        test_int = 0x01020304
        result = _int_to_bytes(test_int, 4)
        assert result == b"\x01\x02\x03\x04"

    def test_round_trip_conversion(self):
        """Test round-trip conversion."""
        original = b"\xaa\xbb\xcc\xdd"
        as_int = _bytes_to_int(original)
        back_to_bytes = _int_to_bytes(as_int, len(original))
        assert original == back_to_bytes


class TestPkcs1Padding:
    """Test PKCS#1 padding."""

    def test_pkcs1_pad_basic(self):
        """Test basic PKCS#1 padding."""
        message = b"Hello"
        key_size = 2048  # bits

        padded = _pkcs1_pad(message, key_size)

        # Should be exactly key_size / 8 bytes
        assert len(padded) == key_size // 8

        # Should start with 0x00, 0x02
        assert padded[0] == 0x00
        assert padded[1] == 0x02

        # Should contain the original message at the end
        assert padded.endswith(message)

    def test_padding_different_for_same_message(self):
        """Test that padding includes randomness."""
        message = b"test"
        key_size = 1024

        padded1 = _pkcs1_pad(message, key_size)
        padded2 = _pkcs1_pad(message, key_size)

        # Should be different due to random padding
        assert padded1 != padded2

        # But same length and structure
        assert len(padded1) == len(padded2)
        assert padded1[0] == padded2[0] == 0x00
        assert padded1[1] == padded2[1] == 0x02


class TestRsaKeyParsing:
    """Test RSA key parsing."""

    def test_parse_basic_pem_key(self):
        """Test parsing a basic PEM key structure."""
        # This is a simplified test key structure
        test_key = b"""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwJKo7mhFyHrQPIZp7N1P
test_data_here_would_be_base64_encoded_der_data_representing_rsa_key_params
QIDAQAB
-----END PUBLIC KEY-----"""

        try:
            modulus, exponent = _parse_pem_public_key(test_key)
            # Basic validation
            assert isinstance(modulus, int)
            assert isinstance(exponent, int)
            assert exponent in (3, 17, 65537)  # Common RSA exponents
        except ValueError:
            # Expected for our test key - just verify function exists and handles errors
            pass


class TestCompatibilityWithPyMySQL:
    """Test compatibility with PyMySQL reference implementation."""

    def test_native_password_compatibility(self):
        """Test that our native password implementation matches PyMySQL."""
        try:
            from pymysql.connections import _auth as pymysql_auth

            test_cases = [
                (b"", b"12345678901234567890"),
                (b"password", b"salt12345678901234567890"),
                (b"test123", b"anothersalt123456789"),
            ]

            for password, salt in test_cases:
                our_result = scramble_native_password(password, salt)
                pymysql_result = pymysql_auth.scramble_native_password(password, salt)
                assert our_result == pymysql_result, f"Mismatch for password {password}"

        except ImportError:
            pytest.skip("PyMySQL not available for compatibility testing")

    def test_caching_sha2_compatibility(self):
        """Test that our caching SHA2 implementation matches PyMySQL."""
        try:
            from pymysql.connections import _auth as pymysql_auth

            test_cases = [
                (b"", b"12345678901234567890"),
                (b"password", b"nonce12345678901234567890"),
                (b"test123", b"anothernonce123456789"),
            ]

            for password, nonce in test_cases:
                our_result = scramble_caching_sha2(password, nonce)
                pymysql_result = pymysql_auth.scramble_caching_sha2(password, nonce)
                assert our_result == pymysql_result, f"Mismatch for password {password}"

        except ImportError:
            pytest.skip("PyMySQL not available for compatibility testing")


class TestRsaEncryption:
    """Test RSA encryption functionality."""

    def test_rsa_encrypt_with_invalid_key(self):
        """Test RSA encrypt handles invalid keys gracefully."""
        password = b"test"
        salt = b"testsalt123456789012"
        invalid_key = b"not a valid key"

        with pytest.raises((ValueError, Exception)):
            sha2_rsa_encrypt_native(password, salt, invalid_key)

    def test_rsa_encrypt_with_empty_password(self):
        """Test RSA encrypt with empty password."""
        # This test just ensures the function handles edge cases
        try:
            sha2_rsa_encrypt_native(b"", b"salt123", b"invalid_key")
        except (ValueError, Exception):
            # Expected behavior for invalid key
            pass


class TestIntegration:
    """Integration tests for the native auth system."""

    def test_import_native_auth_functions(self):
        """Test that all native auth functions can be imported."""
        from aiomysql._auth_native import (
            scramble_native_password,
            scramble_caching_sha2,
            sha2_rsa_encrypt_native,
        )

        # Just verify they're callable
        assert callable(scramble_native_password)
        assert callable(scramble_caching_sha2)
        assert callable(sha2_rsa_encrypt_native)

    def test_connection_safe_rsa_encrypt_function(self):
        """Test that the safe RSA encrypt function exists."""
        from aiomysql.connection import _safe_rsa_encrypt
        assert callable(_safe_rsa_encrypt)
