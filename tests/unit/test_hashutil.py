"""Tests for hash utilities."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from lb3.config import Config, HashingConfig
from lb3.hashutil import hash_bytes, hash_file, hash_str, hash_string, verify_hash


class TestPurposeScopedHashing:
    """Test purpose-scoped salted SHA-256 hashing."""

    def test_hash_str_basic_functionality(self):
        """Test basic hash_str functionality."""
        value = "test_value"
        purpose = "window_title"

        result = hash_str(value, purpose)

        # Should be a hex string
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex length
        assert all(c in "0123456789abcdef" for c in result)

    def test_purpose_scoping(self):
        """Test that different purposes produce different hashes."""
        value = "same_value"
        purposes = ["window_title", "file_path", "url", "exe_path", "free_text"]

        hashes = {}
        for purpose in purposes:
            hash_result = hash_str(value, purpose)
            assert purpose not in hashes or hashes[purpose] != hash_result
            hashes[purpose] = hash_result

        # All hashes should be different
        hash_values = list(hashes.values())
        assert len(set(hash_values)) == len(
            hash_values
        ), "All purpose hashes should be unique"

    def test_same_input_same_purpose_same_hash(self):
        """Test that same input with same purpose produces same hash."""
        value = "consistent_value"
        purpose = "window_title"

        hash1 = hash_str(value, purpose)
        hash2 = hash_str(value, purpose)

        assert hash1 == hash2, "Same input and purpose should produce same hash"

    def test_different_values_different_hashes(self):
        """Test that different values produce different hashes."""
        purpose = "window_title"

        hash1 = hash_str("value1", purpose)
        hash2 = hash_str("value2", purpose)

        assert hash1 != hash2, "Different values should produce different hashes"

    def test_empty_string_hashing(self):
        """Test hashing of empty strings."""
        empty_hash = hash_str("", "window_title")
        non_empty_hash = hash_str("not_empty", "window_title")

        assert empty_hash != non_empty_hash
        assert len(empty_hash) == 64

    def test_unicode_handling(self):
        """Test hashing of Unicode strings."""
        unicode_value = "test_ñ_🔒_中文"

        hash_result = hash_str(unicode_value, "free_text")

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_salt_effect(self):
        """Test that changing salt changes hash output."""
        value = "test_value"
        purpose = "window_title"

        # Get hash with default salt
        original_hash = hash_str(value, purpose)

        # Mock different salt
        different_config = Config(
            hashing=HashingConfig(
                salt="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )
        )

        with patch("lb3.hashutil.get_effective_config", return_value=different_config):
            different_salt_hash = hash_str(value, purpose)

        assert (
            original_hash != different_salt_hash
        ), "Different salts should produce different hashes"

    def test_hash_formula_structure(self):
        """Test that hash follows the expected formula structure."""
        import hashlib

        from lb3.config import get_effective_config

        value = "test_value"
        purpose = "window_title"
        config = get_effective_config()

        # Manual calculation
        salt_bytes = bytes.fromhex(config.hashing.salt)
        hasher = hashlib.sha256()
        hasher.update(salt_bytes)
        hasher.update(purpose.encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(value.encode("utf-8"))
        expected_hash = hasher.hexdigest()

        # Function result
        actual_hash = hash_str(value, purpose)

        assert (
            actual_hash == expected_hash
        ), "Hash should follow salt||purpose||0x00||utf8(value) formula"

    def test_all_purpose_literals(self):
        """Test that all defined purpose literals work."""
        value = "test_value"
        purposes = ["window_title", "file_path", "url", "exe_path", "free_text"]

        for purpose in purposes:
            hash_result = hash_str(value, purpose)
            assert len(hash_result) == 64
            assert all(c in "0123456789abcdef" for c in hash_result)

    def test_concurrent_hashing(self):
        """Test concurrent hashing produces consistent results."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        value = "concurrent_test"
        purpose = "window_title"
        num_threads = 20
        hashes_per_thread = 10

        def hash_multiple():
            return [hash_str(value, purpose) for _ in range(hashes_per_thread)]

        all_hashes = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(hash_multiple) for _ in range(num_threads)]

            for future in as_completed(futures):
                thread_hashes = future.result()
                all_hashes.extend(thread_hashes)

        # All hashes should be identical
        expected_hash = hash_str(value, purpose)
        for hash_result in all_hashes:
            assert (
                hash_result == expected_hash
            ), "Concurrent hashing should be consistent"


class TestLegacyHashFunctions:
    """Test legacy hash functions for backward compatibility."""

    def test_hash_string_functionality(self):
        """Test legacy hash_string function."""
        text = "test_string"
        result = hash_string(text)

        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex

    def test_hash_string_different_algorithms(self):
        """Test hash_string with different algorithms."""
        text = "test_string"

        sha256_hash = hash_string(text, "sha256")
        md5_hash = hash_string(text, "md5")

        assert len(sha256_hash) == 64
        assert len(md5_hash) == 32
        assert sha256_hash != md5_hash

    def test_hash_bytes_functionality(self):
        """Test hash_bytes function."""
        data = b"test_bytes"
        result = hash_bytes(data)

        assert isinstance(result, str)
        assert len(result) == 64

    def test_hash_file_functionality(self):
        """Test hash_file function."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            temp_file.write("test content for file hashing")
            temp_path = Path(temp_file.name)

        try:
            file_hash = hash_file(temp_path)

            assert isinstance(file_hash, str)
            assert len(file_hash) == 64

            # Hash should be consistent
            file_hash2 = hash_file(temp_path)
            assert file_hash == file_hash2

        finally:
            temp_path.unlink()

    def test_verify_hash_string(self):
        """Test hash verification for strings."""
        text = "verification_test"
        expected_hash = hash_string(text)

        assert verify_hash(text, expected_hash)
        assert not verify_hash(text, "wrong_hash")
        assert not verify_hash("different_text", expected_hash)

    def test_verify_hash_bytes(self):
        """Test hash verification for bytes."""
        data = b"verification_test_bytes"
        expected_hash = hash_bytes(data)

        assert verify_hash(data, expected_hash)
        assert not verify_hash(data, "wrong_hash")
        assert not verify_hash(b"different_data", expected_hash)

    def test_verify_hash_case_insensitive(self):
        """Test that hash verification is case insensitive."""
        text = "case_test"
        expected_hash = hash_string(text).upper()

        assert verify_hash(text, expected_hash.lower())
        assert verify_hash(text, expected_hash.upper())


class TestHashConsistency:
    """Test hash consistency across different scenarios."""

    def test_cross_platform_consistency(self):
        """Test that hashing produces consistent results."""
        # This test ensures that our hash implementation is deterministic
        test_cases = [
            ("", "window_title"),
            ("simple", "file_path"),
            ("with spaces", "url"),
            ("special!@#$%^&*()", "exe_path"),
            ("unicode_ñ_🔒_中文", "free_text"),
            ("very_long_string_" * 100, "window_title"),
        ]

        # Generate hashes and verify they're consistent
        for value, purpose in test_cases:
            hash1 = hash_str(value, purpose)
            hash2 = hash_str(value, purpose)

            assert (
                hash1 == hash2
            ), f"Hash should be consistent for {value!r} with {purpose}"
            assert len(hash1) == 64, f"Hash should be 64 chars for {value!r}"

    def test_salt_isolation(self):
        """Test that different salts properly isolate hash spaces."""
        value = "isolation_test"
        purpose = "window_title"

        # Create configs with different salts
        config1 = Config(
            hashing=HashingConfig(
                salt="1111111111111111111111111111111111111111111111111111111111111111"
            )
        )
        config2 = Config(
            hashing=HashingConfig(
                salt="2222222222222222222222222222222222222222222222222222222222222222"
            )
        )

        with patch("lb3.hashutil.get_effective_config", return_value=config1):
            hash1 = hash_str(value, purpose)

        with patch("lb3.hashutil.get_effective_config", return_value=config2):
            hash2 = hash_str(value, purpose)

        assert hash1 != hash2, "Different salts should produce different hashes"
        assert len(hash1) == len(hash2) == 64, "Both hashes should be valid SHA-256"
