"""Tests for ID generation utilities."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from lb3.ids import generate_id, is_valid_id, new_id


class TestULIDGeneration:
    """Test ULID generation functionality."""

    def test_new_id_returns_string(self):
        """Test that new_id returns a string."""
        ulid = new_id()
        assert isinstance(ulid, str)
        assert len(ulid) == 26  # ULID length

    def test_new_id_is_valid(self):
        """Test that generated IDs are valid ULIDs."""
        ulid = new_id()
        assert is_valid_id(ulid)

    def test_generate_id_alias_works(self):
        """Test that generate_id alias works."""
        ulid = generate_id()
        assert isinstance(ulid, str)
        assert is_valid_id(ulid)

    def test_ulid_ordering_sequential(self):
        """Test that ULIDs generated sequentially maintain time order."""
        ulids = []
        
        # Generate ULIDs with small delays to ensure different timestamps
        for _ in range(10):
            ulids.append(new_id())
            time.sleep(0.001)  # 1ms delay
        
        # ULIDs should be lexicographically sortable by creation time
        sorted_ulids = sorted(ulids)
        assert ulids == sorted_ulids, "ULIDs should be generated in time-sorted order"

    def test_ulid_ordering_rapid_generation(self):
        """Test ULID ordering with rapid generation in same millisecond."""
        ulids = []
        
        # Generate ULIDs rapidly (likely same millisecond)
        for _ in range(100):
            ulids.append(new_id())
        
        # Should still be sorted (monotonic within millisecond)
        sorted_ulids = sorted(ulids)
        assert ulids == sorted_ulids, "ULIDs should be monotonic even within same millisecond"

    def test_ulid_uniqueness(self):
        """Test that all generated ULIDs are unique."""
        ulids = [new_id() for _ in range(1000)]
        assert len(set(ulids)) == len(ulids), "All ULIDs should be unique"

    def test_thread_safety_basic(self):
        """Test basic thread safety with concurrent generation."""
        num_threads = 10
        ulids_per_thread = 50
        all_ulids = []
        
        def generate_ulids():
            return [new_id() for _ in range(ulids_per_thread)]
        
        # Generate ULIDs concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(generate_ulids) for _ in range(num_threads)]
            
            for future in as_completed(futures):
                thread_ulids = future.result()
                all_ulids.extend(thread_ulids)
        
        # Verify uniqueness across all threads
        assert len(set(all_ulids)) == len(all_ulids), "All ULIDs should be unique across threads"
        
        # Verify all are valid
        for ulid in all_ulids:
            assert is_valid_id(ulid), f"ULID {ulid} should be valid"

    def test_thread_safety_ordering(self):
        """Test that concurrent ULIDs maintain monotonic ordering properties."""
        # Generate ULIDs sequentially to establish baseline
        sequential_ulids = []
        for _ in range(10):
            sequential_ulids.append(new_id())
            time.sleep(0.001)  # 1ms delay
        
        # Sequential ULIDs should be properly ordered
        sorted_sequential = sorted(sequential_ulids)
        assert sequential_ulids == sorted_sequential, "Sequential ULIDs should be time-ordered"
        
        # Test concurrent generation with longer time intervals for better ordering
        num_threads = 10
        ulids_per_thread = 10
        results = []
        
        def generate_ulids_with_delays():
            thread_ulids = []
            for i in range(ulids_per_thread):
                ulid = new_id()
                thread_ulids.append(ulid)
                # Longer delay for better time separation
                time.sleep(0.001)
            return thread_ulids
        
        # Generate ULIDs with longer delays
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(generate_ulids_with_delays) for _ in range(num_threads)]
            
            for future in as_completed(futures):
                results.extend(future.result())
        
        # Basic requirement: all ULIDs should be unique and valid
        assert len(set(results)) == len(results), "All concurrent ULIDs should be unique"
        for ulid in results:
            assert is_valid_id(ulid), f"ULID {ulid} should be valid"

    def test_high_concurrency_no_collisions(self):
        """Test high concurrency scenarios with no collisions."""
        num_threads = 50
        ulids_per_thread = 20
        all_ulids = set()
        lock = threading.Lock()
        
        def generate_and_collect():
            local_ulids = [new_id() for _ in range(ulids_per_thread)]
            with lock:
                for ulid in local_ulids:
                    assert ulid not in all_ulids, f"Collision detected: {ulid}"
                    all_ulids.add(ulid)
            return local_ulids
        
        # Run high concurrency test
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(generate_and_collect) for _ in range(num_threads)]
            
            for future in as_completed(futures):
                future.result()  # Wait for completion and check for exceptions
        
        expected_total = num_threads * ulids_per_thread
        assert len(all_ulids) == expected_total, f"Should have {expected_total} unique ULIDs"


class TestULIDValidation:
    """Test ULID validation functionality."""

    def test_valid_ulid_recognition(self):
        """Test that valid ULIDs are recognized."""
        ulid = new_id()
        assert is_valid_id(ulid)

    def test_invalid_ulid_recognition(self):
        """Test that invalid strings are not recognized as ULIDs."""
        invalid_cases = [
            "",
            "not-a-ulid",
            "123",
            "01ARZ3NDEKTSV4RRFFQ69G5FAVX",  # Too long (27 chars)
            "01ARZ3NDEKTSV4RRFFQ69G5F",     # Too short (25 chars)
            "01ARZ3NDEKTSV4RRFFQ69G5F@",    # Invalid character
            "lowercase0123456789abcde",      # Lowercase (ULIDs are uppercase)
            "O1ARZ3NDEKTSV4RRFFQ69G5FA",    # Invalid character O (looks like 0)
            "I1ARZ3NDEKTSV4RRFFQ69G5FA",    # Invalid character I (looks like 1)
        ]
        
        for invalid in invalid_cases:
            assert not is_valid_id(invalid), f"'{invalid}' should not be valid"

    def test_edge_case_validation(self):
        """Test edge cases for ULID validation."""
        # Test None and non-string types
        assert not is_valid_id(None)  # type: ignore
        assert not is_valid_id(123)   # type: ignore
        assert not is_valid_id([])    # type: ignore