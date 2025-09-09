"""Unit tests for URL hashing utilities in hashutil."""

import pytest

from lb3.hashutil import extract_domain, hash_domain, hash_str, hash_url


class TestURLHashing:
    """Test URL hashing utilities."""
    
    def test_extract_domain(self):
        """Test domain extraction from URLs."""
        # Standard URLs
        assert extract_domain("https://www.google.com/search?q=test") == "www.google.com"
        assert extract_domain("http://example.org/path/to/page") == "example.org"
        assert extract_domain("https://subdomain.example.com:8080/") == "subdomain.example.com:8080"
        
        # URLs with ports
        assert extract_domain("http://localhost:3000/app") == "localhost:3000"
        assert extract_domain("https://api.example.com:443/v1/users") == "api.example.com:443"
        
        # Edge cases
        assert extract_domain("https://example.com") == "example.com"
        assert extract_domain("ftp://files.example.com/download") == "files.example.com"
        assert extract_domain("") == ""
        assert extract_domain("not-a-url") == ""
        
        # URLs without scheme
        assert extract_domain("//www.example.com/path") == "www.example.com"
    
    def test_hash_url(self):
        """Test URL hashing.""" 
        url1 = "https://www.google.com/search?q=test"
        url2 = "https://www.google.com/search?q=different"
        url3 = "https://www.google.com/search?q=test"  # Same as url1
        
        hash1 = hash_url(url1)
        hash2 = hash_url(url2)
        hash3 = hash_url(url3)
        
        # Hashes should be deterministic
        assert hash1 == hash3
        
        # Different URLs should have different hashes
        assert hash1 != hash2
        
        # Hashes should be hex strings
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA-256 hex length
        assert all(c in "0123456789abcdef" for c in hash1)
    
    def test_hash_domain(self):
        """Test domain hashing."""
        domain1 = "www.google.com"
        domain2 = "www.example.com"  
        domain3 = "www.google.com"  # Same as domain1
        
        hash1 = hash_domain(domain1)
        hash2 = hash_domain(domain2) 
        hash3 = hash_domain(domain3)
        
        # Hashes should be deterministic
        assert hash1 == hash3
        
        # Different domains should have different hashes
        assert hash1 != hash2
        
        # Hashes should be hex strings
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA-256 hex length
    
    def test_hash_consistency_with_hash_str(self):
        """Test that hash_url and hash_domain use hash_str with 'url' purpose."""
        test_url = "https://example.com/test"
        test_domain = "example.com"
        
        # Should be equivalent to direct hash_str calls
        assert hash_url(test_url) == hash_str(test_url, "url")
        assert hash_domain(test_domain) == hash_str(test_domain, "url")
    
    def test_empty_and_edge_case_hashing(self):
        """Test hashing of empty and edge case inputs.""" 
        # Empty strings should hash consistently
        empty_hash = hash_url("")
        assert isinstance(empty_hash, str)
        assert len(empty_hash) == 64
        
        # Same with domains
        empty_domain_hash = hash_domain("")
        assert isinstance(empty_domain_hash, str)
        assert len(empty_domain_hash) == 64
        
        # Edge case URLs
        weird_urls = [
            "javascript:void(0)",
            "data:text/html,<h1>Test</h1>", 
            "about:blank",
            "chrome://settings/",
            "file:///C:/test.html"
        ]
        
        for url in weird_urls:
            url_hash = hash_url(url)
            assert isinstance(url_hash, str)
            assert len(url_hash) == 64
            
            # Extract and hash domain
            domain = extract_domain(url)
            domain_hash = hash_domain(domain)
            assert isinstance(domain_hash, str)
            assert len(domain_hash) == 64
    
    def test_url_hash_privacy(self):
        """Test that URL hashing preserves privacy."""
        sensitive_urls = [
            "https://banking.example.com/account/123456789",
            "https://mail.google.com/mail/u/0/#inbox/abc123def456",
            "https://docs.google.com/document/d/secret-document-id/edit"
        ]
        
        for url in sensitive_urls:
            url_hash = hash_url(url)
            domain = extract_domain(url)
            domain_hash = hash_domain(domain)
            
            # Hash should not contain any part of the original URL
            assert "banking" not in url_hash
            assert "123456789" not in url_hash
            assert "secret-document-id" not in url_hash
            assert "abc123def456" not in url_hash
            
            # Domain hash should not contain domain parts (when domain is sensitive)
            if "banking" in domain:
                assert "banking" not in domain_hash
    
    def test_domain_extraction_consistency(self):
        """Test domain extraction consistency across related URLs."""
        base_domain = "www.example.com"
        
        urls_same_domain = [
            f"https://{base_domain}/",
            f"https://{base_domain}/page1",
            f"https://{base_domain}/page2?param=value",
            f"http://{base_domain}:8080/api/endpoint",
            f"https://{base_domain}/deep/path/to/resource"
        ]
        
        # All should extract to same base domain (except the one with port)
        domains = [extract_domain(url) for url in urls_same_domain]
        
        assert domains[0] == base_domain
        assert domains[1] == base_domain  
        assert domains[2] == base_domain
        assert domains[3] == f"{base_domain}:8080"  # Port included
        assert domains[4] == base_domain
    
    def test_hash_determinism_across_sessions(self):
        """Test that hashes are deterministic across different calls."""
        test_cases = [
            ("https://www.google.com", "www.google.com"),
            ("https://github.com/user/repo", "github.com"),
            ("http://localhost:3000", "localhost:3000"),
            ("", "")
        ]
        
        for url, expected_domain in test_cases:
            # Hash same URL/domain multiple times
            url_hashes = [hash_url(url) for _ in range(5)]
            domain_hashes = [hash_domain(expected_domain) for _ in range(5)]
            
            # All hashes should be identical
            assert len(set(url_hashes)) == 1, f"URL hashing not deterministic for {url}"
            assert len(set(domain_hashes)) == 1, f"Domain hashing not deterministic for {expected_domain}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])