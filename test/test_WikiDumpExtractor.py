"""Tests for the main module."""

from src.wiki_dump_extractor.wiki_dump_extractor import WikiDumpExtractor


def test_WikiDumpExtractor():
    """Test the WikiDumpExtractor class."""
    extractor = WikiDumpExtractor("test/data/tiny_dump.xml.bz2")
    assert extractor.namespace == "http://www.mediawiki.org/xml/export-0.11/"
    assert len(list(extractor.iter_pages())) == 70


def test_WikiDumpExtractor_extract_pages_to_new_xml(tmp_path):
    """Test the extract_pages_to_new_xml method."""
    extractor = WikiDumpExtractor("test/data/tiny_dump.xml.bz2")
    extractor.extract_pages_to_new_xml(tmp_path / "tiny_dump_new.xml.bz2", limit=70)
    assert (tmp_path / "tiny_dump_new.xml.bz2").exists()
    assert (tmp_path / "tiny_dump_new.xml.bz2").stat().st_size > 100_000


def test_WikiDumpExtractor_iter_page_batches():
    """Test the iter_page_batches method."""
    extractor = WikiDumpExtractor("test/data/tiny_dump.xml.bz2")
    batches = list(extractor.iter_page_batches(batch_size=5, limit=10))
    assert len(batches) == 10
    assert len(batches[0]) == 5
    assert len(batches[-1]) == 5
