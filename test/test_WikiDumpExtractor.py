"""Tests for the main module."""

from src.wiki_dump_extractor.wiki_dump_extractor import (
    WikiXmlDumpExtractor,
    WikiAvroDumpExtractor,
)


def test_WikiDumpExtractor():
    """Test the WikiDumpExtractor class."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    assert extractor.namespace == "http://www.mediawiki.org/xml/export-0.11/"
    assert len(list(extractor.iter_pages())) == 70


def test_WikiDumpExtractor_extract_pages_to_new_xml(tmp_path):
    """Test the extract_pages_to_new_xml method."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    extractor.extract_pages_to_new_xml(tmp_path / "tiny_dump_new.xml.bz2", limit=70)
    assert (tmp_path / "tiny_dump_new.xml.bz2").exists()
    assert (tmp_path / "tiny_dump_new.xml.bz2").stat().st_size > 100_000


def test_WikiXmlDumpExtractor_iter_page_batches():
    """Test the iter_page_batches method."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    batches = list(extractor.iter_page_batches(batch_size=5, limit=10))
    assert len(batches) == 10
    assert len(batches[0]) == 5
    assert len(batches[-1]) == 5


def test_WikiXmlDumpExtractor_extract_pages_to_avro(tmp_path):
    """Test the extract_pages_to_avro method."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    ignored_fields = ["timestamp", "page_id", "revision_id", "redirect_title"]
    extractor.extract_pages_to_avro(
        tmp_path / "tiny_dump.avro",
        batch_size=10,
        limit=7,
        ignored_fields=ignored_fields,
        ignore_redirects=False,
    )

    assert (tmp_path / "tiny_dump.avro").exists()
    assert (tmp_path / "tiny_dump.avro").stat().st_size > 100_000
    avro_extractor = WikiAvroDumpExtractor(tmp_path / "tiny_dump.avro")
    assert len(list(avro_extractor.iter_pages())) == 70
