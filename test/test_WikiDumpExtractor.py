"""Tests for the main module."""

from src.wiki_dump_extractor.wiki_dump_extractor import (
    WikiXmlDumpExtractor,
    WikiAvroDumpExtractor,
)
import lmdb


def test_WikiDumpExtractor():
    """Test the WikiDumpExtractor class."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    assert extractor.namespace == "http://www.mediawiki.org/xml/export-0.11/"
    assert len(list(extractor.iter_pages())) == 70


def test_WikiDumpExtractor_extract_pages_to_new_xml(tmp_path):
    """Test the extract_pages_to_new_xml method."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    extractor.extract_pages_to_new_xml(tmp_path / "tiny_dump_new.xml.bz2", limit=70)
    dump = WikiXmlDumpExtractor(tmp_path / "tiny_dump_new.xml.bz2")
    assert len(list(dump.iter_pages())) == 70
    assert (tmp_path / "tiny_dump_new.xml.bz2").exists()
    assert (tmp_path / "tiny_dump_new.xml.bz2").stat().st_size > 100_000


def test_WikiXmlDumpExtractor_iter_page_batches():
    """Test the iter_page_batches method."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    batches = list(extractor.iter_page_batches(batch_size=5, page_limit=18))
    assert len(batches) == 4
    assert len(batches[0]) == 5
    assert len(batches[-1]) == 3


def test_WikiXmlDumpExtractor_extract_pages_to_avro(tmp_path):
    """Test the extract_pages_to_avro method."""
    extractor = WikiXmlDumpExtractor("test/data/tiny_dump.xml.bz2")
    ignored_fields = ["timestamp", "page_id", "revision_id", "redirect_title"]
    redirects_db_path = tmp_path / "redirects.lmdb"
    page_index_db = tmp_path / "page_index.lmdb"

    extractor.extract_pages_to_avro(
        tmp_path / "tiny_dump.avro",
        batch_size=10,
        page_limit=70,
        ignored_fields=ignored_fields,
        redirects_db_path=redirects_db_path,
    )

    assert (tmp_path / "tiny_dump.avro").exists()
    assert (tmp_path / "tiny_dump.avro").stat().st_size > 100_000
    assert redirects_db_path.exists()

    avro_extractor = WikiAvroDumpExtractor(tmp_path / "tiny_dump.avro")
    print([page.title for page in avro_extractor.iter_pages()])
    assert len(list(avro_extractor.iter_pages())) == 6

    dump = WikiAvroDumpExtractor(tmp_path / "tiny_dump.avro")
    dump.index_pages(page_index_db)
    assert page_index_db.exists()

    # Test that redirects were stored correctly
    env = lmdb.open(str(redirects_db_path), readonly=True)
    with env.begin() as txn:
        # Just check that we have some redirects
        cursor = txn.cursor()
        redirect_count = sum(1 for _ in cursor)
        assert redirect_count == 64
    env.close()

    # Test that page index was created correctly
    env = lmdb.open(str(page_index_db), readonly=True)
    with env.begin() as txn:
        # Check that we have some page indices
        cursor = txn.cursor()
        index_count = sum(1 for _ in cursor)
        assert index_count > 0

        # Check that indices are valid file positions
        for _, pos in cursor:
            assert int(pos.decode("utf-8")) >= 0
    env.close()
