"""Tests for the main module."""

from datetime import datetime

import lmdb
import pytest
from lxml import etree

from src.wiki_dump_extractor.wiki_dump_extractor import (
    Page,
    WikiAvroDumpExtractor,
    WikiXmlDumpExtractor,
)

NS = "http://www.mediawiki.org/xml/export-0.11/"


def _page_elem(inner_xml: str) -> etree._Element:
    return etree.fromstring(f'<page xmlns="{NS}">{inner_xml}</page>'.encode())


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


def test_from_xml_complete_page():
    elem = _page_elem(
        """
        <title>Anarchism</title>
        <id>12</id>
        <revision>
          <id>1268985510</id>
          <timestamp>2025-01-12T13:23:25Z</timestamp>
          <text>Hello world</text>
        </revision>
        """
    )
    page = Page.from_xml(elem, NS)
    assert page.title == "Anarchism"
    assert page.page_id == 12
    assert page.revision_id == "1268985510"
    assert page.text == "Hello world"
    assert page.timestamp == datetime(2025, 1, 12, 13, 23, 25)
    assert page.redirect_title is None


def test_from_xml_redirect_page():
    elem = _page_elem(
        """
        <title>AccessibleComputing</title>
        <id>10</id>
        <redirect title="Computer accessibility"/>
        <revision>
          <id>1219062925</id>
          <timestamp>2024-04-15T14:38:04Z</timestamp>
          <text>#REDIRECT [[Computer accessibility]]</text>
        </revision>
        """
    )
    page = Page.from_xml(elem, NS)
    assert page.redirect_title == "Computer accessibility"
    assert page.text.startswith("#REDIRECT")


@pytest.mark.parametrize(
    "inner_xml, expected",
    [
        (
            """
            <title>Incomplete</title>
            <id>99</id>
            <revision>
              <id>1</id>
              <timestamp>2025-01-12T13:23:25Z</timestamp>
            </revision>
            """,
            {
                "title": "Incomplete",
                "page_id": 99,
                "revision_id": "1",
                "text": "",
                "timestamp": datetime(2025, 1, 12, 13, 23, 25),
            },
        ),
        (
            """
            <title>Suppressed</title>
            <id>100</id>
            <revision>
              <id>2</id>
              <timestamp>2025-01-12T13:23:25Z</timestamp>
              <text deleted="deleted" />
            </revision>
            """,
            {
                "title": "Suppressed",
                "page_id": 100,
                "revision_id": "2",
                "text": "",
                "timestamp": datetime(2025, 1, 12, 13, 23, 25),
            },
        ),
        (
            """
            <title>NoRevision</title>
            <id>101</id>
            """,
            {
                "title": "NoRevision",
                "page_id": 101,
                "revision_id": "",
                "text": "",
                "timestamp": None,
            },
        ),
        (
            "<ns>0</ns>",
            {
                "title": "",
                "page_id": 0,
                "revision_id": "",
                "text": "",
                "timestamp": None,
            },
        ),
        (
            """
            <title>Bad</title>
            <id>not-an-int</id>
            <revision>
              <id>3</id>
              <timestamp>not-a-date</timestamp>
              <text>ok</text>
            </revision>
            """,
            {
                "title": "Bad",
                "page_id": 0,
                "revision_id": "3",
                "text": "ok",
                "timestamp": None,
            },
        ),
    ],
)
def test_from_xml_missing_or_invalid_fields(inner_xml, expected):
    page = Page.from_xml(_page_elem(inner_xml), NS)
    assert page.title == expected["title"]
    assert page.page_id == expected["page_id"]
    assert page.revision_id == expected["revision_id"]
    assert page.text == expected["text"]
    assert page.timestamp == expected["timestamp"]
    assert page.redirect_title is None


def test_iter_pages_skips_missing_text_without_aborting(tmp_path):
    dump_path = tmp_path / "incomplete.xml"
    dump_path.write_text(
        f"""<?xml version="1.0" encoding="UTF-8"?>
<mediawiki xmlns="{NS}" version="0.11">
  <page>
    <title>Complete</title>
    <id>1</id>
    <revision>
      <id>10</id>
      <timestamp>2025-01-12T13:23:25Z</timestamp>
      <text>Hello</text>
    </revision>
  </page>
  <page>
    <title>MissingText</title>
    <id>2</id>
    <revision>
      <id>11</id>
      <timestamp>2025-01-12T13:23:25Z</timestamp>
    </revision>
  </page>
</mediawiki>
""",
        encoding="utf-8",
    )
    pages = list(WikiXmlDumpExtractor(dump_path).iter_pages())
    assert [page.title for page in pages] == ["Complete", "MissingText"]
    assert pages[0].text == "Hello"
    assert pages[1].text == ""
