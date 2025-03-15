from abc import ABC, abstractmethod
from typing import Iterator, Union, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from copy import deepcopy
import bz2
import fastavro
from lxml import etree


@dataclass
class Page:
    """
    Represents a page in the Wikipedia dump.

    Attributes
    ----------
    page_id: int
        The ID of the page.
    title: str
        The title of the page.
    timestamp: datetime
        The timestamp of the page.
    redirect_title: str | None
        The title of the page if it is a redirect.
    revision_id: str
        The ID of the revision.
    text: str
        The text of the page.
    """

    title: str = ""
    text: str = ""
    page_id: int = 0
    timestamp: datetime = None
    redirect_title: Union[str, None] = None
    revision_id: str = ""

    @classmethod
    def get_avro_schema(cls, ignored_fields=None) -> dict:
        schema = {
            "type": "record",
            "name": "Page",
            "fields": [
                {"name": "page_id", "type": "int"},
                {"name": "title", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "redirect_title", "type": ["string", "null"]},
                {"name": "revision_id", "type": "string"},
                {"name": "text", "type": "string"},
            ],
        }
        if ignored_fields is not None:
            schema["fields"] = [
                field
                for field in schema["fields"]
                if field["name"] not in ignored_fields
            ]
        return schema

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_xml(cls, elem: etree.Element, namespace: str) -> "Page":
        redirect_elem = elem.find(f"./{{{namespace}}}redirect")
        redirect_title = (
            redirect_elem.get("title") if redirect_elem is not None else None
        )
        timestamp = elem.find(f".//{{{namespace}}}timestamp").text
        revision = elem.find(f".//{{{namespace}}}revision")
        if revision is not None:
            revision_id = revision.find(f"./{{{namespace}}}id")
            if revision_id is not None:
                revision_id = revision_id.text
        return cls(
            page_id=int(elem.find(f"./{{{namespace}}}id").text),
            title=elem.find(f"./{{{namespace}}}title").text,
            timestamp=datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ"),
            redirect_title=redirect_title,
            revision_id=revision_id,
            text=elem.find(f".//{{{namespace}}}text").text,
        )

    def get_wikipedia_url(self) -> str:
        return f"https://en.wikipedia.org/wiki/{self.title}"


class ExtractorBase(ABC):
    def iter_page_batches(
        self, batch_size: int, limit: Optional[int] = None
    ) -> Iterator[List[Page]]:
        """Iterate over pages in batches.

        Each return is a list of Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text.

        This method iterates over the pages in the dump file and yields batches of
        pages. If a limit is provided, the iteration will stop after the specified
        number of batches have been returned.

        Parameters
        ----------
        batch_size : int
            The number of pages per batch.
        limit : int | None, optional
            The maximum number of batches to return.

        Returns
        -------
        Iterator[list[Page]]
            An iterator over lists of pages.
        """
        batch = []
        batches_returned = 0
        for page in self.iter_pages():
            batch.append(page)
            if len(batch) >= batch_size:
                yield batch
                batches_returned += 1
                batch = []
            if limit is not None and batches_returned >= limit:
                break
        if batch:
            yield batch

    @abstractmethod
    def iter_pages(self) -> Iterator[Page]:
        """Iterate over all pages in the dump file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text.
        """


class WikiXmlDumpExtractor(ExtractorBase):
    """A class for extracting pages from a MediaWiki XML dump file.
    This class provides functionality to parse and extract pages from MediaWiki XML
    dump files, which can be either uncompressed (.xml) or bzip2 compressed
    (.xml.bz2). It handles the XML namespace detection automatically and provides
    iterators for processing pages individuallyor in batches.

    Parameters
    ----------
    file_path : str
        Path to the MediaWiki XML dump file (.xml or .xml.bz2)

    Examples
    --------
    >>> extractor = WikiDumpExtractor("dump.xml.bz2")
    >>> for page in extractor.iter_pages():
    ...     print(page.title)

    >>> # Process pages in batches
    >>> for batch in extractor.iter_page_batches(batch_size=100):
    ...     process_batch(batch)
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.namespace = self._detect_namespace()

    def _get_xml_handle(self):
        """Return a handle to the XML file (handle both .xml and .xml.bz2)"""
        if self.file_path.endswith(".xml"):
            return open(self.file_path, "rb")
        elif self.file_path.endswith(".xml.bz2"):
            return bz2.open(self.file_path, "rb")
        else:
            raise ValueError(f"Unsupported file type: {self.file_path}")

    def _detect_namespace(self) -> str:
        """Detect the namespace of the XML file
        This will be e.g. "http://www.mediawiki.org/xml/export-0.11/"
        """
        with self._get_xml_handle() as file:
            first_element = next(etree.iterparse(file, events=("end",)))[1]
            return first_element.tag[1 : first_element.tag.find("}")]

    def _iter_xml_page_elements(self) -> Iterator[etree.Element]:
        """Iterate over all XML elements tagged as pages in the dump file"""
        tag = f"{{{self.namespace}}}page"
        with self._get_xml_handle() as f:
            for _, elem in etree.iterparse(f, events=("end",), tag=tag, recover=True):
                yield elem
                self._clean_up_xml_page_element(elem)

    def _clean_up_xml_page_element(self, page_xml: etree.Element):
        """Clean up the XML element. This is critical to avoid memory leaks."""
        page_xml.clear()
        while page_xml.getprevious() is not None:
            del page_xml.getparent()[0]

    def iter_pages(self) -> Iterator[Page]:
        """Iterate over all pages in the dump file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text.
        """
        for page_xml in self._iter_xml_page_elements():
            yield Page.from_xml(page_xml, self.namespace)

    def extract_pages_to_new_xml(
        self, output_file: Union[str, Path], limit: Union[int, None] = 50
    ):
        """Create a smaller XML dump file by extracting a limited number of pages.

        This is useful for debugging, testing, creating examples, etc.

        Parameters
        ----------
        output_file : str
            Path where to save the output XML file. Can be a .xml or .xml.bz2 file.
        page_limit : int, optional
            Maximum number of pages to extract, by default 70
        """
        output_file = Path(output_file)
        new_root = etree.Element("mediawiki", nsmap={None: self.namespace})
        new_root.set("version", "0.11")
        for i, elem in enumerate(self._iter_xml_page_elements()):
            if i >= limit:
                break
            new_root.append(deepcopy(elem))

        tree = etree.ElementTree(new_root)
        if output_file.suffix.endswith(".bz2"):
            with bz2.open(output_file, "wb") as f:
                tree.write(f, pretty_print=True, encoding="utf-8", xml_declaration=True)
        else:
            tree.write(
                output_file, pretty_print=True, encoding="utf-8", xml_declaration=True
            )

    def extract_pages_to_avro(
        self,
        output_file: Union[str, Path],
        ignore_redirects: bool = True,
        ignored_fields: List[str] = ["text"],
        batch_size: int = 10_000,
        limit: int = None,
        codec: str = "zstandard",
    ):
        """Convert the XML dump file to an Avro file.

        Parameters
        ----------
        output_file : str
            Path where to save the output Avro file.
        ignore_redirects : bool, optional
            Whether to ignore redirects, by default True
        ignored_fields : List[str], optional
            Fields to ignore, by default ["text"]
        batch_size : int, optional
            Number of pages per batch, by default 10_000
        limit : int, optional
            Maximum number of pages to extract, by default None
        codec : str, optional
            Codec to use for compression, by default "zstandard"
        """
        target_path = Path(output_file)
        if target_path.exists():
            target_path.unlink()

        schema = Page.get_avro_schema(ignored_fields=ignored_fields)
        with target_path.open("a+b") as f:
            for batch in self.iter_page_batches(batch_size=batch_size, limit=limit):
                records = [
                    page.to_dict()
                    for page in batch
                    if (not ignore_redirects) or (page.redirect_title is None)
                ]
                fastavro.writer(f, schema, records, codec=codec)


class WikiAvroDumpExtractor(ExtractorBase):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def iter_pages(self) -> Iterator[Page]:
        """Iterate over all pages in the Avro file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text, depending on what
        was saved in the Avro file.
        """
        with open(self.file_path, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                yield Page(**record)
