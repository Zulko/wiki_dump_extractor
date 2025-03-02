from typing import Iterator, Union, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from copy import deepcopy
import bz2

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

    page_id: int
    title: str
    timestamp: datetime
    redirect_title: Union[str, None]
    revision_id: str
    text: str

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


class WikiDumpExtractor:
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

    def _iter_page_elements(self) -> Iterator[etree.Element]:
        """Iterate over all XML elements tagged as pages in the dump file"""
        tag = f"{{{self.namespace}}}page"
        with self._get_xml_handle() as f:
            for _, elem in etree.iterparse(f, events=("end",), tag=tag, recover=True):
                yield elem
                self._clean_up(elem)

    def _clean_up(self, page_xml: etree.Element):
        """Clean up the XML element. This is critical to avoid memory leaks."""
        page_xml.clear()
        while page_xml.getprevious() is not None:
            del page_xml.getparent()[0]

    def iter_pages(self, limit: Optional[int] = None) -> Iterator[Page]:
        """Iterate over all pages in the dump file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text.
        """
        for page_xml in self._iter_page_elements():
            yield Page.from_xml(page_xml, self.namespace)
            if limit is not None and len(self._iter_page_elements()) >= limit:
                break

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
        for page in self.iter_pages(limit):
            batch.append(page)
            if len(batch) >= batch_size:
                yield batch
                batches_returned += 1
                batch = []
            if limit is not None and batches_returned >= limit:
                break
        if batch:
            yield batch

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
        for i, elem in enumerate(self._iter_page_elements()):
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
