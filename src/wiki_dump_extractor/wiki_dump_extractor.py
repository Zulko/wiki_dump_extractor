from abc import ABC, abstractmethod
from typing import (
    Iterator,
    Union,
    Optional,
    List,
    Callable,
    Any,
    Generator,
    AsyncGenerator,
    Tuple,
)
from dataclasses import dataclass, asdict
from datetime import datetime
import multiprocessing
import itertools
from pathlib import Path
from copy import deepcopy
import bz2
import fastavro
from lxml import etree
from tqdm.auto import tqdm
import shutil
import plyvel
import aiostream


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

    _fields = ["page_id", "title", "timestamp", "redirect_title", "revision_id", "text"]

    @classmethod
    def get_avro_schema(cls, ignored_fields=None, fields=None) -> dict:
        schema = {
            "type": "record",
            "name": "Page",
            "fields": [
                {"name": "page_id", "type": ["int", "null"], "default": None},
                {"name": "title", "type": ["string", "null"], "default": None},
                {"name": "timestamp", "type": ["string", "null"], "default": None},
                {"name": "redirect_title", "type": ["string", "null"], "default": None},
                {"name": "revision_id", "type": ["string", "null"], "default": None},
                {"name": "text", "type": ["string", "null"], "default": None},
            ],
        }
        schema["fields"] = [
            field
            for field in schema["fields"]
            if ignored_fields is None
            or (field["name"] not in ignored_fields)
            and (fields is None or field["name"] in fields)
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
    @abstractmethod
    def _iter_pages(self) -> Iterator[Page]:
        """Iterate over all pages in the dump file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text.
        """
        raise NotImplementedError

    def iter_pages(
        self,
        page_limit: Optional[int] = None,
        page_filter: Optional[Callable[[Page], bool]] = None,
    ) -> Iterator[Page]:
        """Iterate over all pages in the dump file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text.
        """
        page_count = 0
        for page in self._iter_pages():
            if page_filter is None or page_filter(page):
                yield page
                page_count += 1
                if page_limit is not None and page_count >= page_limit:
                    break

    def iter_page_batches(
        self,
        batch_size: int,
        page_limit: Optional[int] = None,
        page_filter: Optional[Callable[[Page], bool]] = None,
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
        page_limit : int | None, optional
            The maximum number of pages to return.
        page_filter : Callable[[Page], bool], optional
            A function that takes a Page object and returns a boolean.
            If the function returns False, the page will not be included in the batch.

        Returns
        -------
        Iterator[list[Page]]
            An iterator over lists of pages.
        """
        batch = []
        batches_returned = 0
        for page in self.iter_pages(page_limit=page_limit, page_filter=page_filter):
            batch.append(page)
            if len(batch) >= batch_size:
                yield batch
                batches_returned += 1
                batch = []
        if batch:
            yield batch

    async def process_pages_async(
        self,
        process_fn,
        num_workers=5,
        page_limit=None,
        page_filter=None,
        ordered=False,
    ) -> AsyncGenerator[Tuple[Page, Any], None]:
        async def wrapper_fn(item):
            return item, await process_fn(item)

        page_iterator = self.iter_pages(page_limit=page_limit, page_filter=page_filter)
        async_iterator = aiostream.stream.iterate(page_iterator)
        async for result in aiostream.stream.map(
            async_iterator, wrapper_fn, task_limit=num_workers, ordered=ordered
        ):
            yield result

    def process_page_batches_in_parallel(
        self,
        process_fn: Callable[[List[Page], int], Any],
        num_workers: int,
        batch_size: int,
        page_limit: Optional[int] = None,
        page_filter: Optional[Callable[[Page], bool]] = None,
        ordered_results: bool = False,
    ):
        """Apply a function to each batch of pages in parallel.

        This method is useful for speeding up the extraction of pages from the
        dump file.

        Parameters
        ----------
        action : Callable[[List[Page], int], Any]
            A function that takes a list of Page objects and an integer and returns
            any value.
        num_workers : int
            The number of workers to use.
        batch_size : int
            The number of pages per batch.
        page_limit : int | None, optional
            The maximum number of pages to return.
        page_filter : Callable[[Page], bool], optional
            A function that takes a Page object and returns a boolean.
            If the function returns False, the page will not be included in the batch.
        """
        batches = self.iter_page_batches(
            batch_size=batch_size, page_limit=page_limit, page_filter=page_filter
        )
        batches_and_indices = zip(batches, itertools.count())
        with multiprocessing.Pool(num_workers) as pool:
            if ordered_results:
                for batch_result in pool.imap(process_fn, batches_and_indices):
                    yield batch_result
            else:
                for batch_result in pool.imap_unordered(
                    process_fn, batches_and_indices
                ):
                    yield batch_result

    def extract_pages_to_avro(
        self,
        output_file: Union[str, Path],
        redirects_file: Optional[Union[str, Path]] = None,
        ignored_fields: List[str] = None,
        fields: List[str] = None,
        batch_size: int = 10_000,
        page_limit: int = None,
        codec: str = "zstandard",
        page_filter: Optional[Callable[[Page], bool]] = None,
    ):
        """Convert the XML dump file to an Avro file.

        Parameters
        ----------
        output_file : str
            Path where to save the output Avro file.
        redirects_file : str | Path, optional
            Path where to save the redirects Avro file.
        ignored_fields : List[str], optional
            Fields to ignore, by default ["text"]
        batch_size : int, optional
            Number of pages per batch, by default 10_000
        page_limit : int, optional
            Maximum number of pages to extract, by default None
        codec : str, optional
            Codec to use for compression, by default "zstandard"
        page_filter : Callable[[Page], bool], optional
            A function that takes a Page object and returns a boolean.
            If the function returns False, the page will not be included in the Avro file.
        """
        target_path = Path(output_file)
        if target_path.exists():
            target_path.unlink()
        if redirects_file is not None:
            redirects_path = Path(redirects_file)
            if redirects_path.exists():
                redirects_path.unlink()
        schema = Page.get_avro_schema(ignored_fields=ignored_fields, fields=fields)
        redirects_schema = Page.get_avro_schema(
            ignored_fields=["text", "timestamp", "revision_id"]
        )
        with target_path.open("a+b") as f:
            batches = self.iter_page_batches(
                batch_size=batch_size, page_limit=page_limit
            )
            total = None if page_limit is None else page_limit // batch_size
            for batch in tqdm(batches, total=total):
                if redirects_file is not None:
                    redirects = [
                        p.to_dict() for p in batch if p.redirect_title is not None
                    ]
                    with redirects_path.open("a+b") as fr:
                        fastavro.writer(fr, redirects_schema, redirects, codec=codec)
                    batch = [p for p in batch if p.redirect_title is None and p.text]
                records = [
                    page.to_dict()
                    for page in batch
                    if page_filter is None or page_filter(page)
                ]
                fastavro.writer(f, schema, records, codec=codec)


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

    def _iter_pages(self):
        for i, page_xml in enumerate(self._iter_xml_page_elements()):
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


class WikiAvroDumpExtractor(ExtractorBase):
    def __init__(self, file_path: str, index_dir: Optional[Union[str, Path]] = None):
        self.file_path = file_path
        self.index_dir = index_dir

    def _iter_pages(self) -> Iterator[Page]:
        """Iterate over all pages in the Avro file.

        The returned elements are Page objects with fields title, page_id,
        timestamp, redirect_title, revision_id, and text, depending on what
        was saved in the Avro file.
        """
        with open(self.file_path, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                yield Page(**record)

    def index_pages(self, index_dir: Union[str, Path]):
        """Index the pages in the Avro file.

        Parameters
        ----------
        index_file : str
            Path where to save the index file.
        """
        index_dir = Path(index_dir)
        if index_dir.exists():
            shutil.rmtree(index_dir)
        with plyvel.DB(str(index_dir), create_if_missing=True) as db:
            with open(self.file_path, "rb") as f:
                reader = fastavro.reader(f)
                previous_idx = reader.fo.tell()
                idx_to_log = previous_idx
                for record in tqdm(reader):
                    new_idx = reader.fo.tell()
                    if new_idx != previous_idx:
                        idx_to_log = previous_idx
                    db.put(
                        record["title"].encode("utf-8"), str(idx_to_log).encode("utf-8")
                    )
                    previous_idx = new_idx
        self.index_dir = index_dir

    @classmethod
    def _get_page_using_index(
        cls, title: str, db: plyvel.DB, avro_reader: fastavro.reader
    ) -> Page:
        """Get a page by its title using the index.

        Parameters
        ----------
        title : str
            The title of the page to get.
        """
        entry = db.get(title.encode("utf-8"))
        if entry is None:
            raise ValueError(f"Page with title {title} not found in index")
        idx = int(entry)
        avro_reader.fo.seek(idx)
        page = next(avro_reader)
        while page["title"] != title:
            page = next(avro_reader)
        assert page["title"] == title
        return Page(**page)

    def get_page_by_title(self, title: str) -> Page:
        """Get a page by its title.

        Parameters
        ----------
        title : str
            The title of the page to get.
        """
        with plyvel.DB(str(self.index_dir)) as db:
            with open(self.file_path, "rb") as f:
                reader = fastavro.reader(f)
                next(reader)
                return self._get_page_using_index(title, db, reader)

    def iter_pages_by_title(self, titles: List[str]) -> Generator[Page, None, None]:
        """Get a page by its title.

        Parameters
        ----------
        title : str
            The title of the page to get.
        """
        with plyvel.DB(str(self.index_dir)) as db:
            with open(self.file_path, "rb") as f:
                reader = fastavro.reader(f)
                next(reader)
                for title in titles:
                    yield self._get_page_using_index(title, db, reader)
