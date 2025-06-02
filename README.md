# Wiki dump extractor

A python library to extract and analyze pages from a wiki dump.

This library is used in particular in the [Landnotes](https://github.com/Zulko/landnotes) project to extract and analyze pages from the Wikipedia dump.

The project is hosted on [GitHub](https://github.com/zulko/wiki_dump_extractor) an the HTML documentation is available [here](https://zulko.github.io/wiki_dump_extractor/).

## Scope

Make the wikipedia dumps easier to work with:

- Extract pages from a wiki dump
- Be easy to install and run
- Be fast (can iterate over 50,000 pages / secong using Avro)
- Be memory efficient
- Allow for batch processing and parallel processing

Provide utilities for page analysis:

- Date parsing
- Section extraction
- Text cleaning
- and more.

## Usage

To simply iterate over the pages in the dump:

```python
from wiki_dump_extractor import WikiDumpExtractor

dump_file = "enwiki-20220301-pages-articles-multistream.xml.bz2"
extractor = WikiDumpExtractor(file_path=dump_file)
for page in extractor.iter_pages(limit=1000):
    print(page.title)
```

To extract the pages by batches (here we save the pages separate CSV files):

```python
from wiki_dump_extractor import WikiDumpExtractor

dump_file = "enwiki-20220301-pages-articles-multistream.xml.bz2"
extractor = WikiDumpExtractor(file_path=dump_file)
batches = extractor.iter_page_batches(batch_size=1000, limit=10)
for i, batch in enumerate(batches):
    df = pandas.DataFrame([page.to_dict() for page in batch])
    df.to_csv(f"batch_{i}.csv")
```

### Converting the dump to Avro

There are many reasons why you might want to convert the dump to Avro. The original `xml.bz2` dump is 22Gb but very slow to read from (250/s), the uncompressed dump is 107Gb, relatively fast to read (this library uses lxml which reads thousands of pages per second), however 50% of the pages in there are empty redirect pages.

The following code converts the batch to a 28G avro dump that only contains the 12 million real pages, stores redirects in a fast LMDB database, and creates an index for quick page lookups. The operation takes ~40 minutes depending on your machine.

```python
from wiki_dump_extractor import WikiXmlDumpExtractor

file_path = "enwiki-20250201-pages-articles-multistream.xml"
extractor = WikiXmlDumpExtractor(file_path=file_path)
ignored_fields = ["timestamp", "page_id", "revision_id", "redirect_title"]
extractor.extract_pages_to_avro(
    output_file="wiki_dump.avro",
    redirects_db_path="redirects.lmdb",  # LMDB database for fast redirect lookups
    ignored_fields=ignored_fields,
)
```

Then index the pages for fast lookups:

```python
from wiki_dump_extractor import WikiAvroDumpExtractor

extractor = WikiAvroDumpExtractor(file_path="wiki_dump.avro")
extractor.index_pages(page_index_db="page_index.lmdb")
```

Later on, read the Avro file and use redirects and index as follows (reads the 12 million pages in ~3-4 minutes depending on your machine):

```python
from wiki_dump_extractor import WikiAvroDumpExtractor

# Create extractor
extractor = WikiAvroDumpExtractor(
    file_path="wiki_dump.avro",
    index_dir="page_index.lmdb"  # Use the index for faster lookups
)

# Get pages with automatic redirect resolution
pages = extractor.get_page_batch_by_title(
    ["Page Title 1", "Page Title 2"]
)
```

## Installation

```bash
pip install wiki-dump-extractor
```

Or from the source in development mode:

```bash
pip install -e .
```

To use the LLM-specific module (that would be mostly if you are on a project like Landnotes), use

```bash
pip install wiki-dump-extractor[llm]
```

Or locally:
```bash
pip install -e ".[llm]"
```

To install with tests, use `pip install -e ".[dev]"` then run the tests with `pytest` in the root directory.

### Requirements for running the LLM utils

```bash
# Add the Cloud SDK distribution URI as a package source
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update the package list and install the Cloud SDK
sudo apt-get update && sudo apt-get install google-cloud-sdk
```
