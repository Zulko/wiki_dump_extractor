# Wiki dump extractor

A python library to extract and analyze pages from a wiki dump.

## Scope

- Extract pages from a wiki dump
- Be easy to install and run
- Be fast (currently ~2000 pages per second on uncompressed archive, 250/s on .bz2 archive)
- Be memory efficient
- Allow for batch processing
- Offer a few light utilities for page analysis (e.g. extracting tags and category)

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

## Installation


```bash
pip install wiki-dump-extractor
```

Or from the source in development mode:

```bash
pip install -e .
```

To run the tests just use `pytest` in the root directory.
