# Wiki dump extractor

A python library to extract and analyze pages from a wiki dump.

## Scope

- Extract pages from a wiki dump
- Be easy to install and run
- Be fast (currently 2-10k pages/s on uncompressed archive, 250/s on .bz2 archive, 50k/s on Avro)
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

### Converting the dump to Avro

There are many reasons why you might want to convert the dump to Avro. The original `xml.bz2` dump is 22Gb but very slow to read from (250/s), the uncompressed dump is 107Gb, relatively fast to read (this library uses lxml which reads thousands of pages per second), however 50% of the pages in there are empty redirect pages.

The following code converts the batch to a 28G avro dump that only contains the 12 million real pages, and will be much faster to read. The operation takes ~40 minutes depending on your machine.

```python
from wiki_dump_extractor import WikiXmlDumpExtractor

file_path = "enwiki-20250201-pages-articles-multistream.xml"
extractor = WikiXmlDumpExtractor(file_path=file_path)
ignored_fields = ["timestamp", "page_id", "revision_id", "redirect_title"]
extractor.extract_pages_to_avro(
    output_file="wiki_dump.avro",
    ignore_redirects=True,
    ignored_fields=ignored_fields,
)
```

Later on, read the Avro file as follows (reads the 12 million pages in ~3-4 minutes depending on your machine)

```python
from wiki_dump_extractor import WikiAvroDumpExtractor

extractor = WikiAvroDumpExtractor(file_path="wiki_dump.avro")
for page in extractor.iter_pages(limit=1000):
    pass
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

### Requirements for running the LLM utils

```bash
# Add the Cloud SDK distribution URI as a package source
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update the package list and install the Cloud SDK
sudo apt-get update && sudo apt-get install google-cloud-sdk
```
