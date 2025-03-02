# Examples

- [Extract pages from a wiki dump](extract_pages.py)
- [Extract pages from a wiki dump and save to a new XML file](extract_pages_to_new_xml.py)

The tiny dump file for produced using:


```python
dump_path = "enwiki-20250201-pages-articles-multistream.xml"
extractor = WikiDumpExtractor(dump_path)
extractor.extract_pages_to_new_xml("tiny_dump.xml.bz2", limit=70)
```