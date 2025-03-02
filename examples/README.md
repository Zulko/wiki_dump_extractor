# Examples



## About the tiny dump


```python
dump_path = "enwiki-20250201-pages-articles-multistream.xml"
extractor = WikiDumpExtractor(dump_path)
extractor.extract_pages_to_new_xml("tiny_dump.xml.bz2", limit=70)
```