from wiki_dump_extractor import WikiDumpExtractor

extractor = WikiDumpExtractor("tiny_dump.xml.bz2")

for page in extractor.iter_pages():
    print(page.title)
