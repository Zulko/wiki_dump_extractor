from wiki_dump_extractor import WikiDumpExtractor, page_utils
from multiprocessing import Pool
from pathlib import Path
from tqdm import tqdm
import pandas


def extract_infos(page_text: str) -> dict:
    infos = {
        "dates": page_utils.extract_dates(page_text),  # slow!
        "has_date": page_utils.has_date(page_text),
        "categories": page_utils.extract_categories(page_text),
        "infobox_category": page_utils.extract_infobox_category(page_text),
    }
    coordinates = page_utils.extract_geospatial_coordinates(page_text)
    if coordinates:
        infos["longitude"], infos["latitude"] = coordinates
    return infos


def save_batch_to_parquet(batch_and_path):
    batch, target_path = batch_and_path
    records = [{**page.to_dict(), **extract_infos(page.text)} for page in batch]
    df = pandas.DataFrame(records).set_index("page_id")
    df.to_parquet(target_path)
    return df


def file_path_iterator(output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for i in range(1000_000):
        yield output_dir / f"batch_{i:06d}.parquet"


def extract_pages_to_parquet_sequential(
    dump_file, output_dir, batch_size=1000, max_batches=None
):
    extractor = WikiDumpExtractor(file_path=dump_file)
    batches = extractor.iter_page_batches(batch_size=batch_size, limit=max_batches)
    for batch_and_filepath in zip(batches, file_path_iterator(output_dir)):
        save_batch_to_parquet(batch_and_filepath)


def extract_pages_to_parquet_parallel(
    dump_file, output_dir, batch_size=1000, n_workers=7, max_batches=None
):
    extractor = WikiDumpExtractor(file_path=dump_file)
    iterator = extractor.iter_page_batches(batch_size=batch_size, limit=max_batches)
    batches_and_paths = zip(iterator, file_path_iterator(output_dir))
    with Pool(processes=n_workers) as pool:
        pooled_jobs = pool.imap_unordered(save_batch_to_parquet, batches_and_paths)
        for batch_result in pooled_jobs:
            # here we could also be yielding the results of the batch
            pass


extract_pages_to_parquet_parallel(
    dump_file="tiny_dump.xml.bz2",
    output_dir="info_parquets/",
    batch_size=10,
    max_batches=5,
)
