import re
import gzip
import pandas
from tqdm.auto import tqdm
from pathlib import Path


class WikiSqlExtractor:
    """
    Extracts data from a Wikipedia SQL dump.

    Parameters
    ----------
    file_path : str or Path
        The path to the Wikipedia SQL dump file.

    Examples
    --------

    >>> extractor = WikiSqlExtractor("enwiki-20240301-pages-articles-multistream.xml.bz2")
    >>> df = extractor.to_pandas_dataframe(columns=[...])
    """

    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.columns = self._get_column_names()

    def _get_file_handle(self):
        if str(self.file_path).endswith(".gz"):
            return gzip.open(self.file_path, mode="rt", encoding="utf-8")
        else:
            return open(self.file_path, mode="rt", encoding="utf-8")

    def _get_column_names(self):
        with self._get_file_handle() as f:
            for line in f:
                line = line.strip()
                if "CREATE TABLE" in line:
                    # Read until we find the end of CREATE TABLE statement
                    block = ""
                    line = next(f).strip()
                    while not line.startswith(")"):
                        block += f"{line}\n"
                        line = next(f).strip()
                    return [
                        match.group(1)
                        for line in block.split("\n")
                        if line.startswith("`")
                        for match in re.finditer("`(\w+)`\s+[^,\n]+", line)
                    ]

    def _iter_through_data_rows(self, max_rows=None):
        insert_re = re.compile(r"INSERT INTO `?\w+`? VALUES (.*);", re.IGNORECASE)
        count = 0
        skipped = 0
        with tqdm(total=max_rows, desc="Processing rows") as pbar:
            for line in self._get_file_handle():
                line = line.strip()
                if line.startswith("INSERT INTO"):
                    match = insert_re.search(line)
                    if match:
                        values_str = match.group(1)
                        # The values are in the format: (val1,val2,val3),(val1,val2,val3),...
                        # Remove the starting and ending parentheses if needed.
                        if values_str.startswith("(") and values_str.endswith(")"):
                            values_str = values_str[1:-1]
                        # Now split into individual records.
                        records = values_str.split("),(")

                        for record in records:
                            try:
                                yield eval(record.replace("NULL", "None"))
                                count += 1
                                pbar.update(1)
                                if max_rows is not None and count >= max_rows:
                                    return
                            except Exception as _e:
                                skipped += 1
        print("Skipped lines:", skipped)

    def to_pandas_dataframe(self, columns=None, max_rows=None, row_filter=None):
        """Reads the data from the database and returns a pandas DataFrame.

        This is optimized for memory consumption.

        Parameters
        ----------
        row_filter : callable, optional
            A function that takes a record and returns True if the record
            should be included in the DataFrame.
        columns : list, optional
            A list of columns to include in the DataFrame.
        max_rows : int, optional
            The maximum number of rows to read from the database.
        """
        rows = []
        for row in self._iter_through_data_rows(max_rows=max_rows):
            record = dict(zip(self.columns, row))
            if row_filter is not None and not row_filter(record):
                continue
            if columns is not None:
                row = [record[column] for column in columns]
            rows.append(row)
        return pandas.DataFrame(rows, columns=columns or self.columns)
