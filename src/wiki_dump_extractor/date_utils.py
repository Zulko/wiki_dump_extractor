import re
from datetime import datetime
from typing import List, Dict, ClassVar, Pattern
from abc import ABC, abstractmethod
import warnings

# Define month name to number mapping
_MONTH_MAP = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

# Dictionary to convert written numbers to integers
_WRITTEN_NUMBERS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
    "eleventh": 11,
    "twelfth": 12,
    "thirteenth": 13,
    "fourteenth": 14,
    "fifteenth": 15,
    "sixteenth": 16,
    "seventeenth": 17,
    "eighteenth": 18,
    "nineteenth": 19,
    "twentieth": 20,
    "twenty-first": 21,
    "twenty-second": 22,
    "twenty-third": 23,
    "twenty-fourth": 24,
    "twenty-fifth": 25,
    "twenty-sixth": 26,
    "twenty-seventh": 27,
    "twenty-eighth": 28,
    "twenty-ninth": 29,
    "thirtieth": 30,
    "thirty-first": 31,
}

# Common month pattern for reuse
_MONTHS_PATTERN = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
)


class DateFormat(ABC):
    """Base class for all date format detectors."""

    name: ClassVar[str]
    pattern: ClassVar[Pattern]

    @classmethod
    @abstractmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        """Convert a regex match to a datetime object.

        Parameters
        ----------
        match : re.Match
            The regex match object containing the date information

        Returns
        -------
        datetime
            The parsed datetime object

        Raises
        ------
        ValueError
            If the match cannot be converted to a valid datetime
        """
        pass

    @classmethod
    def convert_month_to_number(cls, month_name: str) -> int:
        """Convert month name to its numerical representation.

        Raises
        ------
        ValueError
            If the month name is not recognized
        """
        month = _MONTH_MAP.get(month_name.lower())
        if month is None:
            raise ValueError(f"Unknown month name: {month_name}")
        return month

    @classmethod
    def list_dates(cls, text: str) -> bool:
        """Check if the text contains any dates detected by regex patterns."""
        results = []
        errors = []
        for match in cls.pattern.finditer(text):
            try:
                dt_obj = cls.match_to_datetime(match)
                if dt_obj is not None:
                    results.append(
                        {
                            "date_str": match.group(0),
                            "format": cls.name,
                            "datetime": dt_obj,
                        }
                    )
            except ValueError as err:
                errors.append(f"Error parsing date: {match.group(0)} - {err}")
        return results, errors


class SlashDMYMDYFormat(DateFormat):
    """Format for DD/MM/YYYY or MM/DD/YYYY dates."""

    name = "SLASH_DMY_MDY"
    pattern = re.compile(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b(?![-/])", re.IGNORECASE)

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        date_str = match.group(0)
        parts = re.split(r"[-/]", date_str)
        if len(parts) != 3:
            raise ValueError(f"Invalid slash date format: {date_str}")

        # Try MM/DD/YYYY first (American format)        print(int(parts[2]), int(parts[0]), int(parts[1]))
        try:
            return datetime(int(parts[2]), int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            # Try DD/MM/YYYY (European format)
            try:
                return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
            except (ValueError, IndexError):
                return None


class DashYMDFormat(DateFormat):
    """Format for YYYY-MM-DD dates."""

    name = "DASH_YMD"
    pattern = re.compile(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", re.IGNORECASE)

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        date_str = match.group(0)
        parts = re.split(r"[-/]", date_str)
        if len(parts) != 3:
            raise ValueError(f"Invalid dash date format: {date_str}")

        try:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
        except (ValueError, IndexError):
            return None


class DayMonthYearFormat(DateFormat):
    """Format for DD Month YYYY dates."""

    name = "DAY_MONTH_YEAR"
    pattern = re.compile(
        rf"\b(\d{{1,2}})\s+({_MONTHS_PATTERN})[,\s]+(\d{{2,4}})\b", re.IGNORECASE
    )

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        day, month_str, year = match.groups()
        month = cls.convert_month_to_number(month_str)
        return datetime(int(year), month, int(day))


class MonthDayYearFormat(DateFormat):
    """Format for Month DD YYYY dates."""

    name = "MONTH_DAY_YEAR"
    pattern = re.compile(
        rf"\b({_MONTHS_PATTERN})\s+(\d{{1,2}})(?:st|nd|rd|th)?[,\s]+(\d{{2,4}})\b",
        re.IGNORECASE,
    )

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        month_str, day, year = match.groups()
        month = cls.convert_month_to_number(month_str)
        return datetime(int(year), month, int(day))


class MonthYearFormat(DateFormat):
    """Format for Month YYYY dates."""

    name = "MONTH_YEAR"
    pattern = re.compile(rf"\b({_MONTHS_PATTERN})\s+(\d{{2, 4}})\b", re.IGNORECASE)

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        month_str, year = match.groups()
        month = cls.convert_month_to_number(month_str)
        return datetime(int(year), month, 1)  # Default to 1st day of month


class WrittenDateFormat(DateFormat):
    """Format for Month the day, year dates."""

    name = "WRITTEN_DATE"
    pattern = re.compile(
        rf"\b({_MONTHS_PATTERN})\s+the\s+(?:(\d{{1,2}})(?:st|nd|rd|th)?|([a-z]+))[,\s]+(\d{{2,4}})\b",
        re.IGNORECASE,
    )

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        groups = match.groups()
        month_str = groups[0]

        # Check if the day is a number or written out
        if groups[1] is not None:  # Numeric day like "the 15th"
            day = int(groups[1])
        else:  # Written day like "the third"
            written_day = groups[2].lower()
            if written_day in _WRITTEN_NUMBERS:
                day = _WRITTEN_NUMBERS[written_day]
            else:
                raise ValueError(f"Unsupported written day number: {match.group(0)}")

        year = int(groups[3])
        month = cls.convert_month_to_number(month_str)

        return datetime(year, month, day)


class WikiDateFormat(DateFormat):
    """Format for {{Birth date|YYYY|MM|DD}}."""

    name = "WIKI_BIRTH_DATE"
    pattern = re.compile(r"{{.*\|(\d{4})\|(\d{1,2})\|(\d{1,2})", re.IGNORECASE)

    @classmethod
    def match_to_datetime(cls, match: re.Match) -> datetime:
        year, month, day = match.groups()
        # Check if the template is a birth date template
        return datetime(int(year), int(month), int(day))


# Register all date format handlers
_DATE_FORMATS = [
    SlashDMYMDYFormat,
    DashYMDFormat,
    DayMonthYearFormat,
    MonthDayYearFormat,
    MonthYearFormat,
    WrittenDateFormat,
    WikiDateFormat,
]


def extract_dates(text: str) -> List[Dict]:
    """Extract dates from text with context information.

    Parameters
    ----------
    text : str
        The text to extract dates from.

    Returns
    -------
    List[Dict]
        A list of dictionaries containing:
        - 'date_str': The original date string found
        - 'format': The name of the date format
        - 'datetime': The parsed datetime object (if parsing was successful)
    """
    all_results = []
    all_errors = []
    for date_format in _DATE_FORMATS:
        results, errors = date_format.list_dates(text)
        all_results.extend(results)
        all_errors.extend(errors)
    return all_results, all_errors
