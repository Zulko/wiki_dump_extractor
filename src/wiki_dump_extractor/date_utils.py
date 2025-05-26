import re
from datetime import datetime
from typing import List, Dict, ClassVar, Pattern, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict


@dataclass(slots=True)
class Date:
    year: int
    month: Optional[int] = None
    day: Optional[int] = None
    is_approximate: bool = False

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.month is None:
            return True
        if not 1 <= self.month <= 12:
            raise ValueError(f"Month must be between 1 and 12, got {self.month}")

        if self.day is None:
            return True

        # Validate day based on month and year
        max_days = 31  # Default for most months

        if self.month in [4, 6, 9, 11]:  # April, June, September, November
            max_days = 30
        elif self.month == 2:  # February
            # Check for leap year
            if (self.year % 4 == 0 and self.year % 100 != 0) or (self.year % 400 == 0):
                max_days = 29
            else:
                max_days = 28

        if not 1 <= self.day <= max_days:
            raise ValueError(
                f"Day must be between 1 and {max_days} for month {self.month}, got {self.day}"
            )

    def to_string(self) -> str:
        if self.year < 0:
            result = f"{-self.year:04d} BC"
        else:
            result = f"{self.year:04d}"
        if self.month is not None:
            result += f"/{self.month:02d}"
        if self.day is not None:
            result += f"/{self.day:02d}"
        return result

    def to_dict(self) -> Dict:
        return asdict(self)


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


@dataclass(slots=True)
class DetectedDate:
    date: Date
    format: str
    date_str: str

    def to_dict(self) -> Dict:
        return {
            "date": self.date.to_dict(),
            "format": self.format,
            "date_str": self.date_str,
        }


class DateFormat(ABC):
    """Base class for all date format detectors."""

    name: ClassVar[str]
    pattern: ClassVar[Pattern]

    @classmethod
    @abstractmethod
    def match_to_date(cls, match: re.Match) -> datetime:
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
                date = cls.match_to_date(match)
                if date is not None:
                    detected_date = DetectedDate(
                        date_str=match.group(0), format=cls.name, date=date
                    )
                    results.append(detected_date)

            except ValueError as err:
                errors.append(
                    f"Error parsing {cls.name} date: {match.group(0)} - {err}"
                )
        return results, errors


class SlashDMYMDYFormat(DateFormat):
    """Format for DD/MM/YYYY or MM/DD/YYYY dates."""

    name = "SLASH_DMY_MDY"
    pattern = re.compile(
        r"\B[^|](\d{1,2})[-/](\d{1,2})[-/](\d{1,4})(?:\s+(BC|BCE))?\b", re.IGNORECASE
    )

    @classmethod
    def match_to_date(cls, match: re.Match) -> Date:
        day, month, year, bc = match.groups()
        day, month, year = int(day), int(month), int(year)
        if bc:
            year = -year

        # Try MM/DD/YYYY first (American format)
        try:
            return Date(year, month, day)
        except (ValueError, IndexError):
            # Try DD/MM/YYYY (European format)
            try:
                return Date(year, day, month)
            except (ValueError, IndexError):
                return None


class DashYMDFormat(DateFormat):
    """Format for YYYY-MM-DD dates."""

    name = "DASH_YMD"
    pattern = re.compile(
        r"\b(\d{1,4})[-/](\d{1,2})[-/](\d{1,2})(?:\s+(BC|BCE))?\b", re.IGNORECASE
    )

    @classmethod
    def match_to_date(cls, match: re.Match) -> Date:
        groups = match.groups()
        year, month, day, bc = groups
        if bc:
            year = -int(year)
        return Date(int(year), int(month), int(day))


class DayMonthYearFormat(DateFormat):
    """Format for DD Month YYYY dates."""

    name = "DAY_MONTH_YEAR"
    re_dmy = rf"""\b
        (\d{{1,2}})                          # Day (1-2 digits)
        \s+
        ({_MONTHS_PATTERN})               # Month (provided externally)
        [,\s]+
        (?:AD\s*)?
        (\d{{1,4}})                         # Year (1-4 digits)
        (?:\s+(BC|BCE))?                    # Optional ' BC'
        \b
    """
    pattern = re.compile(re_dmy, re.VERBOSE | re.IGNORECASE)

    @classmethod
    def match_to_date(cls, match: re.Match) -> Date:
        day, month_str, year, bc = match.groups()
        month = cls.convert_month_to_number(month_str)
        if bc:
            year = -int(year)
        return Date(int(year), month, int(day))


class MonthDayYearFormat(DateFormat):
    """Format for Month DD YYYY dates."""

    name = "MONTH_DAY_YEAR"
    re_mdy = rf"""
        \b
        ({_MONTHS_PATTERN})        # Month name
        \s+
        (\d{{1,2}})                # Day (1 or 2 digits)
        (?:st|nd|rd|th)?           # Optional ordinal suffix
        [,\s]+
        (?:AD\s*)?
        (\d{{1,4}})                # Year (1 to 4 digits)
        (?:\s+(BC|BCE))?           # Optional ' BC'
        \b
    """
    pattern = re.compile(re_mdy, re.VERBOSE | re.IGNORECASE)

    @classmethod
    def match_to_date(cls, match: re.Match) -> Date:
        month_str, day, year, bc = match.groups()
        month = cls.convert_month_to_number(month_str)
        if bc:
            year = -int(year)
        return Date(int(year), month, int(day))


class MonthYearFormat(DateFormat):
    """Format for Month YYYY dates."""

    name = "MONTH_YEAR"
    pattern = re.compile(
        rf"\b({_MONTHS_PATTERN})\s*(?:AD\s*)?(\d{{2,4}})(?:\s+(BC|BCE))?\b",
        re.IGNORECASE,
    )

    @classmethod
    def match_to_date(cls, match: re.Match) -> Date:
        # Check if there's a digit before the month
        start_pos = match.start()

        # If this is preceded by a digit and space, it's likely a "7 December 2012" format
        # which should be handled by DayMonthYearFormat instead
        if start_pos >= 2 and match.string[start_pos - 2 : start_pos].strip().isdigit():
            return None

        month_str, year, bc = match.groups()
        month = cls.convert_month_to_number(month_str)
        if bc:
            year = -int(year)
        return Date(year=int(year), month=month, day=None)


class YearFormat(DateFormat):
    """Format for YYYY dates."""

    name = "YEAR"
    pattern = re.compile(
        r"\b(?:c\.|in|from|to)\s*(?:AD\s*)?(\d{1,4})(?:\s*(BC|BCE))?[\s,\.,\)]",
        re.IGNORECASE,
    )

    @classmethod
    def match_to_date(cls, match: re.Match) -> Date:
        year, bc = match.groups()
        year = int(year)
        if bc:
            year = -year
        return Date(year=year, month=None, day=None)


class WrittenDateFormat(DateFormat):
    """Format for Month the day, year dates."""

    name = "WRITTEN_DATE"
    pattern = re.compile(
        rf"\b({_MONTHS_PATTERN})\s+the\s+(?:(\d{{1,2}})(?:st|nd|rd|th)?|([a-z]+))[,\s]+(\d{{1,4}})+(?:\s+(BC|BCE))?\b",
        re.IGNORECASE,
    )

    @classmethod
    def match_to_date(cls, match: re.Match) -> datetime:
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
        if groups[4]:
            year = -year
        month = cls.convert_month_to_number(month_str)

        return Date(year, month, day)


class WikiDateFormat(DateFormat):
    """Format for {{Birth date|YYYY|MM|DD|...}}."""

    name = "WIKI_BIRTH_DATE"
    pattern = re.compile(r"{{[^|]*\|(\d{1,4})\|(\d{1,2})\|(\d{1,2}).*}}", re.IGNORECASE)

    @classmethod
    def match_to_date(cls, match: re.Match) -> datetime:
        year, month, day = match.groups()
        # Check if the template is a birth date template
        return Date(int(year), int(month), int(day))


# Register all date format handlers
_DATE_FORMATS = [
    SlashDMYMDYFormat,
    DashYMDFormat,
    DayMonthYearFormat,
    MonthDayYearFormat,
    MonthYearFormat,
    WrittenDateFormat,
    WikiDateFormat,
    YearFormat,
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


@dataclass
class DateRange:
    start: Date
    end: Date

    def to_string(self) -> str:
        start = self.start.to_string()
        end = self.end.to_string()
        if self.start.is_approximate:
            start = f"~{start}"
        if self.end.is_approximate:
            end = f"~{end}"
        return f"{start} - {end}"

    @classmethod
    def from_parsed_string(cls, date: str) -> "DateRange":
        """
        Parse a string representation of a date or date range into a DateRange object.

        Examples:
        1810 -> ~1810/01/01 - ~1810/12/31
        1810-1812 -> ~1810/01/01 - ~1812/12/31
        1810/1812 -> ~1810/01/01 - ~1812/12/31
        1810/03/05 -> 1810/03/05 - 1810/03/05
        1810/03 -> ~1810/03/01 - ~1810/03/31
        1810/03 - 1812/05 -> ~1810/03/01 - ~1812/05/31
        1810/03/05 - 1812/05/07 -> 1810/03/05 - 1812/05/07
        1611/1612 - 1615/1617 -> ~1611/01/01 - ~1617/12/31
        1930s - 1940s -> ~1930/01/01 - ~1949/12/31
        1930s -> ~1930/01/01 - ~1939/12/31
        """
        date = date.strip()

        if "-" in date:
            start_str, end_str = date.split("-")
            start_range = cls.from_parsed_string(start_str)
            end_range = cls.from_parsed_string(end_str)
            return DateRange(start=start_range.start, end=end_range.end)

        # Replace YYYY BC with negative year
        date = re.sub(r"\b(\d{1,4})\s*BC\b", r"-\1", date)

        match date:
            case _ if match := re.match(r"^-?\d{1,4}$", date):
                # Single year (e.g., "1810")
                year = int(match.group(0))
                return DateRange(
                    start=Date(year, 1, 1, is_approximate=True),
                    end=Date(year, 12, 31, is_approximate=True),
                )
            case _ if match := re.match(r"^(\d{1,3}0)s$", date):
                # Decade (e.g., "1930s")
                decade_start = int(match.group(1))
                return DateRange(
                    start=Date(decade_start, 1, 1, is_approximate=True),
                    end=Date(decade_start + 9, 12, 31, is_approximate=True),
                )
            case _ if match := re.match(r"^(-?\d{1,4})/(-?\d{1,4})$", date):
                # Year range (e.g., "1810/1812")

                # Only treat as year/year if the second number is > 12 (not a month)
                if not 1 <= int(match.group(2)) <= 12:
                    # Year range (e.g., "1810/1812")
                    start_year, end_year = map(int, match.groups())
                    return DateRange(
                        start=Date(start_year, 1, 1, is_approximate=True),
                        end=Date(end_year, 12, 31, is_approximate=True),
                    )
                else:
                    year, month = map(int, match.groups())
                    # Get last day of month
                    if month == 12:
                        last_day = 31
                    elif month in [4, 6, 9, 11]:
                        last_day = 30
                    elif month == 2:
                        # Simple leap year calculation
                        last_day = (
                            29
                            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                            else 28
                        )
                    else:
                        last_day = 31
                    return DateRange(
                        start=Date(year, month, 1, is_approximate=True),
                        end=Date(year, month, last_day, is_approximate=True),
                    )
            case _ if match := re.match(r"^(-?\d{1,4})/(\d{1,2})/(\d{1,2})$", date):
                # Full date (e.g., "1810/03/05")
                year, month, day = map(int, match.groups())
                return DateRange(
                    start=Date(year, month, day, is_approximate=False),
                    end=Date(year, month, day, is_approximate=False),
                )
            case _:
                raise ValueError(f"Unsupported date format: {date}")

    def to_dict(self) -> Dict:
        return {
            "start": self.start.to_dict(),
            "end": self.end.to_dict(),
        }
