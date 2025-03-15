import re
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum, auto


class DateFormat(Enum):
    """Enum representing different date formats found in text."""

    SLASH_DMY_MDY = auto()  # DD/MM/YYYY or MM/DD/YYYY
    DASH_YMD = auto()  # YYYY-MM-DD
    DAY_MONTH_YEAR = auto()  # DD Month YYYY
    MONTH_DAY_YEAR = auto()  # Month DD YYYY
    MONTH_YEAR = auto()  # Month YYYY
    WRITTEN_DATE = auto()  # Month the day, year


_months = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
)

# Define patterns with names
_date_patterns = {
    DateFormat.SLASH_DMY_MDY: r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b",  # DD/MM/YYYY or MM/DD/YYYY
    DateFormat.DASH_YMD: r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b",  # YYYY/MM/DD or YYYY-MM-DD
    DateFormat.DAY_MONTH_YEAR: rf"\b\d{{1,2}}\s+(?:{_months})[,\s]+\d{{2,4}}\b",  # DD Month YYYY
    DateFormat.MONTH_DAY_YEAR: rf"\b(?:{_months})\s+\d{{1,2}}(?:st|nd|rd|th)?[,\s]+\d{{2,4}}\b",  # Month DD YYYY
    DateFormat.MONTH_YEAR: rf"\b(?:{_months})\s+\d{{4}}\b",  # Month YYYY
    DateFormat.WRITTEN_DATE: r"\b(?:{_months})\s+the\s+(?:\d{{1,2}}(?:st|nd|rd|th)?|[a-z]+)[,\s]+\d{{2,4}}\b",  # May the third, 1988
}

# Compile each pattern separately
_compiled_patterns = {
    format_type: re.compile(pattern, re.IGNORECASE)
    for format_type, pattern in _date_patterns.items()
}


def _parse_date_to_datetime(
    date_str: str, format_type: DateFormat
) -> Optional[datetime]:
    """Convert a date string to a datetime object based on its format."""
    try:
        if format_type == DateFormat.SLASH_DMY_MDY:
            # Try both MM/DD/YYYY and DD/MM/YYYY formats
            parts = re.split(r"[-/]", date_str)
            if len(parts) == 3:
                # Try MM/DD/YYYY first (American format)
                try:
                    return datetime(int(parts[2]), int(parts[0]), int(parts[1]))
                except (ValueError, IndexError):
                    # Try DD/MM/YYYY (European format)
                    try:
                        return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                    except (ValueError, IndexError):
                        return None

        elif format_type == DateFormat.DASH_YMD:
            parts = re.split(r"[-/]", date_str)
            if len(parts) == 3:
                return datetime(int(parts[0]), int(parts[1]), int(parts[2]))

        elif format_type == DateFormat.DAY_MONTH_YEAR:
            # Extract components with regex
            match = re.match(r"\b(\d{1,2})\s+([A-Za-z]+)[,\s]+(\d{2,4})\b", date_str)
            if match:
                day, month_str, year = match.groups()
                month = _convert_month_to_number(month_str)
                if month:
                    return datetime(int(year), month, int(day))

        elif format_type == DateFormat.MONTH_DAY_YEAR:
            # Extract components with regex
            match = re.match(
                r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?[,\s]+(\d{2,4})\b", date_str
            )
            if match:
                month_str, day, year = match.groups()
                month = _convert_month_to_number(month_str)
                if month:
                    return datetime(int(year), month, int(day))

        elif format_type == DateFormat.MONTH_YEAR:
            # Extract components with regex for format like "May 2023"
            match = re.match(r"\b([A-Za-z]+)\s+(\d{2,4})\b", date_str)
            if match:
                month_str, year = match.groups()
                month = _convert_month_to_number(month_str)
                if month:
                    return datetime(int(year), month, 1)
    except (ValueError, IndexError):
        pass

    return None


def _convert_month_to_number(month_name: str) -> Optional[int]:
    """Convert month name to its numerical representation."""
    month_map = {
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
    return month_map.get(month_name.lower())


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
        - 'format': The format of the date (as DateFormat enum)
        - 'datetime': The parsed datetime object (if parsing was successful)
    """
    results = []

    # Check each pattern
    for format_type, pattern in _compiled_patterns.items():
        matches = pattern.finditer(text)

        for match in matches:
            date_str = match.group(0)

            dt_obj = _parse_date_to_datetime(date_str, format_type)

            results.append(
                {
                    "date_str": date_str,
                    "format": format_type,
                    "datetime": dt_obj,
                }
            )

    return results
