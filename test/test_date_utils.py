import pytest

from src.wiki_dump_extractor.date_utils import DateRange, extract_dates


@pytest.mark.parametrize(
    "date, expected",
    [
        ("1810", "~1810/01/01 - ~1810/12/31"),
        ("1810-1812", "~1810/01/01 - ~1812/12/31"),
        ("1810/1812", "~1810/01/01 - ~1812/12/31"),
        ("1810/03/05", "1810/03/05 - 1810/03/05"),
        ("1810/03", "~1810/03/01 - ~1810/03/31"),
        ("1810/03 - 1812/05", "~1810/03/01 - ~1812/05/31"),
        ("1810/03/05 - 1812/05/07", "1810/03/05 - 1812/05/07"),
        ("1611/1612 - 1615/1617", "~1611/01/01 - ~1617/12/31"),
        ("1930s - 1940s", "~1930/01/01 - ~1949/12/31"),
        ("1930s", "~1930/01/01 - ~1939/12/31"),
        ("55 BC", "~0055 BC/01/01 - ~0055 BC/12/31"),
        ("55 BC - 52 BC", "~0055 BC/01/01 - ~0052 BC/12/31"),
        ("55 BC/03", "~0055 BC/03/01 - ~0055 BC/03/31"),
    ],
)
def test_parse_string_to_date_range(date, expected):
    assert DateRange.from_parsed_string(date).to_string() == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        # # Year
        ("in 1805,", "1805"),
        ("from 934,", "0934"),
        ("to 58,", "0058"),
        ("in 55 BC,", "0055 BC"),
        ("in 55 BCE,", "0055 BC"),
        ("16 November 8 BCE", "0008 BC/11/16"),
        ("in AD 55.", "0055"),
        ("in AD 7.", "0007"),
        ("6 November AD 15", "0015/11/06"),
        # # Year Month
        # ("1810/03,", "1810/03/01"),
        # ("934/03", "934/03/01"),
        # ("58/03", "58/03/01"),
        ("March 55 BC", "0055 BC/03"),
        ("March 1810", "1810/03"),
        # Year Month Day
        ("1810/03/05", "1810/03/05"),
        ("55/02/03 BC", "0055 BC/02/03"),
        ("12 July 100 BC", "0100 BC/07/12"),
        ("7 December 43 BC", "0043 BC/12/07"),
        # month day year
        ("March 5th, 1810", "1810/03/05"),
        # Wikipedia date formats
        ("{{Birth date|1810|03|05}}", "1810/03/05"),
        ("{{Birth date|1810|03|05|deg=y}}", "1810/03/05"),
        # # SlashDMYMDYFormat
        # ("1810/03/05", "1810/03/05"),
    ],
)
def test_extract_dates(text, expected):
    detected_dates, errors = extract_dates(text)
    assert len(errors) == 0, errors
    print(detected_dates)
    assert expected in [
        detected_date.date.to_string() for detected_date in detected_dates
    ]
