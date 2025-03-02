import re


def extract_geospatial_coordinates(text: str) -> tuple[float, float] | None:
    """Return geographical coordinates (latitude, longitude) from Wikipedia page text.

    Parameters
    ----------
    text : str
        The wikipedia page text to extract coordinates from.

    Returns
    -------
    tuple[float, float] | None
        The geographical coordinates (latitude, longitude) or None if no coordinates are found.
    """
    if text is None:
        return None

    # Match {{Coord}} template variations
    coord_pattern = r"""
        \{\{[Cc]oord\s*\|
        (\d+)\s*\|              # Degrees latitude
        (\d+)\s*\|              # Minutes latitude
        (\d+)?\s*\|?            # Optional seconds latitude
        ([NS])\s*\|             # North/South indicator
        (\d+)\s*\|              # Degrees longitude
        (\d+)\s*\|              # Minutes longitude
        (\d+)?\s*\|?            # Optional seconds longitude
        ([EW])                  # East/West indicator
    """
    match = re.search(coord_pattern, text, re.VERBOSE)

    if match:
        try:
            lat_deg, lat_min, lat_sec, lat_dir = match.group(1, 2, 3, 4)
            lon_deg, lon_min, lon_sec, lon_dir = match.group(5, 6, 7, 8)

            # Convert to decimal degrees
            lat = float(lat_deg) + float(lat_min) / 60 + (float(lat_sec or 0) / 3600)
            lon = float(lon_deg) + float(lon_min) / 60 + (float(lon_sec or 0) / 3600)

            # Apply direction
            if lat_dir == "S":
                lat = -lat
            if lon_dir == "W":
                lon = -lon

            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return (lat, lon)
        except ValueError:
            pass

    # Match coordinates in infoboxes
    infobox_pattern = r"""
        \|\s*(?:
            latitude\s*=\s*([+-]?\d+\.?\d*)|
            longitude\s*=\s*([+-]?\d+\.?\d*)
        )
    """
    matches = re.finditer(infobox_pattern, text, re.VERBOSE | re.IGNORECASE)
    lat = lon = None
    for match in matches:
        if match.group(1):  # latitude
            try:
                lat = float(match.group(1))
            except ValueError:
                continue
        if match.group(2):  # longitude
            try:
                lon = float(match.group(2))
            except ValueError:
                continue

    if lat is not None and lon is not None:
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)

    return None


def extract_categories(text: str) -> list[str]:
    """Extract categories from Wikipedia text using regex patterns."""
    return re.findall(r"\[\[Category:(.*?)\]\]", text)


def extract_infobox_category(text: str) -> str | None:
    """Extract the broad category from the infobox of a Wikipedia page.

    Parameters
    ----------
    text : str
        The wikipedia page text to extract the infobox category from.
    """
    infobox_match = re.search(r"\{\{Infobox\s+([^\|\}]+)", text)
    if infobox_match:
        broad_category = infobox_match.group(1).strip().lower()
        broad_category = broad_category.split("\n")[0].split("<!--")[0].split("|")[0]
        broad_category = broad_category.strip()


_months = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
)

_date_patterns = [
    # Define month names once
    # More readable date patterns
    r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}",  # DD/MM/YYYY or MM/DD/YYYY
    r"\d{4}[-/]\d{1,2}[-/]\d{1,2}",  # YYYY/MM/DD
    rf"\b\d{{1,2}}\s+(?:{_months})[,\s]+\d{{2,4}}\b",  # DD Month YYYY
    rf"\b(?:{_months})\s+\d{{1,2}}(?:st|nd|rd|th)?[,\s]+\d{{2,4}}\b",  # Month DD YYYY
    r"\b\d{4}\b",  # Just year
]

# Combine patterns and compile once
_date_pattern_re = re.compile("|".join(_date_patterns), re.IGNORECASE)


def extract_dates(text: str) -> list[str]:
    """Extract dates from Wikipedia text using regex patterns."""
    return _date_pattern_re.findall(text)


def has_date(text: str) -> bool:
    """
    Return true if the text contains any dates detected by regex patterns.
    """
    return bool(_date_pattern_re.search(text))
