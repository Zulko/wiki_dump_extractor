from typing import Tuple, Optional, List
import re


def extract_geospatial_coordinates(text: str) -> Optional[Tuple[float, float]]:
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


def extract_categories(text: str) -> List[str]:
    """Extract categories from Wikipedia text using regex patterns."""
    return re.findall(r"\[\[Category:(.*?)\]\]", text)


def extract_infobox_category(text: str) -> Optional[str]:
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


def parse_infobox(page_text: str) -> dict:
    """Parse the infobox from a Wikipedia page text.

    Example of infobox. This recognizes the "{{Infobox" pattern. then parses
    the fields starting with "|" as key-value pairs.

    {{Infobox military conflict
    | conflict          = First Battle of the Marne
    | partof            = the [[Western Front (World War I)|Western Front]] of [[World War I]]
    | image             = German soldiers Battle of Marne WWI.jpg
    | image_size        = 300
    | caption           = German soldiers (wearing distinctive [[pickelhaube]
    | date              = 5–14 September 1914
    | place             = [[Marne River]] near [[Brasles]], east of Paris
    | coordinates       = {{coord|49|1|N|3|23|E|region:FR_type:event|display= inline}}
    | result            = Allied victory
    | territory         = German advance to Paris repulsed
    }}

    Parameters
    ----------
    page_text : str
        The wikipedia page text to extract the infobox from.

    Returns
    -------
    dict
        The infobox as a dictionary.
    """

    # Find the infobox pattern with proper handling of nested templates
    def find_matching_braces(text, start_pos):
        """Find the position of the matching closing braces from a starting position."""
        stack = []
        i = start_pos
        while i < len(text):
            if text[i : i + 2] == "{{":
                stack.append("{{")
                i += 2
            elif text[i : i + 2] == "}}":
                if not stack:
                    return i
                stack.pop()
                i += 2
                if not stack:
                    return i
            else:
                i += 1
        return -1  # No matching braces found

    infobox_start = page_text.find("{{Infobox")
    if infobox_start == -1:
        return {}

    infobox_end = find_matching_braces(page_text, infobox_start)
    if infobox_end == -1:
        return {}

    infobox_text = page_text[infobox_start + 9 : infobox_end]

    # Extract the category (first line after "{{Infobox")
    category_match = re.match(r"([^\|\n]+)", infobox_text)
    category = category_match.group(1).strip().lower() if category_match else ""
    category = re.sub(r"<!--.*?-->", "", category)

    # Parse the key-value pairs
    result = {"category": category}

    # Find all lines starting with "|"
    field_matches = re.finditer(
        r"\|\s*([a-z0-9_]+)\s*=(.*)(?=\n\||$)",
        infobox_text,
        re.MULTILINE,
    )

    for match in field_matches:
        key = match.group(1).strip()
        value = match.group(2).strip()

        # Remove HTML comments from value
        value = re.sub(r"<!--.*?-->", "", value)
        result[key] = value

    return result


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


def extract_dates(text: str) -> List[str]:
    """Extract dates from Wikipedia text using regex patterns."""
    return _date_pattern_re.findall(text)


def has_date(text: str) -> bool:
    """
    Return true if the text contains any dates detected by regex patterns.
    """
    return bool(_date_pattern_re.search(text))


import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class Section:
    level: int
    title: str
    text: str = ""
    children: List["Section"] = field(default_factory=list)
    parent: Optional["Section"] = None

    def to_dict(self):
        """Convert the Section to a dictionary representation."""
        return {
            "section_title": self.title,
            "text": self.text,
            "children": [child.to_dict() for child in self.children],
        }

    @property
    def title_with_parents(self):
        if self.parent is not None and self.parent.title is not None:
            return f"{self.parent.title_with_parents} > {self.title}"
        else:
            return self.title

    @classmethod
    def from_page_section_texts(cls, texts: List[str]) -> "Section":
        """Build a tree of Section objects from a list of section texts."""
        roots: List[Section] = []
        stack: List[Section] = []

        for text in texts:
            current_section = cls.from_single_section_text(text)

            # Pop sections from stack until the stack is empty or the top section is of a lower level.
            while stack and stack[-1].level >= current_section.level:
                stack.pop()

            if not stack:
                # No parent available; this is a root section.
                roots.append(current_section)
            else:
                # The current section is a child of the last section in the stack.
                stack[-1].children.append(current_section)
                current_section.parent = stack[-1]

            # Push the current section onto the stack.
            stack.append(current_section)

        if len(roots) == 1:
            return roots[0]
        else:
            # If no sections were found, return an empty list
            if not roots:
                return Section(level=0, title="Root", text="")

            # Create a root section to hold all sections if there are multiple roots
            root_section = Section(level=0, title="Root", text="")
            for section in roots:
                root_section.children.append(section)
                section.parent = root_section
            return root_section

    def from_single_section_text(section_text: str) -> "Section":
        """
        Parse a heading string of the form '== Title ==' or '=== Title ==='
        and return a Section with the appropriate level and title.
        """
        # Regex: group1: leading equals signs, group2: title, then trailing equals that match group1
        if "\n" not in section_text:
            return Section(level=0, title=None, text=section_text)
        header, text = section_text.split("\n", 1)

        match = re.match(r"^(=+)\s*(.*?)\s*\1$", header)
        if match:
            equals = match.group(1)
            title = match.group(2)
            level = len(equals)

            return Section(level=level, title=title, text=text)
        else:
            return Section(level=0, title=None, text=section_text)

    def get_section_text_by_title(self, title: str) -> str:
        if self.title == title or self.title_with_parents == title:
            return self.text
        else:
            for child in self.children:
                result = child.get_section_text_by_title(title)
                if result is not None:
                    return result

    def __str__(self):
        return f"Section[{self.level}](title={self.title}, text={self.text[:20]}...)"
