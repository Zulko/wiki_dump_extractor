from typing import Tuple, Optional, List

from dataclasses import dataclass, field
import re
import regex


def get_short_description(text: str) -> str:
    """Return the short description of the page."""
    # Look for {{short description|...}} template
    match = re.search(r"\{\{short description\|(.*?)\}\}", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


def remove_appendix_sections(text: str) -> str:
    """Remove sections like References, Notes, etc. from the text."""
    sections_to_remove = [
        "References",
        "Notes",
        "Bibliography",
        "Further reading",
        "External links",
        "See also",
    ]
    for section in sections_to_remove:
        text = text.split(f"= {section} =")[0]
        text = text.split(f"={section}=")[0]
    return text

    return re.sub(r"== References ==.*?== Notes ==.*?", "", text, flags=re.DOTALL)


def remove_comments_and_citations(text: str) -> str:
    """Return the text without comments and citations."""
    patterns = [
        r"<!--.*?-->",
        r"{{Cite.*?}}",
        r"{{cite.*?}}",
        r"{{citation.*?}}",
        r"{{sfn.*?}}",
        r"<ref>.*?</ref>",
        r"<ref.*?>.*?</ref>",
        r"<ref.*?/>",
    ]
    for pattern in patterns:
        text = re.sub(pattern, " ", text)

    return text


def replace_titles_with_section_headers(text):
    text = re.sub(
        r"^===\s*(.*?)\s*===", r"\nSubsection: \1\n\n", text, flags=re.MULTILINE
    )
    text = re.sub(r"^==\s*(.*?)\s*==", r"\nSection: \1\n\n", text, flags=re.MULTILINE)
    text = re.sub(r"^=\s*(.*?)\s*=", r"\nChapter: \1\n\n", text, flags=re.MULTILINE)
    return text


def replace_file_links_with_captions(text):
    def replace_file_tag(match):
        # Extract the full content between File/Image: and ]]
        content = match.group(2)
        parts = [part.strip() for part in content.split("|")]
        if len(parts) == 1:
            # Just [[File:filename.jpg]] — remove it
            return ""
        else:
            # [[File:filename.jpg|...|...|text of interest]]
            return f"\n\n(image caption: {parts[-1]})\n\n"  # assume last part is the text of interest

    # Match both File and Image tags
    return re.sub(
        r"\[\[(File|Image):(.*?)\]\]",
        replace_file_tag,
        text,
        flags=re.DOTALL,
    )


def replace_nsbp_by_spaces(text: str) -> str:
    """Replace spaces with underscores in the text."""
    return text.replace("&nbsp;", " ").replace("{{Nbsp}}", " ").replace("<br />", " ; ")


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
    """Extract categories from Wikipedia text.

    Parameters
    ----------
    text : str
        The Wikipedia page text to extract categories from.

    Returns
    -------
    List[str]
        A list of category names with spaces normalized and sorted alphabetically.
    """
    # Match standard category syntax, handling both [[Category:Name]] and [[Category:Name|*]] formats
    # The pipe character can be used for sorting in Wikipedia but we don't need that part
    categories = re.findall(
        r"\[\[Category:([^|\]]+)(?:\|[^\]]+)?\]\]", text, re.IGNORECASE
    )

    # Clean up category names
    cleaned_categories = []
    for category in categories:
        # Remove HTML comments
        category = re.sub(r"<!--.*?-->", "", category)
        # Normalize whitespace
        category = re.sub(r"\s+", " ", category.strip())
        if category:
            cleaned_categories.append(category)

    # Return unique categories sorted alphabetically
    return sorted(set(cleaned_categories))


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


# Compile the pattern once for better performance
BRACE_PATTERN = regex.compile(r"\{\{(?:[^{}]|(?R))*\}\}")


def _find_matching_braces(text, start_pos):
    """Find the position of the matching closing braces from a starting position."""
    # Assume we're already inside a {{ pair, so we need to find the matching }}
    # Extract the substring starting from start_pos
    substring = text[start_pos:]

    # Look for the pattern that matches balanced braces
    match = BRACE_PATTERN.search(substring)

    if match:
        # Calculate the end position in the original string
        # Add 2 for the closing braces
        return start_pos + match.end()

    return -1


def parse_infobox(page_text: str) -> Tuple[dict, str]:
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

    infobox_start = page_text.find("{{Infobox")
    if infobox_start == -1:
        return {}, ""

    infobox_end = _find_matching_braces(page_text, infobox_start)
    if infobox_end == -1:
        return {}, ""

    original_infobox_text = page_text[infobox_start:infobox_end]
    infobox_text = original_infobox_text.replace("{{Infobox", "")

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

    return result, original_infobox_text


def extract_links(wiki_text):
    """Extract all the links of the form [[true page|text]] in a dict of the form
    {text: true page}"""
    pattern = r"\[\[([^|]+?)(?:\|(.*?))?\]\]"
    result = {}
    for match in re.finditer(pattern, wiki_text):
        if not match.group(2):
            text = match.group(1).strip()
            page = text.replace("_", " ")
        else:
            text = match.group(2).strip()
            page = match.group(1).strip().replace("_", " ")
        if text:
            already_assigned = result.get(text)
            if already_assigned is None:
                result[text] = match.group(1).strip()
            else:
                if isinstance(already_assigned, str):
                    result[text] = [already_assigned]
                if page not in result[text]:
                    result[text].append(page)
    return result


def extract_filenames(wiki_text):
    """
    Extract the filename from a MediaWiki file link using regular expressions.

    Args:
        wiki_file_text (str): The MediaWiki file link text

    Yields:
        str: Each extracted filename found in the text
    """
    # Pattern to match filename in MediaWiki file syntax
    # [[File:filename.ext|...]] or [[Image:filename.ext|...]]
    pattern = r"\[\[(File|Image):([^|]+?)(?:\|.*?)?\]\]"

    for match in re.finditer(pattern, wiki_text):
        yield match.group(2).strip()


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

    def with_cleaned_text(self):
        text = remove_comments_and_citations(self.text)
        text = replace_nsbp_by_spaces(text)
        text = replace_file_links_with_captions(text)
        return Section(
            level=self.level,
            title=self.title,
            text=text,
        )

    @classmethod
    def from_page_text(cls, text: str) -> "Section":
        """Build a tree of Section objects from a page text."""
        root_section = Section(level=0, title="Root", text="")
        section_stack = [root_section]

        # Pattern to match section headers
        header_pattern = re.compile(r"^(=+)\s*(.*?)\s*\1$", re.MULTILINE)

        # Find all section headers
        header_matches = list(header_pattern.finditer(text))

        # If no headers found, put all text in root section
        if not header_matches:
            root_section.text = text
            return root_section

        # Process each section
        for i, match in enumerate(header_matches):
            # Get section level and title
            level = len(match.group(1))
            title = match.group(2)

            # Get section text (includes the header itself)
            start = match.start()
            end = (
                header_matches[i + 1].start()
                if i < len(header_matches) - 1
                else len(text)
            )
            section_text = text[start:end]

            # Add text before the first section to root
            if i == 0 and start > 0:
                root_section.text = text[:start]

            # Create new section
            new_section = Section(level=level, title=title, text=section_text)

            # Pop sections from stack until appropriate parent is found
            while len(section_stack) > 1 and section_stack[-1].level >= level:
                section_stack.pop()

            # Add as child to parent
            parent = section_stack[-1]
            parent.children.append(new_section)
            new_section.parent = parent

            # Add to stack
            section_stack.append(new_section)

        return root_section

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

    def all_subsections_text_dict(self, text_dict: Optional[dict] = None) -> dict:
        """
        Recursively collect text from a section and all its subsections.

        Args:
            text_dict: Dictionary to store section titles and texts

        Returns:
            Dictionary mapping section titles to their text content
        """
        if text_dict is None:
            text_dict = {}

        text_dict[self.title] = self.text

        for sub_section in self.children:
            sub_section.all_subsections_text_dict(text_dict)

        return text_dict

    def __str__(self):
        return f"Section[{self.level}](title={self.title}, text={self.text[:20]}...)"
