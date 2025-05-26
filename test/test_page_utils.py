"""Tests for the page_utils module."""

import pytest
from src.wiki_dump_extractor import page_utils


def test_extract_geospatial_coordinates_degrees_minutes_seconds():
    """Test extracting coordinates in degrees, minutes, seconds format."""
    wiki_text = """
    Paris {{Coord|48|51|24|N|2|21|03|E|display=inline,title}}
    """

    coordinates = page_utils.extract_geospatial_coordinates(wiki_text)
    assert coordinates is not None
    latitude, longitude = coordinates
    assert latitude == pytest.approx(48.85667, abs=0.001)  # 48°51'24"N ≈ 48.85667
    assert longitude == pytest.approx(2.35083, abs=0.001)  # 2°21'03"E ≈ 2.35083


def test_extract_geospatial_coordinates_infobox():
    """Test extracting coordinates from infobox format."""
    wiki_text = """
    {{Infobox settlement
    | name = Tokyo
    | coordinates = {{coord|35|41|N|139|41|E|display=inline,title}}
    }}
    """

    coordinates = page_utils.extract_geospatial_coordinates(wiki_text)
    assert coordinates is not None
    latitude, longitude = coordinates
    assert latitude == pytest.approx(35.6833, abs=0.001)  # 35°41'N ≈ 35.6833
    assert longitude == pytest.approx(139.6833, abs=0.001)  # 139°41'E ≈ 139.6833


def test_extract_geospatial_coordinates_none():
    """Test that None is returned when no coordinates are present."""
    wiki_text = """
    This is a page about a concept with no geographical location.
    It contains no coordinates.
    """

    coordinates = page_utils.extract_geospatial_coordinates(wiki_text)
    assert coordinates is None


def test_extract_geospatial_coordinates_malformed():
    """Test handling of malformed coordinate data."""
    wiki_text = """
    {{Coord|invalid|data|display=inline}}
    """

    coordinates = page_utils.extract_geospatial_coordinates(wiki_text)
    assert coordinates is None  # Assuming the function returns None for invalid data


@pytest.mark.parametrize(
    "text, expected",
    [
        ("[[File:image.jpg|caption]]", "\n\n(image caption: caption)\n\n"),
        ("[[File:image.jpg]]", ""),
    ],
)
def test_replace_file_links_with_captions(text, expected):
    assert page_utils.replace_file_links_with_captions(text) == expected
