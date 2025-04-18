from typing import List, Dict
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.settings import ModelSettings
from wiki_dump_extractor import page_utils, date_utils
import mwparserfromhell

events_prompt = """
You are an expert event extraction assistant.

Your task is to carefully analyze the provided Wikipedia text and extract ALL distinct events for which there is a date and place mentioned, strictly following the guidelines below:

Each event must be represented as a JSON object containing these specific fields:

who: List every individual or group involved, separated by "|". Include all mentioned participants and use their full names, which would be the name of their wikipedia page (e.g. Louis XIV, Peter the Great, etc.).

what: Concisely summarize what occurred in the event. Avoid including dates or location here.

where: Clearly specify the exact location or venue, providing as much detail as possible (e.g., landmark, building name, region).

city: Include the name of the city if available.

when: Provide the exact event date in the format YYYY/MM/DD (use "YYYY BC" for years before Christ). If the exact date isn't explicitly stated, provide a date range in the format YYYY/MM/DD - YYYY/MM/DD. If the day is unknown, give the month as YYYY/MM. If neither day nor month is available, only use the year (YYYY).

Additional critical instructions:

If information about a single event is scattered across the text, integrate all details into one comprehensive event entry.

Ensure no events are omitted; review the text carefully and extract all life events, career events, historical events, political events, military events, cultural events, scientific events, etc. Pay special attention to dates mentioned in the text, as they often indicate important events.

Return the result as a JSON list, even if only a single event is identified.

Example of a correctly structured event:

[
  {
    "who": "Benedetto Marcello|Rosanna Scalfi",
    "what": "Married in a secret ceremony",
    "where": "Saint Mark's Basilica",
    "city": "Venice",
    "when": "1728/05/20"
  }
]

Your response must strictly adhere to these instructions and formatting.
"""


class Event(BaseModel):
    who: str
    what: str
    where: str
    city: str
    when: str

    def to_string(self):
        return f"{self.when} - {self.where} ({self.city}) [{self.who}] {self.what}"


class EventsResponse(BaseModel):
    events: List[Event]

    def to_string(self):
        return "\n\n".join([e.to_string() for e in self.events])


async def get_all_events(
    text: str,
    model="google-gla:gemini-2.0-flash-lite",
    **model_settings,
) -> List[Dict]:
    """Get all events in a text using an LLM.

    This requires to set the `GOOGLE_API_KEY` environment variable.

    Examples
    --------

    .. code:: python

        dump = WikiAvroDumpExtractor("wiki_dump.avro", index_dir="wiki_dump_idx")
        page = dump.get_page_by_title("Giuseppe Verdi")
        cleaned_text = llm_utils.format_page_text_for_llm(page.text)
        results = await llm_utils.get_all_events(cleaned_text)
        print (results.data.to_string())
    """
    agent = Agent(
        model=model,
        system_prompt=events_prompt,
        result_type=EventsResponse,
        model_settings=ModelSettings(**model_settings),
    )
    return await agent.run(text)


def format_value(value: str) -> str:
    value = page_utils.remove_comments_and_citations(value)
    value = value.replace("[[", "").replace("]]", "")
    # Find all WikiDateFormat patterns in the value and replace them with formatted dates


def clean_text_for_llm(text: str) -> str:
    text = page_utils.remove_appendix_sections(text)
    text = page_utils.replace_titles_with_section_headers(text)
    text = page_utils.remove_comments_and_citations(text)
    text = page_utils.replace_file_links_with_captions(text)
    text = page_utils.replace_nsbp_by_spaces(text)
    text = text

    for match in date_utils.WikiDateFormat.pattern.finditer(text):
        try:
            date = date_utils.WikiDateFormat.match_to_date(match)
            formatted_date = date.to_string() + "  "
            # Replace the matched pattern with the formatted date
            text = text.replace(match.group(0), formatted_date)
        except (ValueError, AttributeError):
            # Skip if date parsing fails
            continue
    fully_parsed = str(mwparserfromhell.parse(text).strip_code())
    return str(fully_parsed.replace("[[", "").replace("]]", ""))


def format_page_text_for_llm(text: str, include_infobox: bool = True) -> str:
    infobox, infobox_text = page_utils.parse_infobox(text)
    if infobox:
        text = text.replace(infobox_text, "")

    formatted_text = clean_text_for_llm(text)
    if not include_infobox:
        return formatted_text

    if infobox is None:
        return formatted_text

    cleaned_fields = [
        (key.replace("_", " "), clean_text_for_llm(value))
        for key, value in infobox.items()
        if value.strip() != ""
    ]
    str_fields = [
        f"- {key}: {value}" for key, value in cleaned_fields if value.strip() != ""
    ]

    return "Infos from the infobox:\n" + "\n".join(str_fields) + "\n\n" + formatted_text


__all__ = ["get_all_events"]


"""
You are an expert event extraction assistant.

Your task is to carefully analyze the provided Wikipedia text and extract ALL distinct events for which there is a date and place mentioned, strictly following the guidelines below:

Each event must be represented as a JSON object containing these specific fields:

who: List every individual or group involved, separated by "|". Include all mentioned participants.

what: Concisely summarize what occurred in the event. Avoid including dates or location here.

where: Clearly specify the exact location or venue, providing as much detail as possible (e.g., landmark, building name, region).

city: Include the name of the city if available.

when: Provide the exact event date in the format YYYY/MM/DD (use "YYYY BC" for years before Christ). If the exact date isn't explicitly stated, provide a date range in the format YYYY/MM/DD - YYYY/MM/DD. If the day is unknown, give the month as YYYY/MM. If neither day nor month is available, only use the year (YYYY).

Additional critical instructions:

If information about a single event is scattered across the text, integrate all details into one comprehensive event entry.

Ensure no events are omitted; review the text carefully and extract all life events, career events, historical events, political events, military events, cultural events, scientific events, etc. Pay special attention to dates mentioned in the text, as they often indicate important events.

Return the result as a JSON list, even if only a single event is identified.

Example of a correctly structured event:

[
  {
    "who": "Benedetto Marcello|Rosanna Scalfi",
    "what": "Married in a secret ceremony",
    "where": "Saint Mark's Basilica",
    "city": "Venice",
    "when": "1728/05/20"
  }
]

Your response must strictly adhere to these instructions and formatting.
"""
