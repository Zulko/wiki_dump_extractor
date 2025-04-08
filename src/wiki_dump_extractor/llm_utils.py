from typing import List, Dict
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.settings import ModelSettings
from wiki_dump_extractor import page_utils
import mwparserfromhell

events_prompt = """
You are an expert event extraction assistant.

Your task is to carefully analyze the provided Wikipedia text and extract ALL distinct events into a structured JSON format, strictly following the guidelines below:

Each event must be represented as a JSON object containing these specific fields:

who: List every individual or group involved, separated by "|". Include all mentioned participants.

what: Concisely summarize what occurred in the event. Avoid including dates or location here.

where: Clearly specify the exact location or venue, providing as much detail as possible (e.g., landmark, building name, region).

city: Include the name of the city if available.

when: Provide the exact event date in the format YYYY/MM/DD (use "YYYY BC" for years before Christ). If the exact date isn't explicitly stated, provide a date range in the format YYYY/MM/DD - YYYY/MM/DD. If the day is unknown, give the month as YYYY/MM. If neither day nor month is available, only use the year (YYYY).

Additional critical instructions:

If information about a single event is scattered across the text, integrate all details into one comprehensive event entry.

Ensure no events are omitted; review the text carefully.

Always provide at least one event; do not return an empty list.

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
    preprocess_text=True,
    **model_settings,
) -> List[Dict]:
    """Get all events in a text using an LLM.

    This requires to set the `GOOGLE_API_KEY` environment variable.

    Examples
    --------

    .. code:: python

        dump = WikiAvroDumpExtractor("wiki_dump.avro", index_dir="wiki_dump_idx")
        page = dump.get_page_by_title("Giuseppe Verdi")
        cleaned_text = page_utils.remove_comments_and_citations(page.text)
        cleaned_text = str(mwparserfromhell.parse(cleaned_text).strip_code())
        events = get_all_events(cleaned_text)
    """
    if preprocess_text:
        text = page_utils.remove_comments_and_citations(text)
        text = str(mwparserfromhell.parse(text).strip_code())
    agent = Agent(
        model=model,
        system_prompt=events_prompt,
        result_type=EventsResponse,
        model_settings=ModelSettings(**model_settings),
    )
    return await agent.run(text)


__all__ = ["get_all_events"]
