from typing import List, Dict
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.settings import ModelSettings

event_prompt = """
You are an expert event extraction assistant.
Your task is to extract ALL events from the provided Wikipedia text into
structured JSON format, strictly adhering to the following instructions:

Each event JSON object must contain these fields:

who: List every person or group involved. Separate multiple names with "|".

what: Provide a concise summary of the event. Avoid repeating date or location.

where: Give detailed location information (e.g., place, building), as specific as possible.

city: Provide the city name, if known.

when: Provide the exact date in YYYY/MM/DD format, or a date range (YYYY/MM/DD - YYYY/MM/DD). Always search thoroughly for an exact day.

If multiple pieces of information about the same event appear scattered throughout the text, combine them into a single, complete event entry. If some information is missing, make a reasoned inference based on the available context.

Example of correctly structured event:

{
"who": "Benedetto Marcello|Rosanna Scalfi",
"what": "Married in a secret ceremony",
"where": "Saint Mark's Basilica",
"city": "Venice",
"when": "1728/05/20"
}

Important:

Return events as a list, even if there is only one event.

Do not return an empty list; the response must always contain at least one event.
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
    text: str, model="google-gla:gemini-1.5-flash-8b"
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
    agent = Agent(
        model=model,
        system_prompt=events_prompt,
        result_type=EventsResponse,
        model_settings=ModelSettings(temperature=0.1),
    )
    return await agent.run(text)


__all__ = ["get_all_events"]
