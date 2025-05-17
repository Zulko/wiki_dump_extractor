from typing import List, Dict, Any
import json
import os
from pathlib import Path
import time
import asyncio

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.settings import ModelSettings
from wiki_dump_extractor import page_utils, date_utils
from dataclasses import dataclass, field
from google import genai
from google.genai._transformers import process_schema
import mwparserfromhell
from google.cloud import storage
from google.cloud import aiplatform

with open(Path(__file__).parent / "event_extraction_prompt.md", "r") as f:
    events_prompt = f.read()


class Event(BaseModel):
    who: str
    what: str
    where: str
    city: str
    when: str

    def to_string(self):
        return f"{self.when} - {self.where} ({self.city}) [{self.who}] {self.what}"


class EventsList(BaseModel):
    events: List[Event]

    def to_string(self):
        return "\n\n".join([e.to_string() for e in self.events])


@dataclass
class PageEventExtractionRequest:
    page_title: str
    text: str
    model_settings: Dict[str, Any] = field(default_factory=lambda: {"temperature": 0})

    def _get_run_params(self, client: genai.Client = None):
        if client is None:
            client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        schema = EventsList.model_json_schema()
        process_schema(client=client, schema=schema)
        return {
            "contents": self.text,
            "config": {
                "response_mime_type": "application/json",
                "response_schema": schema,
                "system_instruction": events_prompt,
                "max_output_tokens": 8192,
                **self.model_settings,
            },
        }

    @classmethod
    def from_page(cls, page, model_settings: Dict[str, Any] = None):
        text = format_page_text_for_llm(page.text)
        return cls(page_title=page.title, text=text, model_settings=model_settings)

    def _process_response(self, response):
        usage = response.usage_metadata
        usage_dict = {
            "prompt_token_count": usage.prompt_token_count,
            "candidates_token_count": usage.candidates_token_count,
        }
        events = EventsList(**response.parsed)
        return events, usage_dict

    def run(self, model: str = "gemini-2.0-flash"):
        client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        params = self._get_run_params(client)
        response = client.models.generate_content(model=model, **params)
        return self._process_response(response)

    async def run_async(self, model: str = "gemini-2.0-flash"):
        client = genai.AsyncClient(api_key=os.environ["GEMINI_API_KEY"])
        params = self._get_run_params(client)
        response = await client.models.generate_content(model=model, **params)
        return self._process_response(response)

    def to_jsonl_request(self, client=None) -> str:
        if client is None:
            client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        schema = EventsList.model_json_schema()
        process_schema(client=client, schema=schema)
        text = events_prompt + "\n\nBelow is the text to analyze:\n\n" + self.text
        request = {
            "contents": [
                {"role": "user", "parts": [{"text": text}]},
            ],
            "generationConfig": {
                "response_mime_type": "application/json",
                "response_schema": schema,
                "max_output_tokens": 8192,
                **self.model_settings,
            },
        }
        return {"page_title": self.page_title, "request": request}

    @staticmethod
    def list_to_batch_jsonl(
        requests: List["PageEventExtractionRequest"],
        path: str | Path,
    ) -> str:
        """
        Writes requests to a JSONL file, uploads to GCS, and runs Gemini batch inference.

        Args:
            requests: List of PageEventExtractionRequest objects
            output_jsonl: Local path to save the JSONL file
            bucket_name: GCS bucket name to upload the JSONL file
            batch_output_dir: Optional directory path in GCS for batch outputs

        Returns:
            Batch job ID string
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write requests to JSONL file
        api_client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        with open(path, "w") as f:
            for request in requests:
                f.write(json.dumps(request.to_jsonl_request(api_client)) + "\n")


def process_batch_request(
    jsonl_path: Path, batch_name: str, bucket_name: str, model: str = "gemini-2.0-flash"
):
    # Upload JSONL to Google Cloud Storage

    # NORMALIZE MODEL NAME
    jsonl_path = Path(jsonl_path)
    model_mapping = {
        "gemini-2.0-flash": "gemini-2.0-flash-001",
        "gemini-2.0-flash-lite": "gemini-2.0-flash-lite-001",
        "gemini-2.0-pro": "gemini-2.0-pro-001",
    }
    vertex_model = model
    if model in model_mapping:
        vertex_model = f"publishers/google/models/{model_mapping[model]}"

    # GENERATE CLIENT AND BUCKET URIS

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{batch_name}/{batch_name}.jsonl")
    blob.upload_from_filename(jsonl_path)

    gcs_input_uri = f"gs://{bucket_name}/{batch_name}/{batch_name}.jsonl"
    print(gcs_input_uri)
    gcs_output_uri = f"gs://{bucket_name}/{batch_name}/batch_output/"

    # PERFORM THE BATCH PREDICTION
    print(f"Launching {batch_name}...")
    aiplatform.init()
    batch_job = aiplatform.BatchPredictionJob.create(
        model_name=vertex_model,
        job_display_name=f"event-extraction-{batch_name}-{str(int(time.time()))}",
        gcs_source=gcs_input_uri,
        gcs_destination_prefix=gcs_output_uri,
        instances_format="jsonl",
        predictions_format="jsonl",
        sync=True,
    )
    print(f"Batch job {batch_job.resource_name} started. Waiting for completion...")
    batch_job.wait()

    # DOWNLOAD THE RESULTS
    print(f"Downloading and parsing results for {batch_name}...")
    blobs = list(bucket.list_blobs(prefix=f"{batch_name}/batch_output/"))

    # Find the most recent folder
    folders = set()
    for blob in blobs:
        if "/" in blob.name.replace(f"{batch_name}/batch_output/", "", 1):
            folder_name = blob.name.split("/")[2]
            folders.add(folder_name)

    if folders:
        last_folder = sorted(folders)[-1]
        predictions_path = f"{batch_name}/batch_output/{last_folder}/predictions.jsonl"
        blob = bucket.blob(predictions_path)
        jsonl_content = blob.download_as_text()

        # Download to a local file
        local_predictions_path = f"{jsonl_path.parent}/{batch_name}_predictions.jsonl"
        with open(local_predictions_path, "w") as f:
            f.write(jsonl_content)
    else:
        print("No batch output folders found")

    # PARSE THE RESULTS
    results_by_page = {}
    failed = {}
    errored = {}
    total_usage = {}
    with open(local_predictions_path, "r") as f:
        for line in f:
            page_results = json.loads(line)
            page_title = page_results["page_title"]
            try:
                usage = page_results["response"]["usageMetadata"]
                for key in ["promptTokenCount", "candidatesTokenCount"]:
                    total_usage[key] = total_usage.get(key, 0) + usage.get(key, 0)
                    candidate = page_results["response"]["candidates"][0]
                    if candidate["finishReason"] != "STOP":
                        failed[page_title] = page_results

                    json_response = candidate["content"]["parts"][0]["text"]
                    events = json.loads(json_response)["events"]
                    results_by_page[page_title] = events
            except Exception as e:
                errored[page_title] = str(e)

    # DELETE THE BATCH OUTPUT FOLDER
    blobs_to_delete = list(bucket.list_blobs(prefix=f"{batch_name}/"))

    for blob in blobs_to_delete:
        blob.delete()

    return results_by_page, failed, errored, total_usage


async def process_batch_request_async(
    jsonl_path: Path, batch_name: str, bucket_name: str, model: str = "gemini-2.0-flash"
):
    """
    Async version of process_batch_request that runs the synchronous operation in a separate thread.

    Args:
        jsonl_path: Path to the JSONL file
        batch_name: Name for the batch job
        bucket_name: GCS bucket name
        model: Model name to use for prediction

    Returns:
        BatchPredictionJob object
    """
    return await asyncio.to_thread(
        process_batch_request, jsonl_path, batch_name, bucket_name, model
    )


def pages_to_batch_request_jsonl(pages: List) -> str:
    return "\n".join([page.to_dict() for page in pages])


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
        result_type=EventsList,
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
    """Format the page by (1) parsing the infox and (2) cleaning the main body"""
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

    return (
        "Infos from the infobox:\n"
        + "\n".join(str_fields)
        + "\n\n"
        + "Wikipedia article text:\n"
        + formatted_text
    )


__all__ = ["get_all_events", "PageEventExtractionRequest"]
