from datetime import date, datetime

import pytest
from dateutil.parser import parse as dtparse

from schematools.events.json import EventsProcessor
from schematools.events.reader import read_messages_from_file

# pytestmark = pytest.mark.skip("all tests disabled")


def test_message_process_insert(here, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    processor = EventsProcessor([gebieden_schema])
    messages = list(read_messages_from_file(events_path))
    blobs = [
        processor.fetch_blob(
            source_id,
            message_headers,
            message_body,
        )
        for source_id, message_headers, message_body in messages
    ]

    assert len(blobs) == 2
    assert blobs[0].fields["code"] == "AA01"
    assert blobs[1].fields["code"] == "AA02"
    assert blobs[0].fields["eind_geldigheid"] is None
    assert blobs[0].fields["begin_geldigheid"] == "2006-06-12"


def test_message_process_update(here, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken_update.gobevents"
    processor = EventsProcessor([gebieden_schema])
    messages = list(read_messages_from_file(events_path))
    # fetch blob for the first message
    first_blob = processor.fetch_blob(*messages[0])
    # fetch update blob
    updated_blob = processor.fetch_blob(
        *messages[1],
        current_blob_value=first_blob.fields,
    )

    assert updated_blob.fields["code"] == "AA01"
    assert dtparse(updated_blob.fields["begin_geldigheid"]).date() == date(2020, 2, 5)
    assert dtparse(updated_blob.fields["registratiedatum"]) == datetime(2020, 2, 5, 15, 6, 43)
