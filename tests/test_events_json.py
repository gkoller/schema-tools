from datetime import date, datetime

from dateutil.parser import parse as dtparse

from schematools.events.json import EventsProcessor
from schematools.events.reader import read_messages_from_file

# pytestmark = pytest.mark.skip("all tests disabled")


def test_message_process_insert(here, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    processor = EventsProcessor([gebieden_schema])
    messages = list(read_messages_from_file(events_path))
    blobs = []
    for source_id, message_headers, message_body in messages:
        key = processor.fetch_key(message_headers)
        blobs.append(
            processor.fetch_event_data(
                key,
                message_headers,
                message_body,
            )
        )

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

    key = processor.fetch_key(messages[0][1])
    first_blob = processor.fetch_event_data(key, *messages[0][1:])
    # fetch updated blob
    updated_blob = processor.fetch_event_data(
        key, *messages[1][1:], current_event=first_blob.fields
    )

    assert updated_blob.fields["code"] == "AA01"
    assert dtparse(updated_blob.fields["begin_geldigheid"]).date() == date(2020, 2, 5)
    assert dtparse(updated_blob.fields["registratiedatum"]) == datetime(2020, 2, 5, 15, 6, 43)


def test_message_process_delete(here, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken_delete.gobevents"
    processor = EventsProcessor([gebieden_schema])
    messages = read_messages_from_file(events_path)
    blobs = []
    for source_id, message_headers, message_body in messages:
        key = processor.fetch_key(message_headers)
        blobs.append(
            processor.fetch_event_data(
                key,
                message_headers,
                message_body,
            )
        )

    assert len(blobs) == 3
    assert blobs[2].event_type == "DELETE"
    assert blobs[2].fields is None
