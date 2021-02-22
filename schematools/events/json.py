__doc__ = """

Eventprocessor that does crud operations on json blob objects.

Spark has it own internal kv store. GOB data will be stored
in this kv storeas json blobs, addressed by a unique key with the following
components:  dataset - table - identificatie [ - volgnummer]

Course of action is as follows:

- GOB Events comes in (ADD/MODIFY/DELETE) from Kafka
- For ADD:
  - Create json blob for the record
  - Store json blob in spark kv store
  - Create Kafka event with this record
- FOR MODIFY:
  - Fetch json blob from spark kv store (based on unique key, see above)
  - Update json blob with the incoming partial event data
  - Store json blob in spark kv store
  - Create Kafka event with the complete record (from the json blob)
- For DELETE:
  - Delete json blob in spark kv store (base on unique key)
  - Create Kafka delete event for this record


The current implementation updates the relational database with
the incoming event data from GOB. This implementation should be converted
so that it modifies the json blob.

Open questions:

How do we handle relations?
Relations in kv store: left-ds - table - left-ident - right-ds - table - right-ident

Is json the most appropriate serialization format for the spark kv store?
Or is there some kind of binary format that is more efficient?

"""

from collections import defaultdict
from dataclasses import dataclass, field
import json
from ..types import DatasetSchema
from typing import List, Dict, Any, Optional
from .fetchers import fetch_insert_data, fetch_update_data

# Maybe
EMPTY_VALUES = {"string": "", "number": None, "integer": None}
EVENT_TYPE_MAPPING = {"ADD": fetch_insert_data, "MODIFY": fetch_update_data}


@dataclass
class Blob:
    key: str
    dataset_id: str
    table_id: str
    fields: dict = field(default_factory=dict)


class BlobMaker:
    def __init__(
        self,
        datasets: Dict[str, DatasetSchema],
    ):
        self.datasets = datasets

    def fetch_empty_blob(self, dataset_id: str, table_id: str) -> Blob:

        fields_dict = {}
        # Don't need temporal fields in blob
        self.datasets[dataset_id].use_dimension_fields = False
        table_schema = self.datasets[dataset_id].get_table_by_id(table_id)
        for ds_field in table_schema.get_fields(add_sub_fields=False):
            try:
                initial_value = EMPTY_VALUES[ds_field.type]
            except KeyError:
                if "geojson" in ds_field.type:
                    initial_value = None
                else:
                    continue
            fields_dict[ds_field.name] = initial_value
        return Blob(None, dataset_id, table_id, fields_dict)

    def fetch_updated_blob(self, blob: Blob, field_updates: dict) -> None:
        # Immutable approach
        return Blob(
            blob.key, blob.dataset_id, blob.table_id, fields={**(blob.fields), **field_updates}
        )


class EventsProcessor:
    def __init__(
        self,
        datasets: List[DatasetSchema],
    ):
        self.datasets: Dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
        self.blob_maker = BlobMaker(self.datasets)
        for dataset_id, dataset in self.datasets.items():
            # Collect the geo fields, needed to add srid tot the WKT value
            self.geo_fields = defaultdict(lambda: defaultdict(list))
            for table in dataset.tables:
                for table_field in dataset.get_table_by_id(table.id).fields:
                    if table_field.is_geo:
                        self.geo_fields[dataset_id][table.id].append(table_field.name)

    def fetch_unique_key(
        self,
        message_headers: Dict[str, Any],
        message_body: Dict[str, Any],
    ):
        event_type = message_headers["event_type"]
        dataset_id = message_headers["catalog"]
        table_id = message_headers["collection"]
        data_fetcher = EVENT_TYPE_MAPPING[event_type]
        event_data = data_fetcher(message_body)
        # Identifier fields are coming from message_body
        identifier_fields = self.datasets[dataset_id].get_table_by_id(table_id).identifier
        id_value = ".".join(str(event_data[fn]) for fn in identifier_fields)

        pass

    def process_message(
        self,
        message_key: str,
        message_headers: Dict[str, Any],
        message_body: Dict[str, Any],
        current_blob_value: Optional[Dict[str, Any]] = None,
    ):

        event_type = message_headers["event_type"]
        dataset_id = message_headers["catalog"]
        table_id = message_headers["collection"]
        data_fetcher = EVENT_TYPE_MAPPING[event_type]
        event_data = data_fetcher(message_body)
        if event_type == "ADD":
            current_blob = self.blob_maker.fetch_empty_blob(dataset_id, table_id)
            # Get field that make the identifier from schema
            identifier = self.datasets[dataset_id].get_table_by_id(table_id).identifier
            id_value = ".".join(str(event_data[fn]) for fn in identifier)
            key = f"{dataset_id}.{table_id}.{id_value}"
        else:
            assert (
                current_blob_value is not None
            ), "For MODIFY events, we need a current_blob_value"
            current_blob = Blob(message_key, dataset_id, table_id, fields=current_blob_value)

        for field_name in self.geo_fields[dataset_id][table_id]:
            geo_value = event_data.get(field_name)
            if geo_value is not None:
                # XXX quick hack
                # there is something in geoserver, but importing it only for one
                # class is a bit too much.
                srid = self.datasets[dataset_id]["crs"].split(":")[1]
                event_data[field_name] = f"SRID={srid};{geo_value}"
        return self.blob_maker.fetch_updated_blob(current_blob, event_data)

    def process_relation(
        self,
        message_key: str,
        message_headers: Dict[str, Any],
        message_body: Dict[str, Any],
        current_blob_value: Optional[Dict[str, Any]] = None,
    ):
        pass

    def fetch_blob(self, message_key, message_headers, message_body, current_blob_value=None):
        """ Return new or updated blob, depending on the type of event """
        is_relation = message_headers["catalog"] == "rel"
        return self.process_message(message_key, message_headers, message_body, current_blob_value)
