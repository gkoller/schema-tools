from collections import defaultdict
from dataclasses import dataclass, field
from ..types import DatasetSchema
from typing import List, Dict, Any, Optional
from .fetchers import fetch_insert_data, fetch_update_data

# Maybe
EMPTY_VALUES = {"string": "", "number": None, "integer": None}
EVENT_TYPE_MAPPING = {
    "ADD": fetch_insert_data,
    "MODIFY": fetch_update_data,
    "DELETE": fetch_insert_data,
}


@dataclass
class Blob:
    key: str
    event_type: str
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
        return Blob(None, None, dataset_id, table_id, fields_dict)

    def fetch_updated_blob(self, blob: Blob, event_type: str, field_updates: dict) -> None:
        # Immutable approach
        return Blob(
            blob.key,
            event_type,
            blob.dataset_id,
            blob.table_id,
            fields={**(blob.fields), **field_updates},
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

    def fetch_key(self, message_headers):
        # For now, we need source_id, catalog and collection
        # Get fields that are part of the identifier from schema
        # identifier = self.datasets[dataset_id].get_table_by_id(table_id).identifier
        identifier = ["source_id"]  # temporary situation
        dataset_id = message_headers["catalog"]
        table_id = message_headers["collection"]
        id_value = ".".join(str(message_headers[fn]) for fn in identifier)
        return f"{dataset_id}.{table_id}.{id_value}"

    def process_message(
        self,
        key: str,
        message_headers: Dict[str, Any],
        message_body: Dict[str, Any],
        current_event: Optional[dict] = None,
    ):

        event_type = message_headers["event_type"]
        dataset_id = message_headers["catalog"]
        table_id = message_headers["collection"]
        # Short circuit for DELETE event
        if event_type == "DELETE":
            return Blob(key, event_type, dataset_id, table_id, None)
        data_fetcher = EVENT_TYPE_MAPPING[event_type]
        event_data = data_fetcher(message_body)

        if event_type == "ADD":
            current_blob = self.blob_maker.fetch_empty_blob(dataset_id, table_id)
        else:
            assert current_event is not None, "For MODIFY events, we need a current_event"
            current_blob = Blob(key, event_type, dataset_id, table_id, fields=current_event)

        for field_name in self.geo_fields[dataset_id][table_id]:
            geo_value = event_data.get(field_name)
            if geo_value is not None:
                # XXX quick hack
                # there is something in geoserver, but importing it only for one
                # class is a bit too much.
                srid = self.datasets[dataset_id]["crs"].split(":")[1]
                event_data[field_name] = f"SRID={srid};{geo_value}"
        return self.blob_maker.fetch_updated_blob(current_blob, event_type, event_data)

    def process_relation(
        self,
        key: str,
        message_headers: Dict[str, Any],
        message_body: Dict[str, Any],
        current_event: Optional[dict] = None,
    ):
        pass
        # Moet Blob maken: key -> representeert volledige event
        # event_data:
        # identificatie -> src_id
        # volgnummer -> src_volgnummer
        # id -> src_id.src_volgnummer
        # <naam-rel-veld>_identificatie -> dst_id
        # <naam-rel-veld>_volgnummer -> dst_volgnummer
        # <naam-rel-veld>_id -> dst_id.dst_volgnummer
        # begin_geldigheid
        # eind_geldigheid

        # dst_dataset_id
        # dst_table_id

    def fetch_event_data(
        self,
        key,
        message_headers,
        message_body,
        current_event: Optional[dict] = None,
    ):
        """ Return new or updated blob, depending on the type of event """
        if message_headers["catalog"] == "rel":
            return self.process_relation(key, message_headers, message_body, current_event)

        return self.process_message(key, message_headers, message_body, current_event)
