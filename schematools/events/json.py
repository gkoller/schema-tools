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

from dataclasses import dataclass, field
from ..types import DatasetSchema
from typing import List, Dict

# Maybe
EMPTY_VALUES = {"string": "", "number": None, "integer": None}


@dataclass
class Blob:
    dataset_id: str
    table_id: str
    fields: dict = field(default_factory=dict)


class BlobMaker:
    def __init__(
        self,
        datasets: List[DatasetSchema],
    ):
        self.datasets: Dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}

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
        return Blob(dataset_id, table_id, fields_dict)

    def update_blob(self, blob: Blob, field_updates: dict):
        # XXX Quite naive approach for now, should be much more robust
        blob.fields.update(field_updates)
