from dataclasses import dataclass

# Tranlation of GOB abbreviated names to amsterdam schema names
DATASET_LOOKUP = {"gbd": "gebieden"}
TABLE_LOOKUP = {"bbk": "bouwblokken", "brt": "buurten", "sdl": "stadsdelen", "wijk": "wijken"}


@dataclass
class SchemaInfo:
    dataset_id: str
    table_id: str
    nm_table_id: str
    relation_fieldname: str
    use_dimension_fields: bool = False

    @classmethod
    def from_collection(cls, collection, datasets):
        pass

        # Based on the collection, create the info needed
        # to be able to generate a Blob that has all the needed info
        # To update relation later on in the chain

        # try:
        #     schema_data = COLLECTION_TO_SCHEMA[collection]
        # except KeyError as e:
        #     raise UnknownRelationException(f"Relation {collection} cannot be handled") from e
        # schema_info = cls(*schema_data)
        # schema_info.use_dimension_fields = datasets[schema_info.dataset_id].use_dimension_fields
        # return schema_info
