from schematools.types import SchemaType, DatasetSchema
from schematools.importer.base import BaseImporter
from schematools.cli import _get_engine
import environ
from sqlalchemy import MetaData, create_engine, inspect

env = environ.Env()


def test_index_creation():
    """Prove that identifier index is created based on schema specificiation """

    test_data = {
        "type": "dataset",
        "id": "TEST",
        "title": "test table",
        "status": "beschikbaar",
        "description": "test table",
        "version": "0.0.1",
        "crs": "EPSG:28992",
        "tables": [
            {
                "id": "test",
                "type": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema", "id"],
                    "display": "id",
                    "identifier": ["col1", "col2"],
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "integer"},
                        "geometry": {
                            "$ref": "https://geojson.org/schema/Geometry.json"
                        },
                        "col1": {"type": "string"},
                        "col2": {"type": "string"},
                        "col3": {"type": "string"},
                    },
                },
            }
        ],
    }

    data = test_data
    ind_index_exists = False
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    DATABASE_URL = env("DATABASE_URL", None)
    db_url = DATABASE_URL
    engine = _get_engine(db_url)

    for table in data["tables"]:
        importer = BaseImporter(dataset_schema, engine)
        # the generate_table and create index
        importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

        CONN = create_engine(DATABASE_URL, client_encoding="UTF-8")
        META_DATA = MetaData(bind=CONN, reflect=True)
        metadata_inspector = inspect(META_DATA.bind)
        indexes = metadata_inspector.get_indexes(
            f"{parent_schema['id']}_{table['id']}", schema=None
        )
        indexes_name = []

        for index in indexes:
            indexes_name.append(index["name"])
        if any("identifier_idx" in i for i in indexes_name):
            ind_index_exists = True
        assert ind_index_exists


def test_index_troughtables_creation():
    """Prove that many-to-many table indexes are created based on schema specificiation """

    test_data = {
        "type": "dataset",
        "id": "TEST",
        "title": "TEST",
        "status": "niet_beschikbaar",
        "version": "0.0.1",
        "crs": "EPSG:28992",
        "identifier": "identificatie",
        "temporal": {
            "identifier": "volgnummer",
            "dimensions": {"geldigOp": ["beginGeldigheid", "eindGeldigheid"]},
        },
        "tables": [
            {
                "id": "test_1",
                "type": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "identifier": ["identificatie", "volgnummer"],
                    "required": ["schema", "id", "identificatie", "volgnummer"],
                    "display": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "identificatie": {"type": "string"},
                        "volgnummer": {"type": "integer"},
                        "beginGeldigheid": {"type": "string", "format": "date-time"},
                        "eindGeldigheid": {"type": "string", "format": "date-time"},
                        "heeftOnderzoeken": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "identificatie": {"type": "string"},
                                    "volgnummer": {"type": "integer"},
                                },
                            },
                            "relation": "TEST:test_2",
                        },
                    },
                    "mainGeometry": "geometrie",
                },
            },
            {
                "id": "test_2",
                "type": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "identifier": ["identificatie", "volgnummer"],
                    "required": ["schema", "id", "identificatie", "volgnummer"],
                    "display": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "identificatie": {"type": "string"},
                        "volgnummer": {"type": "integer"},
                        "beginGeldigheid": {"type": "string", "format": "date-time"},
                        "eindGeldigheid": {"type": "string", "format": "date-time"},
                    },
                },
            },
        ],
    }

    data = test_data
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    DATABASE_URL = env("DATABASE_URL", None)
    db_url = DATABASE_URL
    engine = _get_engine(db_url)
    indexes_name = []

    for table in data["tables"]:

        importer = BaseImporter(dataset_schema, engine)
        # the generate_table and create index
        importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

    for table in data["tables"]:

        dataset_table = dataset_schema.get_table_by_id(table["id"])

        for table in dataset_table.get_through_tables_by_id():

            CONN = create_engine(DATABASE_URL, client_encoding="UTF-8")
            META_DATA = MetaData(bind=CONN, reflect=True)
            metadata_inspector = inspect(META_DATA.bind)
            indexes = metadata_inspector.get_indexes(
                f"{parent_schema['id']}_{table['id']}", schema=None
            )

            for index in indexes:
                indexes_name.append(index["name"])

    number_of_indexes = len(indexes_name)

    # Many-to-many tables must have at least one index
    assert number_of_indexes > 0
