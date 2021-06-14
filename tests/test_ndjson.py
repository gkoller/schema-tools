import pytest

from schematools.importer.ndjson import NDJSONImporter


def test_ndjson_import_nm(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "metingen.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_metingen")]
    assert len(records) == 4
    # A non-object relation, should just lead to _id field
    assert "hoortbijmeetbout_id" in records[0]
    # check value from the ndjson input, should be string according to the schema
    assert records[0]["hoortbijmeetbout_id"] == "13881032"
    records = [
        dict(r)
        for r in engine.execute("SELECT * from meetbouten_metingen_refereertaanreferentiepunten")
    ]
    # Should have a field 'identificatie' in the n-m table
    assert "refereertaanreferentiepunten_identificatie" in records[0]


def test_ndjson_import_jsonpath_provenance(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    assert records[0]["merk_code"] == "12"
    assert records[0]["merk_omschrijving"] == "De meetbout"


@pytest.mark.parametrize("use_dimension_fields", (False, True))
def test_ndjson_import_nm_compound_keys(
    here, engine, ggwgebieden_schema, dbsession, use_dimension_fields
):
    ndjson_path = here / "files" / "data" / "ggwgebieden.ndjson"
    ggwgebieden_schema.use_dimension_fields = use_dimension_fields
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden")]
    assert len(records) == 1
    # An "id" should have been generated, concat of the compound key fields
    assert "id" in records[0]
    assert records[0]["id"] == "03630950000000.1"
    records = [
        dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden_bestaatuitbuurten")
    ]
    assert len(records) == 3
    # Also the temporal fields are present in the database
    columns = {
        "ggwgebieden_id",
        "bestaatuitbuurten_id",
        "ggwgebieden_volgnummer",
        "ggwgebieden_identificatie",
        "bestaatuitbuurten_identificatie",
        "bestaatuitbuurten_volgnummer",
    }

    if use_dimension_fields:
        columns |= {
            "begin_geldigheid",
            "eind_geldigheid",
        }

    assert records[0].keys() == columns


def test_ndjson_import_nm_compound_selfreferencing_keys(
    here, engine, kadastraleobjecten_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "kadastraleobjecten.ndjson"
    importer = NDJSONImporter(kadastraleobjecten_schema, engine)
    importer.generate_db_objects("kadastraleobjecten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    records = [dict(r) for r in engine.execute("SELECT * from brk_kadastraleobjecten")]
    assert len(records) == 2
    # An "id" should have been generated, concat of the compound key fields
    assert "id" in records[0]
    assert records[0]["id"] == "KAD.001.1"
    records = [
        dict(r)
        for r in engine.execute(
            "SELECT * from brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject"
        )
    ]
    assert len(records) == 1
    assert sorted((n, v) for n, v in records[0].items()) == (
        [
            ("is_ontstaan_uit_kadastraalobject_id", "KAD.002.1"),
            ("is_ontstaan_uit_kadastraalobject_identificatie", "KAD.002"),
            ("is_ontstaan_uit_kadastraalobject_volgnummer", "1"),
            ("kadastraleobjecten_id", "KAD.001.1"),
            ("kadastraleobjecten_identificatie", "KAD.001"),
            ("kadastraleobjecten_volgnummer", 1),
        ]
    )


def test_ndjson_import_nested_tables(here, engine, verblijfsobjecten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "verblijfsobjecten.ndjson"
    importer = NDJSONImporter(verblijfsobjecten_schema, engine)
    importer.generate_db_objects("verblijfsobjecten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [
        dict(r)
        for r in engine.execute(
            "SELECT code, omschrijving, parent_id FROM baggob_verblijfsobjecten_gebruiksdoel"
        )
    ]
    assert len(records) == 2
    assert sorted((n, v) for n, v in records[0].items()) == (
        [
            ("code", "1"),
            ("omschrijving", "doel 1"),
            ("parent_id", "VB.1"),
        ]
    )


def test_ndjson_import_1n(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    # The foreign key, needed by Django, should be there
    assert "ligtinbuurt_id" in records[0]
    # And should have the concatenated value
    assert records[0]["ligtinbuurt_id"] == "10180001.1"
    # Should have a field identificatie
    assert "ligtinbuurt_identificatie" in records[0]


def test_inactive_relation_that_are_commented_out(here, engine, stadsdelen_schema, dbsession):
    """Prove that relations that are commented out in the schema are flattened to strings"""
    ndjson_path = here / "files" / "data" / "stadsdelen.ndjson"
    importer = NDJSONImporter(stadsdelen_schema, engine)
    importer.generate_db_objects("stadsdelen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_stadsdelen ORDER BY id")]
    # Field is stringified, because in schema the relation is 'disabled'
    assert records[0]["ligt_in_gemeente"] == '{"identificatie": "0363"}'


def test_missing_fields_in_jsonpath_provenance(here, engine, woonplaatsen_schema, dbsession):
    """Prove that missing fields in jsonpath provenance fields do not crash"""
    ndjson_path = here / "files" / "data" / "woonplaatsen.ndjson"
    importer = NDJSONImporter(woonplaatsen_schema, engine)
    importer.generate_db_objects("woonplaatsen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from baggob_woonplaatsen ORDER BY id")]
    assert len(records) == 2
    assert records[1]["status_code"] is None


def test_ndjson_import_with_shortnames_in_schema(
    here, engine, hr_schema, verblijfsobjecten_schema, dbsession
):
    """Prove that data for schemas with shortnames for tables/fields is imported correctly."""
    ndjson_path = here / "files" / "data" / "hr.ndjson"
    importer = NDJSONImporter(hr_schema, engine)
    importer.generate_db_objects(
        "maatschappelijkeactiviteiten", truncate=True, ind_extra_index=False
    )
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from hr_activiteiten")]
    assert len(records) == 1
    assert records[0]["kvknummer"] == "90004213"
    assert records[0]["gevestigd_in_identificatie"] == "01002"
    assert records[0]["gevestigd_in_volgnummer"] == 3
    assert records[0]["gevestigd_in_id"] == "01002.3"

    records = [
        dict(r) for r in engine.execute("SELECT * from hr_activiteiten_sbi_maatschappelijk")
    ]
    assert len(records) == 1
    assert records[0] == {"parent_id": "90004213", "bronwaarde": 1130, "id": 1}

    records = [
        dict(r)
        for r in engine.execute(
            "SELECT * from hr_activiteiten_heeft_sbi_activiteiten_voor_onderneming"
        )
    ]
    assert len(records) == 1
    assert records[0] == {"parent_id": "90004213", "bronwaarde": 1131, "id": 1}

    records = [dict(r) for r in engine.execute("SELECT * from hr_activiteiten_verblijfsobjecten")]
    assert len(records) == 1
    assert records[0] == {
        "activiteiten_id": "90004213",
        "verblijfsobjecten_id": "01001.1",
        "verblijfsobjecten_identificatie": "01001",
        "verblijfsobjecten_volgnummer": 1,
    }


def test_provenance_for_schema_field_ids_equal_to_ndjson_keys(
    here, engine, woonplaatsen_schema, dbsession
):
    """Prove that imports where the schema field is equal to the key in the imported ndjson
    data are processed correctly."""
    ndjson_path = here / "files" / "data" / "woonplaatsen.ndjson"
    importer = NDJSONImporter(woonplaatsen_schema, engine)
    importer.generate_db_objects("woonplaatsen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from baggob_woonplaatsen ORDER BY id")]
    assert len(records) == 2
    assert records[0]["heeft_dossier_id"] == "GV12"
    assert records[1]["heeft_dossier_id"] is None


def test_ndjson_test_long_postfixed_names(
    here, engine, brk_schema, verblijfsobjecten_schema, dbsession
):
    """Prove that very long names with a postfix are trucacted correctly.

    In this case, the table names is just below the threshhold,
    so should not be truncated.
    """
    importer = NDJSONImporter(brk_schema, engine)
    importer.generate_db_objects(
        "aantekeningenkadastraleobjecten",
        db_table_name="brk_aantekeningenkadastraleobjecten_new",
        truncate=True,
        ind_extra_index=False,
    )
    assert (
        "brk_aantekeningenkadastraleobjecten_new_heeft_betrokken_persoon" in importer.tables.keys()
    )
