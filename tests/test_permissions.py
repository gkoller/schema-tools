import json

import pytest
from psycopg2.errors import DuplicateObject
from sqlalchemy.exc import ProgrammingError

from schematools.importer.ndjson import NDJSONImporter
from schematools.permissions.db import apply_schema_and_profile_permissions


class TestReadPermissions:
    def test_auto_permissions(self, here, engine, gebieden_schema_auth, dbsession):
        """
        Prove that roles are automatically created for each scope in the schema
        LEVEL/A --> scope_level_a
        LEVEL/B --> scope_level_b
        LEVEL/C --> scope_level_c
        """
        ndjson_path = here / "files" / "data" / "gebieden.ndjson"
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}
        profile_path = here / "files" / "profiles" / "gebieden_test.json"
        with open(profile_path) as f:
            profile = json.load(f)
        profiles = {profile["name"]: profile}

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "AUTO", "ALL", create_roles=True
        )
        _check_select_permission_granted(engine, "scope_level_a", "gebieden_buurten")
        _check_select_permission_granted(
            engine, "scope_level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_c", "gebieden_bouwblokken", "begin_geldigheid"
        )

    def test_openbaar_permissions(self, here, engine, afval_schema, dbsession):
        """
        Prove that the default auth scope is "OPENBAAR".
        """

        importer = NDJSONImporter(afval_schema, engine)
        importer.generate_db_objects("containers", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("clusters", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {afval_schema.id: afval_schema}
        profile_path = here / "files" / "profiles" / "gebieden_test.json"
        with open(profile_path) as f:
            profile = json.load(f)
        profiles = {profile["name"]: profile}

        # Create postgres roles
        _create_role(engine, "openbaar")
        _create_role(engine, "bag_r")
        # Check if the roles exist, the tables exist,
        # and the roles have no read privilige on the tables.
        _check_select_permission_denied(engine, "openbaar", "afvalwegingen_containers")
        _check_select_permission_denied(engine, "bag_r", "afvalwegingen_clusters")

        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "openbaar", "OPENBAAR"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "bag_r", "BAG/R"
        )

        _check_select_permission_granted(engine, "openbaar", "afvalwegingen_containers")
        _check_select_permission_denied(engine, "openbaar", "afvalwegingen_clusters")
        _check_select_permission_denied(engine, "bag_r", "afvalwegingen_containers")
        _check_select_permission_granted(engine, "bag_r", "afvalwegingen_clusters")

    def test_interacting_permissions(self, here, engine, gebieden_schema_auth, dbsession):
        """
        Prove that dataset, table, and field permissions are set
        according to the "OF-OF" Exclusief principle:

        * Een user met scope LEVEL/A mag alles uit de dataset gebieden zien,
          behalve tabel bouwblokken.
        * Een user met scope LEVEL/B mag alle velden van tabel bouwblokken zien,
          behalve beginGeldigheid.
        * Een user met scope LEVEL/C mag veld beginGeldigheid zien.
        """

        ndjson_path = here / "files" / "data" / "gebieden.ndjson"
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}
        profile_path = here / "files" / "profiles" / "gebieden_test.json"
        with open(profile_path) as f:
            profile = json.load(f)
        profiles = {profile["name"]: profile}

        # Create postgres roles
        test_roles = ["level_a", "level_b", "level_c"]
        for test_role in test_roles:
            _create_role(engine, test_role)

        # Check if the roles exist, the tables exist,
        # and the roles have no read privilige on the tables.
        for test_role in test_roles:
            for table in ["gebieden_bouwblokken", "gebieden_buurten"]:
                _check_select_permission_denied(engine, test_role, table)

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_a", "LEVEL/A"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_b", "LEVEL/B"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_c", "LEVEL/C"
        )

        # Check if the read priviliges are correct
        _check_select_permission_denied(engine, "level_a", "gebieden_bouwblokken")
        _check_select_permission_granted(engine, "level_a", "gebieden_buurten")

        _check_select_permission_granted(
            engine, "level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "level_b", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_b", "gebieden_buurten")

        _check_select_permission_denied(
            engine, "level_c", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "level_c", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_c", "gebieden_buurten")

    def test_auth_list_permissions(self, here, engine, gebieden_schema_auth_list, dbsession):
        """
        Prove that dataset, table, and field permissions are set,
        according to the "OF-OF" Exclusief principle.
        Prove that when the auth property is a list of scopes, this is interpreted as "OF-OF".

        * Een user met scope LEVEL/A1 of LEVEL/A2 mag alles uit de dataset gebieden zien,
          behalve tabel bouwblokken.
        * Een user met scope LEVEL/B1 of LEVEL/B2 mag alle velden van tabel bouwblokken zien,
          behalve beginGeldigheid.
        * Een user met scope LEVEL/C1 of LEVEL/B2 mag veld beginGeldigheid zien.
        """

        ndjson_path = here / "files" / "data" / "gebieden.ndjson"
        importer = NDJSONImporter(gebieden_schema_auth_list, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {gebieden_schema_auth_list.id: gebieden_schema_auth_list}
        profile_path = here / "files" / "profiles" / "gebieden_test.json"
        with open(profile_path) as f:
            profile = json.load(f)
        profiles = {profile["name"]: profile}

        # Create postgres roles
        test_roles = [
            "level_a1",
            "level_a2",
            "level_b1",
            "level_b2",
            "level_c1",
            "level_c2",
        ]
        for test_role in test_roles:
            _create_role(engine, test_role)

        # Check if the roles exist, the tables exist,
        # and the roles have no read privilige on the tables.
        for test_role in test_roles:
            for table in ["gebieden_bouwblokken", "gebieden_buurten"]:
                _check_select_permission_denied(engine, test_role, table)

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_a1", "LEVEL/A1"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_b1", "LEVEL/B1"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_c1", "LEVEL/C1"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_a2", "LEVEL/A2"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_b2", "LEVEL/B2"
        )
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "level_c2", "LEVEL/C2"
        )

        # Check if the read priviliges are correct
        _check_select_permission_denied(engine, "level_a1", "gebieden_bouwblokken")
        _check_select_permission_granted(engine, "level_a1", "gebieden_buurten")
        _check_select_permission_denied(engine, "level_a2", "gebieden_bouwblokken")
        _check_select_permission_granted(engine, "level_a2", "gebieden_buurten")

        _check_select_permission_granted(
            engine, "level_b1", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "level_b1", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_b1", "gebieden_buurten")
        _check_select_permission_granted(
            engine, "level_b2", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "level_b2", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_b2", "gebieden_buurten")

        _check_select_permission_denied(
            engine, "level_c1", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "level_c1", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_c1", "gebieden_buurten")

        _check_select_permission_denied(
            engine, "level_c2", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "level_c2", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_c2", "gebieden_buurten")

        # Check that there are no INSERT, UPDATE, TRUNCATE, DELETE privileges
        _check_insert_permission_denied(engine, "level_b1", "gebieden_bouwblokken", "id", "'abc'")
        _check_update_permission_denied(
            engine, "level_b1", "gebieden_bouwblokken", "id", "'def'", "id = 'abc'"
        )
        _check_delete_permission_denied(engine, "level_b1", "gebieden_bouwblokken", "id = 'abc'")
        _check_truncate_permission_denied(engine, "level_b1", "gebieden_bouwblokken")

    def test_auto_create_roles(self, here, engine, gebieden_schema_auth, dbsession):
        """
        Prove that dataset, table, and field permissions are set according,
        to the "OF-OF" Exclusief principle:

        * Een user met scope LEVEL/A mag alles uit de dataset gebieden zien,
          behalve tabel bouwblokken.
        * Een user met scope LEVEL/B mag alle velden van tabel bouwblokken zien,
          behalve beginGeldigheid.
        * Een user met scope LEVEL/C mag veld beginGeldigheid zien.

        Drie corresponderende users worden automatisch aangemaakt:
        'scope_level_a', 'scope_level_b', en 'scope_level_c;
        """

        ndjson_path = here / "files" / "data" / "gebieden.ndjson"
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}
        profile_path = here / "files" / "profiles" / "gebieden_test.json"
        with open(profile_path) as f:
            profile = json.load(f)
        profiles = {profile["name"]: profile}

        # These tests commented out due to: Error when trying to teardown test databases
        # Roles may still exist from previous test run. Uncomment when fixed:
        # _check_role_does_not_exist(engine, "scope_level_a")
        # _check_role_does_not_exist(engine, "scope_level_b")
        # _check_role_does_not_exist(engine, "scope_level_c")

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine, "public", ams_schema, profiles, "AUTO", "ALL", create_roles=True
        )
        # Check if roles exist and the read priviliges are correct
        _check_select_permission_denied(engine, "scope_level_a", "gebieden_bouwblokken")
        _check_select_permission_granted(engine, "scope_level_a", "gebieden_buurten")

        _check_select_permission_granted(
            engine, "scope_level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "scope_level_b", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "scope_level_b", "gebieden_buurten")

        _check_select_permission_denied(
            engine, "scope_level_c", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_c", "gebieden_bouwblokken", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "scope_level_c", "gebieden_buurten")

    def test_single_dataset_permissions(
        self, here, engine, gebieden_schema_auth, meetbouten_schema, dbsession
    ):
        """
        Prove when revoking grants on one dataset, other datasets are unaffected.
        """

        # dataset 1: gebieden
        ndjson_path = here / "files" / "data" / "gebieden.ndjson"
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("wijken", truncate=True, ind_extra_index=False)

        # dataset 2: meetbouten
        ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
        importer = NDJSONImporter(meetbouten_schema, engine)
        importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("referentiepunten", truncate=True, ind_extra_index=False)

        # Apply the permissions to gebieden
        apply_schema_and_profile_permissions(
            engine, "public", gebieden_schema_auth, None, "AUTO", "ALL", create_roles=True
        )
        # Check perms on gebieden
        _check_select_permission_granted(engine, "scope_level_a", "gebieden_buurten")
        _check_select_permission_granted(
            engine, "scope_level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_c", "gebieden_bouwblokken", "begin_geldigheid"
        )

        # Apply the permissions to meetbouten
        apply_schema_and_profile_permissions(
            engine, "public", meetbouten_schema, None, "AUTO", "ALL", create_roles=True
        )
        # Check perms on meetbouten
        _check_select_permission_granted(engine, "scope_openbaar", "meetbouten_meetbouten")

        # Revoke permissions for dataset gebieden and set grant again
        apply_schema_and_profile_permissions(
            engine,
            pg_schema="public",
            ams_schema=gebieden_schema_auth,
            profiles=None,
            role="AUTO",
            scope="ALL",
            create_roles=True,
            revoke=True,
        )
        # Check perms again on meetbouten
        _check_select_permission_granted(engine, "scope_openbaar", "meetbouten_meetbouten")


class TestWritePermissions:
    def test_dataset_write_role(self, here, engine, gebieden_schema_auth):
        """
        Prove that a write role with name write_{dataset.id} is created with DML rights
        Check INSERT, UPDATE, DELETE, TRUNCATE permissions
        Check that for SELECT permissions you need an additional scope role.
        """

        ndjson_path = here / "files" / "data" / "gebieden.ndjson"
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Setup schema
        ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}

        # The write_ roles do not have SELECT permissions
        _check_insert_permission_denied(
            engine, "write_gebieden", "gebieden_bouwblokken", "id", "'abc'"
        )

        apply_schema_and_profile_permissions(
            engine=engine,
            pg_schema="public",
            ams_schema=ams_schema,
            profiles=None,
            role="AUTO",
            scope="ALL",
            set_read_permissions=True,
            set_write_permissions=True,
            create_roles=True,
            revoke=True,
        )

        # Drop testuser in case previous tests did not terminate correctly
        with engine.begin() as connection:
            connection.execute("DROP ROLE IF EXISTS testuser")

        _create_role(engine, "testuser")

        with engine.begin() as connection:
            connection.execute("GRANT write_gebieden TO testuser")

        #  It is now possible to INSERT data into the dataset tables
        _check_insert_permission_granted(engine, "testuser", "gebieden_bouwblokken", "id", "'abc'")

        #  The write_ roles do not have SELECT permissions, therefore testuser should not have it
        _check_select_permission_denied(engine, "testuser", "gebieden_bouwblokken")

        #  Without SELECT it is not possible to UPDATE or DELETE on given condition
        _check_update_permission_denied(
            engine, "testuser", "gebieden_bouwblokken", "id", "'def'", "id = 'abc'"
        )
        _check_delete_permission_denied(engine, "testuser", "gebieden_bouwblokken", "id = 'abc'")

        # Add SELECT permissions by granting the appropriate scope to the user
        with engine.begin() as connection:
            connection.execute("GRANT scope_level_b TO testuser")

        # Still not possible to SELECT * because not all columns are within scope
        _check_select_permission_denied(engine, "testuser", "gebieden_bouwblokken", "*")

        # But now it's possible to SELECT the columns within scope level_b
        _check_select_permission_granted(
            engine, "testuser", "gebieden_bouwblokken", "id, eind_geldigheid"
        )

        # And it's also possible to UPDATE and DELETE,
        # if the column for the condition is within scope
        _check_update_permission_granted(
            engine, "testuser", "gebieden_bouwblokken", "id", "'def'", "id = 'abc'"
        )
        _check_delete_permission_granted(engine, "testuser", "gebieden_bouwblokken", "id = 'def'")

        # TRUNCATE is also allowed, even though the table is already empty by now
        _check_truncate_permission_granted(engine, "testuser", "gebieden_bouwblokken")

    def test_multiple_datasets_write_roles(self, here, engine, parkeervakken_schema, afval_schema):
        """
        Prove that the write_{dataset.id} roles only have DML rights for their associated
        dataset tables.
        """

        importer = NDJSONImporter(parkeervakken_schema, engine)
        importer.generate_db_objects("parkeervakken", truncate=True, ind_extra_index=False)
        importer = NDJSONImporter(afval_schema, engine)
        importer.generate_db_objects("containers", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("clusters", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {afval_schema.id: afval_schema, parkeervakken_schema.id: parkeervakken_schema}

        apply_schema_and_profile_permissions(
            engine=engine,
            pg_schema="public",
            ams_schema=ams_schema,
            profiles=None,
            role="AUTO",
            scope="ALL",
            set_read_permissions=True,
            set_write_permissions=True,
            create_roles=True,
            revoke=True,
        )

        # Drop testuser in case previous tests did not terminate correctly
        with engine.begin() as connection:
            connection.execute("DROP ROLE IF EXISTS parkeer_tester")
            connection.execute("DROP ROLE IF EXISTS afval_tester")

        _create_role(engine, "parkeer_tester")
        _create_role(engine, "afval_tester")

        with engine.begin() as connection:
            connection.execute("GRANT write_parkeervakken TO parkeer_tester")
            connection.execute("GRANT write_afvalwegingen TO afval_tester")

        #  parkeer_tester has INSERT permission on parkeervakken datasets
        _check_insert_permission_granted(
            engine, "parkeer_tester", "parkeervakken_parkeervakken", "id", "'abc'"
        )
        #  afval_tester has INSERT permission on afvalwegingen datasets
        _check_insert_permission_granted(
            engine, "afval_tester", "afvalwegingen_containers", "id", "3"
        )
        #  parkeer_tester has NO INSERT permission on afvalwegingen datasets
        _check_insert_permission_denied(
            engine, "parkeer_tester", "afvalwegingen_containers", "id", "3"
        )
        #  afval_tester has NO INSERT permission on parkeervakken datasets
        _check_insert_permission_denied(
            engine, "afval_tester", "parkeervakken_parkeervakken", "id", "'abc'"
        )


def _create_role(engine, role):
    """Create role. If role already exists just fail and ignore.
    This may happen if a previous pytest did not terminate correctly.
    """
    try:
        engine.execute('CREATE ROLE "{}"'.format(role))
    except ProgrammingError as e:
        if not isinstance(e.orig, DuplicateObject):
            raise


def _check_role_does_not_exist(engine, role):
    """Check if role does not exist"""
    with engine.begin() as connection:
        result = connection.execute(f"SELECT rolname FROM pg_roles WHERE rolname='{role}'")
        rows = list(result)
        assert len(rows) == 0


def _check_select_permission_denied(engine, role, table, column="*"):
    """Check if role has no SELECT permission on table.
    Fail if role, table or column does not exist.
    """
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute("SET ROLE {}".format(role))
            connection.execute("SELECT {} FROM {}".format(column, table))
            connection.execute("RESET ROLE")
    assert "permission denied for table {}".format(table) in str(e_info)


def _check_select_permission_granted(engine, role, table, column="*"):
    """Check if role has SELECT permission on table.
    Fail if role, table or column does not exist.
    """
    with engine.begin() as connection:
        connection.execute("SET ROLE {}".format(role))
        result = connection.execute("SELECT {} FROM {}".format(column, table))
        connection.execute("RESET ROLE")
    assert result


def _check_insert_permission_granted(engine, role, table, column, value):
    """Check if role has INSERT permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype.
    """
    with engine.begin() as connection:
        connection.execute("SET ROLE {}".format(role))
        result = connection.execute("INSERT INTO {} ({}) VALUES ({})".format(table, column, value))
        connection.execute("RESET ROLE")
    assert result


def _check_insert_permission_denied(engine, role, table, column, value):
    """Check if role has no INSERT permission on table.
    Fail if role, table or column does not exist.
    """
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute("SET ROLE {}".format(role))
            connection.execute("INSERT INTO {} ({}) VALUES ({})".format(table, column, value))
            connection.execute("RESET ROLE")
    assert "permission denied for table {}".format(table) in str(e_info)


def _check_update_permission_granted(engine, role, table, column, value, condition):
    """Check if role has UPDATE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype.
    """
    with engine.begin() as connection:
        connection.execute("SET ROLE {}".format(role))
        result = connection.execute(
            "UPDATE {} SET {} =  {} WHERE {}".format(table, column, value, condition)
        )
        connection.execute("RESET ROLE")
    assert result


def _check_update_permission_denied(engine, role, table, column, value, condition):
    """Check if role has no UPDATE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype.
    """
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute("SET ROLE {}".format(role))
            connection.execute(
                "UPDATE {} SET {} =  {} WHERE {}".format(table, column, value, condition)
            )
            connection.execute("RESET ROLE")
    assert "permission denied for table {}".format(table) in str(e_info)


def _check_delete_permission_granted(engine, role, table, condition):
    """Check if role has DELETE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype."""
    with engine.begin() as connection:
        connection.execute("SET ROLE {}".format(role))
        result = connection.execute("DELETE FROM {} WHERE {}".format(table, condition))
        connection.execute("RESET ROLE")
    assert result


def _check_delete_permission_denied(engine, role, table, condition):
    """Check if role has no DELETE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype."""
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute("SET ROLE {}".format(role))
            connection.execute("DELETE FROM {} WHERE {}".format(table, condition))
            connection.execute("RESET ROLE")
    assert "permission denied for table {}".format(table) in str(e_info)


def _check_truncate_permission_granted(engine, role, table):
    """Check if role has TRUNCATE permission on table.
    Fail if role or table does not exist.
    """
    with engine.begin() as connection:
        connection.execute(f"SET ROLE {role}")
        result = connection.execute(f"TRUNCATE {table}")
        connection.execute("RESET ROLE")
    assert result


def _check_truncate_permission_denied(engine, role, table):
    """Check if role has no TRUNCATE permission on table.
    Fail if role or table does not exist.
    """
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute(f"SET ROLE {role}")
            connection.execute(f"TRUNCATE {table}")
            connection.execute("RESET ROLE")
    assert f"permission denied for table {table}" in str(e_info)
