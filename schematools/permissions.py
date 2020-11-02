from pg_grant import PgObjectType, parse_acl_item, query
from pg_grant.sql import grant, revoke
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from .utils import to_snake_case

PUBLIC_SCOPE = "OPENBAAR"

existing_roles = set()


def introspect_permissions(engine, role):
    schema_relation_infolist = query.get_all_table_acls(engine, schema="public")
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    print(
                        'role "{}" has priviliges {} on table "{}"'.format(
                            role, ",".join(acl.privs), schema_relation_info.name
                        )
                    )


def revoke_permissions(engine, role):
    grantee = role
    schema_relation_infolist = query.get_all_table_acls(engine, schema="public")
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    print(
                        'revoking ALL priviliges of role "{}" on table "{}"'.format(
                            role, schema_relation_info.name
                        )
                    )
                    revoke_statement = revoke(
                        "ALL", PgObjectType.TABLE, schema_relation_info.name, grantee
                    )
                    engine.execute(revoke_statement)


def apply_schema_and_profile_permissions(
    engine, ams_schema, profiles, role, scope, dry_run=False, create_roles=False
):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        if ams_schema:
            create_acl_from_schemas(
                session, ams_schema, role, scope, dry_run, create_roles
            )
        if profiles:
            profile_list = profiles.values()
            create_acl_from_profiles(engine, "public", profile_list, role, scope)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def create_acl_from_profiles(engine, schema, profile_list, role, scope):
    acl_list = query.get_all_table_acls(engine, schema="public")
    priviliges = [
        "SELECT",
    ]
    grantee = role
    for profile in profile_list:
        if scope in profile["scopes"]:
            for dataset, details in profile["schema_data"]["datasets"].items():
                for item in acl_list:
                    if item.name.startswith(dataset + "_"):
                        grant_statement = grant(
                            priviliges,
                            PgObjectType.TABLE,
                            item.name,
                            grantee,
                            grant_option=False,
                            schema=schema,
                        )
                        print(grant_statement)
                        engine.execute(grant_statement)


def create_acl_from_schema(session, ams_schema, role, scope, dry_run, create_roles):
    grantee = role if role != "AUTO" else None
    if create_roles and grantee:
        _create_role_if_not_exists(session, grantee)
    dataset_scope = (
        ams_schema.auth
        if ams_schema.auth
        else {
            PUBLIC_SCOPE,
        }
    )
    dataset_scope_set = (
        {dataset_scope} if isinstance(dataset_scope, str) else set(dataset_scope)
    )

    if dataset_scope_set - {PUBLIC_SCOPE}:
        print(
            'Found dataset read permission for "{}" to scopes "{}"'.format(
                ams_schema.id, dataset_scope_set
            )
        )
    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = "{}_{}".format(
            table.dataset.id, to_snake_case(table.id)
        )  # een aantal table.id's zijn camelcase
        table_scope = table.auth if table.auth else dataset_scope
        table_scope_set = (
            {table_scope} if isinstance(table_scope, str) else set(table_scope)
        )
        if table.auth:
            print(
                'Found table read permission for "{}" to scopes "{}"'.format(
                    table_name, table_scope_set
                )
            )
            print(
                '"{}" overrules "{}" for read permission of "{}"'.format(
                    table_scope_set, dataset_scope_set, table_name
                )
            )
        contains_field_grants = False
        fields = [field for field in table.fields if field.name != "schema"]
        for field in fields:
            if field.auth:
                field_scope = field.auth
                field_scope_set = (
                    {field_scope} if isinstance(field_scope, str) else set(field_scope)
                )
                print(
                    'Found field read permission for "{}" in table "{}" for scopes {}'.format(
                        field.name, table_name, field_scope_set
                    )
                )
                contains_field_grants = True
                print(
                    '"{}" overrules "{}" for read permission of field {} in table {}"'.format(
                        field_scope_set, table_scope_set, field.name, table_name
                    )
                )
                grantees = (
                    [scope_to_role(scope) for scope in field_scope_set]
                    if role == "AUTO"
                    else [role]
                    if scope in field_scope_set
                    else []
                )
                for grantee in grantees:
                    if create_roles:
                        _create_role_if_not_exists(session, grantee, dry_run=dry_run)
                    column_name = to_snake_case(field.name)
                    column_priviliges = [
                        "SELECT ({})".format(column_name),
                    ]  # the space after SELECT is very important
                    _execute_grant(
                        session,
                        grant(
                            column_priviliges,
                            PgObjectType.TABLE,
                            table_name,
                            grantee,
                            grant_option=False,
                            schema="public",
                        ),
                        dry_run=dry_run,
                    )
        if contains_field_grants:
            #  only grant those fields without their own scope. The other field have already been granted above
            grantees = (
                [scope_to_role(scope) for scope in table_scope_set]
                if role == "AUTO"
                else [role]
                if scope in table_scope_set
                else []
            )
            for grantee in grantees:
                if create_roles:
                    _create_role_if_not_exists(session, grantee, dry_run=dry_run)
                for field in fields:
                    if not field.auth:
                        column_name = to_snake_case(field.name)
                        column_priviliges = [
                            "SELECT ({})".format(column_name),
                        ]  # the space after SELECT is very important
                        _execute_grant(
                            session,
                            grant(
                                column_priviliges,
                                PgObjectType.TABLE,
                                table_name,
                                grantee,
                                grant_option=False,
                                schema="public",
                            ),
                            dry_run=dry_run,
                        )
        else:
            # we can grant the whole table instead of field by field
            grantees = (
                [scope_to_role(scope) for scope in table_scope_set]
                if role == "AUTO"
                else [role]
                if scope in table_scope_set
                else []
            )
            for grantee in grantees:
                if create_roles:
                    _create_role_if_not_exists(session, grantee, dry_run=dry_run)
                table_priviliges = [
                    "SELECT",
                ]
                _execute_grant(
                    session,
                    grant(
                        table_priviliges,
                        PgObjectType.TABLE,
                        table_name,
                        grantee,
                        grant_option=False,
                        schema="public",
                    ),
                    dry_run=dry_run,
                )


def create_acl_from_schemas(session, schemas, role, scopes, dry_run, create_roles):
    #  acl_list = query.get_all_table_acls(engine, schema='public')
    #  acl_table_list = [item.name for item in acl_list]
    #  table_names = list()

    _revoke_all_priviliges_from_scope_roles(session, dry_run=dry_run)
    for dataset_name, dataset_schema in schemas.items():
        create_acl_from_schema(
            session, dataset_schema, role, scopes, dry_run, create_roles
        )


def _revoke_all_priviliges_from_scope_roles(session, echo=True, dry_run=False):
    status_msg = "Skipped" if dry_run else "Executed"
    # with engine.begin() as connection:
    result = session.execute(
        text(f"SELECT rolname FROM pg_roles WHERE rolname LIKE 'scope\_%'")
    )
    for rolname in result:
        revoke_statement = (
            f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {rolname[0]};"
        )
        if echo:
            print(f"{status_msg} --> {revoke_statement}")
        if not dry_run:
            session.execute(
                text(revoke_statement)
            )  # .execution_options(autocommit=True))


def _execute_grant(session, grant_statement, echo=True, dry_run=False):
    #  wrap the grant statement in an anonymous code block to catch reasonable exceptions
    #  we don't want to break out the session just because a table or column doesn't exist yet or anymore.
    status_msg = "Skipped" if dry_run else "Executed"
    sql_statement = (
        "DO $$ "
        "BEGIN "
        f"{grant_statement}; "
        "EXCEPTION WHEN undefined_table OR undefined_column THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE; "
        "END "
        "$$"
    )
    if echo:
        print(f"{status_msg} --> {grant_statement}")
    if not dry_run:
        session.execute(sql_statement)


def _create_role_if_not_exists(session, role, echo=True, dry_run=False):
    # wrap the create role statement in an anonymous code block to be able to catch exception
    # we don't want to break out of the session just because the role already exists
    status_msg = "Skipped" if dry_run else "Executed"
    create_role_statement = f"CREATE ROLE {role}"
    if role not in existing_roles:
        sql_statement = (
            "DO $$ "
            "BEGIN "
            f"{create_role_statement}; "
            "EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE; "
            "END "
            "$$"
        )
        if echo:
            print(f"{status_msg} --> {create_role_statement}")
        if not dry_run:
            session.execute(text(sql_statement))
        existing_roles.add(role)


def scope_to_role(scope):
    return f"scope_{scope.lower().replace('/','_')}"
