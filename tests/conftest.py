from __future__ import annotations

import json
import os
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, ContextManager, Dict

import pytest
import sqlalchemy_utils
from more_ds.network.url import URL
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import scoped_session, sessionmaker

from schematools.types import DatasetSchema, Json, ProfileSchema

HERE = Path(__file__).parent


@pytest.fixture(scope="session")
def db_url(worker_id):
    database_url = os.environ.get("DATABASE_URL", "postgresql://localhost/schematools")
    url = make_url(database_url)
    if worker_id == "master":
        # pytest is being run without any workers
        url.database = f"test_{url.database}"
    else:
        url.database = f"test_{url.database}_{worker_id}"
    return str(url)


@pytest.fixture(scope="session")
def engine(db_url):
    _engine = create_engine(db_url)

    if not sqlalchemy_utils.functions.database_exists(_engine.url):
        sqlalchemy_utils.functions.create_database(_engine.url)

    with closing(_engine.connect()) as conn:
        conn.execute("COMMIT;")
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    try:
        yield _engine
    finally:
        sqlalchemy_utils.functions.drop_database(_engine.url)


@pytest.fixture
def connection(engine):
    with closing(engine.connect()) as conn:
        transaction = conn.begin()
        try:
            yield conn
        finally:
            print("Rollback")
            transaction.rollback()
            print("Rollback done")


@pytest.fixture
def metadata(engine):
    _metadata = MetaData(bind=engine)
    try:
        yield _metadata
    finally:
        print("Drop")
        _metadata.drop_all()
        print("Drop done")


@pytest.fixture(scope="session")
def session_factory(engine):
    return scoped_session(sessionmaker(bind=engine))


@pytest.fixture
def session(session_factory):
    _session = session_factory()

    try:
        yield _session
    finally:
        _session.rollback()
        _session.close()


@pytest.fixture
def meta(engine):
    _meta = MetaData()
    try:
        yield _meta
    finally:
        _meta.drop_all(bind=engine)
        _meta.clear()


@pytest.fixture()
def here() -> Path:
    return HERE


@pytest.fixture(scope="session")
def salogger():
    """Enable logging for sqlalchemy, useful for debugging."""
    import logging

    logging.basicConfig()
    logger = logging.getLogger("sqlalchemy.engine")
    logger.setLevel(logging.DEBUG)


class DummyResponse:
    """Class that mimicks requests.Response."""

    def __init__(self, content: Json):
        self.content = content

    def json(self) -> Json:
        return self.content

    def raise_for_status(self) -> None:
        pass


class DummySession:
    """Class that mimicks requests.Session."""

    def __init__(self, maker: DummySessionMaker):
        self.maker = maker

    def get(self, url: URL) -> DummyResponse:
        content = self.maker.fetch_content_for(url)
        return DummyResponse(content)


class DummySessionMaker:
    """Helper fixture that can produce a contextmanager.

    This helper can be configured with several routes.
    These routes will be mocked with a predefined json response.
    This helper class is a callable and return a contextmanager
    that mimicks `requests.Session`.
    """

    def __init__(self):
        self.routes: Dict[URL, Json] = {}

    def add_route(self, path: URL, content: Json) -> None:
        self.routes[path] = content

    def fetch_content_for(self, url: URL) -> Json:
        return self.routes[url]

    def __call__(self) -> ContextManager[None]:
        @contextmanager
        def dummy_session() -> ContextManager[None]:
            yield DummySession(self)

        return dummy_session()


@pytest.fixture()
def schemas_mock(schema_url: URL, monkeypatch: Any) -> DummySessionMaker:
    """Mock the requests to import schemas.

    This allows to run "schema import schema afvalwegingen".
    """

    from schematools.utils import requests

    dummy_session_maker = DummySessionMaker()

    AFVALWEGINGEN_JSON = HERE / "files" / "afvalwegingen_sep_table.json"
    CLUSTERS_JSON = HERE / "files" / "afvalwegingen_clusters-table.json"
    BAGGOB_JSON = HERE / "files" / "verblijfsobjecten.json"

    monkeypatch.setattr(requests, "Session", dummy_session_maker)

    dummy_session_maker.add_route(
        schema_url / "index.json", {"afvalwegingen": "afvalwegingen", "bag": "bag"}
    )

    with open(AFVALWEGINGEN_JSON, "rb") as fh:
        dummy_session_maker.add_route(
            schema_url / "afvalwegingen/dataset",
            content=json.load(fh),
        )

    with open(BAGGOB_JSON, "rb") as fh:
        dummy_session_maker.add_route(
            schema_url / "bag/dataset",
            content=json.load(fh),
        )

    with open(CLUSTERS_JSON, "rb") as fh:
        dummy_session_maker.add_route(
            schema_url / "afvalwegingen/afvalwegingen_clusters-table",
            content=json.load(fh),
        )
    yield dummy_session_maker


@pytest.fixture()
def afval_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/afval.json")


@pytest.fixture()
def meetbouten_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/meetbouten.json")


@pytest.fixture()
def parkeervakken_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/parkeervakken.json")


@pytest.fixture()
def gebieden_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/gebieden.json")


@pytest.fixture()
def bouwblokken_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/bouwblokken.json")


@pytest.fixture()
def gebieden_schema_auth(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/gebieden_auth.json")


@pytest.fixture()
def gebieden_schema_auth_list(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/gebieden_auth_list.json")


@pytest.fixture()
def ggwgebieden_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/ggwgebieden.json")


@pytest.fixture()
def stadsdelen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/stadsdelen.json")


@pytest.fixture()
def verblijfsobjecten_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/verblijfsobjecten.json")


@pytest.fixture()
def kadastraleobjecten_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/kadastraleobjecten.json")


@pytest.fixture()
def meldingen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/meldingen.json")


@pytest.fixture()
def woonplaatsen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/woonplaatsen.json")


@pytest.fixture()
def woningbouwplannen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/woningbouwplannen.json")


@pytest.fixture()
def brp_r_profile_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return ProfileSchema.from_file(here / "files/profiles/BRP_R.json")


@pytest.fixture()
def profile_brk_encoded_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return ProfileSchema.from_file(here / "files/profiles/BRK_encoded.json")


@pytest.fixture()
def profile_brk_read_id_schema(here) -> ProfileSchema:
    return ProfileSchema.from_file(here / "files/profiles/BRK_RID.json")


@pytest.fixture
def profile_verkeer_medewerker_schema() -> ProfileSchema:
    return ProfileSchema.from_dict(
        {
            "name": "verkeer_medewerker",
            "scopes": ["FP/MD"],
            "datasets": {
                "verkeer": {},  # needed to be applied to a dataset.
            },
        }
    )


@pytest.fixture()
def brk_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/brk.json")


@pytest.fixture()
def hr_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/hr.json")
