import inspect

from datasette.app import Datasette
from .create_db import create_dbs
import pytest
import duckdb
from datasette_parquet.winging_it import ProxyConnection
from datasette_parquet import exceptions

# Detect whether the installed Datasette supports the config= constructor
# parameter (1.0+). In pre-1.0, plugin config is passed via metadata=.
_DATASETTE_V1 = 'config' in inspect.signature(Datasette.__init__).parameters


def _query_url(db, sql_params, fmt=""):
    """Build a SQL query URL compatible with pre- and post-1.0 Datasette."""
    if _DATASETTE_V1:
        return f"/{db}/-/query{fmt}?{sql_params}"
    return f"/{db}{fmt}?{sql_params}"


@pytest.fixture(scope="session")
def datasette():
    create_dbs('./fixtures')
    plugin_config = {
        'plugins': {
            'datasette-parquet': {
                'trove': {
                    'directory': './fixtures'
                },
                'duckdb': {
                    'file': './fixtures/fixtures.duckdb'
                }

            }
        }
    }

    if _DATASETTE_V1:
        return Datasette(
            [],
            memory=True,
            config=plugin_config,
        )
    else:
        return Datasette(
            [],
            memory=True,
            metadata=plugin_config,
        )

@pytest.mark.asyncio
async def test_plugin_is_installed(datasette):
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-parquet" in installed_plugins

@pytest.mark.asyncio
async def test_file_mode(datasette):
    response = await datasette.client.get('/duckdb')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_directory_mode(datasette):
    response = await datasette.client.get('/trove')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_json_works(datasette):
    response = await datasette.client.get("/trove/fixtures.json?_size=max&_labels=on&_shape=objects")
    assert response.status_code == 200
    assert response.json()['rows'] == [{'date': '2023-01-01', 'ts': '2023-01-02T03:04:05' }]

@pytest.mark.asyncio
async def test_extraneous_parameters(datasette):
    url = _query_url("trove", "sql=select+%2A+from+fixtures&_hide_sql=1")
    response = await datasette.client.get(url)
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_sql_json(datasette):
    url = _query_url("trove", "sql=select+%2A+from+fixtures&_hide_sql=1", fmt=".json")
    response = await datasette.client.get(url)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_duckdb_table(datasette):
    response = await datasette.client.get("/duckdb/fixtures")
    assert response.status_code == 200

def test_fetchone():
    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)
    fetched = conn.execute('SELECT 1 AS col').fetchone()
    assert fetched['col'] == 1


def test_catch_double_quote_usage_for_literal(datasette):

    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)

    # try reading the parquet file in trove/userdata1.parquet
    explodey_string_with_double_quotes = 'SELECT * from "./trove/userdata1.parquet" WHERE first_name = "Amanda"'

    with pytest.raises(exceptions.DoubleQuoteForLiteraValue):
        result = conn.execute(explodey_string_with_double_quotes).fetchall()