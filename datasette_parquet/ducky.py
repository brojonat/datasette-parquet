import asyncio
from collections import defaultdict
import duckdb
import sqlite3
from .debounce import debounce
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler
from datasette.database import Database, Results
from .ddl import create_views
from .winging_it import ProxyConnection

class SchemaEventHandler(FileSystemEventHandler):
    """React to files being added/removed from the watched directory."""

    def __init__(self, reload):
        super().__init__()

        self.reload = reload

    @debounce(1)
    def on_event(self):
        self.reload()

    def on_moved(self, event):
        super().on_moved(event)
        self.on_event()

    def on_created(self, event):
        super().on_created(event)
        self.on_event()

    def on_deleted(self, event):
        super().on_deleted(event)
        self.on_event()

    def on_modified(self, event):
        super().on_modified(event)
        self.on_event()

def create_directory_connection(directory):
    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)

    for create_view_stmt in create_views(directory):
        conn.conn.execute(create_view_stmt)

    return conn

class DuckDatabase(Database):
    def __init__(self, ds, directory=None, file=None, httpfs=None, watch=None):
        super().__init__(ds)

        self.engine = 'duckdb'

        if directory:
            conn = create_directory_connection(directory)

            def reload():
                self.conn.conn.close()
                self.conn = create_directory_connection(directory)

            event_handler = SchemaEventHandler(reload)
            observer = Observer()
            observer.schedule(event_handler, directory, recursive=True)
            observer.start()
        elif file:
            raw_conn = duckdb.connect(file)
            conn = ProxyConnection(raw_conn)
        else:
            raise Exception('must specify directory or file')

        if httpfs:
            conn.conn.execute('install httpfs;').fetchall()
            conn.conn.execute('load httpfs;').fetchall()

        self.conn = conn

    @property
    def size(self):
        # TODO: implement this? Not sure if it's useful.
        return 0

    async def hidden_table_names(self):
        # The base class queries SQLite-specific pragmas (e.g. pragma_table_list)
        # that don't exist in DuckDB. DuckDB/Parquet files have no SQLite shadow
        # tables, FTS indexes, or spatialite internals, so just return empty.
        return []

    async def get_all_foreign_keys(self):
        # DuckDB/Parquet files don't have foreign keys. The base class
        # implementation queries sqlite_master with SQLite-specific syntax
        # that is incompatible with DuckDB. Return a defaultdict so any
        # table lookup yields the expected {"incoming": [], "outgoing": []}
        # structure.
        return defaultdict(lambda: {"incoming": [], "outgoing": []})

    async def execute_fn(self, fn):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            try:
                return fn(self.conn)
            except duckdb.OperationalError as e:
                raise sqlite3.OperationalError(str(e))
            except duckdb.ProgrammingError as e:
                raise sqlite3.ProgrammingError(str(e))
            except duckdb.InternalError as e:
                raise sqlite3.InternalError(str(e))
            except duckdb.DataError as e:
                raise sqlite3.DatabaseError(str(e))

        return await asyncio.get_running_loop().run_in_executor(
            self.ds.executor, in_thread
        )

    async def execute_write_fn(self, fn, block=True):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            try:
                return fn(self.conn)
            except duckdb.OperationalError as e:
                raise sqlite3.OperationalError(str(e))
            except duckdb.ProgrammingError as e:
                raise sqlite3.ProgrammingError(str(e))
            except duckdb.InternalError as e:
                raise sqlite3.InternalError(str(e))
            except duckdb.DataError as e:
                raise sqlite3.DatabaseError(str(e))

        # We lie, we'll always block.
        return await asyncio.get_running_loop().run_in_executor(
            self.ds.executor, in_thread
        )
