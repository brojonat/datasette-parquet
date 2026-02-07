from datasette import hookimpl

PLUGIN_NAME = 'datasette-parquet'

@hookimpl
def startup(datasette):
    config = datasette.plugin_config(
        PLUGIN_NAME
    )

    if not config:
        return

    from .ducky import DuckDatabase
    from .patches import monkey_patch

    monkey_patch()

    # DuckDB is stricter than SQLite about GROUP BY semantics, which causes
    # Datasette's auto-generated facet suggestion queries to fail. Disable
    # suggest_facets unless the user has explicitly set it.
    if datasette.setting("suggest_facets") is True:
        datasette._settings["suggest_facets"] = False

    for db_name, options in config.items():
        if not 'directory' in options and not 'file' in options:
            raise Exception('datasette-parquet: expected directory or file key for db {}'.format(db))

        if 'directory' in options:
            directory = options['directory']
            db = DuckDatabase(datasette, directory=directory, watch=options.get('watch', False) == True)
            datasette.add_database(db, db_name)
        else:
            file = options['file']
            db = DuckDatabase(datasette, file=file)
            datasette.add_database(db, db_name)

