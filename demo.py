"""Run a demo of the DuckDBController's functions."""

from __future__ import annotations

import logging
from pathlib import Path
import shutil
import time
import typing as t

log = logging.getLogger(__name__)

from red_duck.controllers import DuckDBController

def setup_logging(
    log_level: str = "INFO",
    log_fmt: str = "%(levelname)s | [%(asctime)s] |> %(message)s",
    datefmt: str = "%Y-%m-%d_%H:%M:%S",
    silence_loggers: list[str] = [],
) -> None:
    """Setup logging for the application.

    Usage:
        Somewhere high in your project's execution, ideally any entrypoint like `wsgi.py` for Django, or your
        app's `main.py`/`__main__.py` file, call this function to configure logging for the whole app.

        Then in each module/script, simply import logging and create a variable `log = logging.getLogger(__name__)`
        to setup logging for that module.

    Params:
        log_level (str): The logging level string, i.e. "WARNING", "INFO", "DEBUG", "ERROR", "CRITICAL"
        log_fmt (str): The logging format string.
        datefmt (str): The format for datetimes in the logging string.
        silence_loggers (list[str]): A list of logger names to "disable" by setting their logLevel to "WARNING".
            Use this for any 3rd party modules, or dynamically load a list of loggers to silence from the environment.
    """
    logging.basicConfig(level=log_level, format=log_fmt, datefmt=datefmt)

    if silence_loggers:
        for _logger in silence_loggers:
            logging.getLogger(_logger).setLevel("WARNING")


def cleanup(rm_paths: list[str] = ["example.bak.duckdb", "example.duckdb"]):
    log.info("Running script cleanup")

    for p in rm_paths:
        path: Path = Path(p)

        if path.exists():
            if path.is_file():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                # path.rmdir()
                shutil.rmtree(path=str(path))
        else:
            log.warning(f"Could not find file/dir to delete: {path}")
            continue


def main(
    duckdb_connection_str: str = ":memory:",
    tbl_name: str = "test_tbl",
    read_only: bool = False,
    config: dict | None = None,
    insert_records: list[dict] = None,
    column_schema: dict = None,
    db_backup_dir: str = "example.bak.duckdb",
    run_cleanup: bool = False,
):
    log.info("Creating DuckDBController")
    db_controller: DuckDBController = DuckDBController(
        connection_str=duckdb_connection_str, read_only=read_only, config=config
    )

    print(f"duckdb_connection_str: {duckdb_connection_str}, run_cleanup: {run_cleanup}")

    log.debug(f"Inserting records: {insert_records}")

    with db_controller as db:
        log.info(f"Table '{tbl_name}' exists: {db.check_table_exists(tbl_name)}")

        db.create_table(table_name=tbl_name, columns=column_schema)
        log.info(f"Table '{tbl_name}' exists: {db.check_table_exists(tbl_name)}")

        db.insert_data(table_name=tbl_name, data=insert_records)

        all_test_tbl_query = f"SELECT * FROM '{tbl_name}';"
        all_test_tbl = db.query(query=all_test_tbl_query)
        log.info(f"All records in table '{tbl_name}':\n{all_test_tbl}")

        log.info(f"Creating parquet backup at {db_backup_dir}")
        db.backup_database(backup_dir=db_backup_dir, format="parquet")

        log.info(f"Creating csv backup at {db_backup_dir}")
        db.backup_database(backup_dir=db_backup_dir, format="csv", csv_delimiter="|")

        log.info(f"Dropping table {tbl_name}")
        db.drop_table(table_name=tbl_name)
        log.info(f"Table '{tbl_name}' exists: {db.check_table_exists(tbl_name)}")
        log.info(
            f"Recordings in {tbl_name} after dropping table: {db.query(all_test_tbl_query)}"
        )

        log.info(f"Restoring database from backup at {db_backup_dir}")
        db.restore_database(backup_dir=db_backup_dir)

        log.info(f"Count {tbl_name} rows: {db.count_rows(table_name=tbl_name)}")

        log.info("Creating Pandas dataframe from data")
        df = db.load_table_into_dataframe(table_name=tbl_name)

        print(df.head(5))

    log.debug(f"Run cleanup: {run_cleanup}")
    if run_cleanup:
        cleanup()


if __name__ == "__main__":
    setup_logging(log_level="DEBUG")

    DUCKDB_FILE = "example.duckdb"
    IN_NAMEDMEMORY = ":memory:example"
    INMEMORY = ":memory:"

    DEMO_INSERT_RECORDS: list[dict] = [
        {"id": 1, "name": "Derrick", "age": 43, "occupation": "carpenter"},
        {"id": 2, "name": "Frankie", "age": 32, "occupation": "dentist"},
        {"id": 3, "name": "Alice", "age": 27, "occupation": "tattoo artist"},
    ]
    DEMO_COL_SCHEMA: dict[str, str] = {
        "id": "INTEGER",
        "name": "TEXT",
        "age": "INTEGER",
        "occupation": "TEXT",
    }
    DEMO_DB_BACKUP_DIR: str = "example.bak.duckdb"
    DEMO_RUN_CLEANUP: bool = True

    ## Set params for script.
    #  If you want to override any values,
    #  change the corresponding variable above.
    run_config: dict = {
        "duckdb_connection_str": INMEMORY,
        "run_cleanup": DEMO_RUN_CLEANUP,
        "read_only": False,
        "config": None,
        "run_cleanup": False,
        "insert_records": DEMO_INSERT_RECORDS,
        "tbl_name": "test_tbl",
        "column_schema": DEMO_COL_SCHEMA,
        "db_backup_dir": DEMO_DB_BACKUP_DIR,
    }

    # main(**run_config)
    main(
        run_cleanup=True,
        duckdb_connection_str=DUCKDB_FILE,
        tbl_name="test_tbl",
        read_only=False,
        config=None,
        insert_records=DEMO_INSERT_RECORDS,
        column_schema=DEMO_COL_SCHEMA,
        db_backup_dir=DEMO_DB_BACKUP_DIR,
    )
