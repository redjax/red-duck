from __future__ import annotations

from contextlib import AbstractContextManager
import logging
import os
from pathlib import Path
import shutil
import typing as t

log: logging.Logger = logging.getLogger(__name__)

import duckdb
import pandas as pd

DEFAULT_PANDAS_ENGINE: str = "pyarrow"


class DuckDBController(AbstractContextManager):
    """Controller class for interacting with a DuckDB database.

    Description:
        Initialize a DuckDB by passing a filepath (or ':memory:' for an in-memory DuckDB instance). The class
        handles interactions with the database, like creating/dropping tables, inserting/updating/deleting data,
        maintenance tasks like vacuuming to reclaim free space, etc.

        The controller can also import data from .csv and .parquet files, and export tables to those filetypes.

    Params:
        connection_str (str): (Default: ":memory:") A path to a DuckDB file, or a string denoting an in-memory database.
        read_only (str): (Default: False) !!NOTWORKING, When `True`, restrict to READ only invocations like `SELECT`.
        config (dict): (Optional) DuckDB configuration dict.
        [DuckDB config documentation](https://duckdb.org/docs/configuration/overview.html#configuration-reference)

    Usage:
    ```python
    with DuckDBController(connection_str="path/to/app.duckdb") as duckdb_ctl:
        ## Create a table from .parquet files
        duckdb_ctl.import_from_parquet(
            table_name="someTable", parquet_files=["file.parquet", "file2.parquet"]
        )

        ## Create a table from .csv files
        duckdb_ctl.import_from_csv(table_name="someOtherTable", csv_files=["file.csv", "file2.csv"])

        ## List tables in database
        duckdb_ctl.list_tables()

        ## If database is a file, get its size in bytes
        duckdb_ctl.database_file_size()

        ## Delete the database file
        duckdb_ctl.delete_database_file()
    ```
    """

    def __init__(
        self,
        connection_str: str = ":memory:",
        read_only: bool = False,
        config: dict | None = None,
    ) -> None:
        self.connection_str: str = connection_str
        self.read_only: bool = read_only
        ## DuckDB configuration option docs:
        #  https://duckdb.org/docs/configuration/overview.html#configuration-reference
        self.config: dict | None = config

        ## DuckDB connection
        self.connection: duckdb.DuckDBPyConnection | None = None
        ## Class logger
        self.logger: logging.Logger = log.getChild("DuckDBController")

    def __enter__(self) -> t.Self:
        self.connection: duckdb.DuckDBPyConnection = self._create_connection()

        return self

    def __exit__(self, exc_type, exc_val, traceback) -> bool:
        if self.connection:
            self.connection.close()

        if exc_val:
            self.logger.error(f"({exc_type}): {exc_val}")

            return False
        else:
            return True

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection.

        Description:
            Uses an in-memory database if connection_str is None or ':memory:'.

            If connection_str is a path string, connection will create parent directories automatically.

        Params:
            connection_str (str): (Default: ":memory:") Path to a .duckdb database file, or None/":memory:" for an in-memory database.

        """
        if self.connection_str is None:
            # Create an in-memory database
            return duckdb.connect(
                database=":memory:", read_only=self.read_only, config=self.config
            )
        else:
            # Connect to a file-based database
            return duckdb.connect(database=self.connection_str)

    def database_file_size(self) -> int:
        """Get the size of the DuckDB database file in bytes."""
        if self.connection_str is None:
            self.logger.warning("Database is in-memory, file size does not apply.")

            return None

        return os.path.getsize(filename=self.connection_str)

    def fetch_data(
        self, table_name: str, limit: int | None = None
    ) -> list[duckdb.DuckDBPyRelation]:
        """Fetch all rows from a table with an optional limit.

        Params:
            table_name (str): Name of table to fetch from.
            limit (int): Maximum number of records to fetch, or None for no limit.
        """
        query: str = f"SELECT * FROM {table_name}"

        if limit:
            query += f" LIMIT {limit}"

        result: list[t.Any] = self.connection.execute(query=query).fetchall()

        return result

    def delete_all_data(self, table_name: str) -> None:
        """Delete all data from a table, without deleting the table itself..

        Params:
            table_name (str): Name of the table to wipe data.
        """
        query: str = f"DELETE FROM {table_name}"

        self.connection.execute(query=query)

    def count_rows(self, table_name: str) -> int:
        """Count the number of rows in a table.

        Params:
            table_name (str): Nameof the table to count records.
        """
        query: str = f"SELECT COUNT(*) FROM {table_name}"
        result: tuple | None = self.connection.execute(query=query).fetchone()

        return result[0] if result else 0

    def show_tables(self) -> list[str]:
        """List all tables in the database."""
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        result: list[t.Any] = self.connection.execute(query=query).fetchall()

        # Extract table names from the query result
        rows: list[t.Any] = [row[0] for row in result]

        return rows

    def list_table_columns(self, table_name: str) -> list[str]:
        """Get the column names of a table.

        Params:
            table_name (str): Name of table to get columns.

        """
        query: str = (
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? AND table_schema = 'main'"
        )
        result: list[str] = self.connection.execute(
            query=query, parameters=[table_name]
        ).fetchall()

        return [row[0] for row in result]

    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Params:
            table_name (str): Name of table to check existence.
        """
        query = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?"
        result: tuple | None = self.connection.execute(query, [table_name]).fetchone()

        return result is not None

    def create_table(self, table_name: str, columns: dict[str, str]) -> None:
        """Create table with specified columns.

        Params:
            table_name (str): Name of table to create.
            columns (dict[str, str]): Dict mapping of columns and their datatypes. Example:
            {"id": "INTEGER PRIMARY KEY", "name": "VARCHAR", "age": "INTEGER"}
        """
        columns_def = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"

        try:
            self.connection.execute(query)
        except Exception as exc:
            msg = f"({type(exc)}) Unhandled exception creating table '{table_name}'. Details: {exc}"
            self.logger.error(msg)

            raise exc

    def drop_table(self, table_name: str) -> None:
        """Drop specified table.

        Params:
            table_name (str): Name of table to drop.
        """
        query: str = f"DROP TABLE IF EXISTS {table_name}"
        try:
            self.connection.execute(query)
        except Exception as exc:
            msg = f"({type(exc)}) Unhandled exception dropping table '{table_name}'. Details: {exc}"
            self.logger.error(msg)

            raise exc

    def insert_data(
        self, table_name: str, data: list[dict[str, t.Any]]
    ) -> t.Literal[False] | None:
        """Insert data into the table.

        Params:
            table_name (str): Name of table to insert data into.
            data (list[dict[str, Any]]): List of dicts with key/value data to insert into database.
        """
        if not data:
            return

        columns: str = ", ".join(data[0].keys())
        placeholders: str = ", ".join("?" for _ in data[0].keys())
        query: str = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values: list[tuple] = [tuple(row.values()) for row in data]

        try:
            self.connection.executemany(query=query, parameters=values)
        except duckdb.ConstraintException as constraint_exc:
            self.logger.error(
                f"Constraint violation while inserting data: {constraint_exc}."
            )

            return False
        except Exception as exc:
            msg: str = (
                f"({type(exc)}) Unhandled exception inserting data into table '{table_name}'. Details: {exc}"
            )
            self.logger.error(msg)

            raise exc

    def update_data(
        self, table_name: str, update_values: dict[str, t.Any], condition: str
    ) -> None:
        """Update data in the table.

        Description:
            Updates a reecord in a speecified table with data represented as a dict.

        Example:
            with DuckDBController() as duckdb_ctl:
                duckdb_ctl.update_data(table_name="users", update_values={"age": 26}, condition="name = 'Bob'")

        Params:
            table_name (str): Name of table to update record.
            update_values (str): Dict with key/value pair of database records to update.

        """
        set_clause: str = ", ".join([f"{key} = ?" for key in update_values.keys()])
        query: str = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"

        try:
            self.connection.execute(
                query=query, parameters=tuple(update_values.values())
            )
        except Exception as exc:
            msg: str = (
                f"({type(exc)}) Unhandled exception updating data in table '{table_name}'. Details: {exc}"
            )
            self.logger.error(msg)

            raise exc

    def delete_data(self, table_name: str, condition: str) -> None:
        """Delete data from the table.

        Params:
            condition (str): SQL condition to match data to delete, i.e. 'name = "Bob"'
        """
        query: str = f"DELETE FROM {table_name} WHERE {condition}"

        try:
            self.connection.execute(query)
        except Exception as exc:
            msg: str = (
                f"({type(exc)}) Unhandled exception deleting data in table '{table_name}'. Details: {exc}"
            )
            self.logger.error(msg)

            raise exc

    def query(self, query: str) -> list[duckdb.DuckDBPyRelation]:
        """Run a query and return the result.

        Params:
            query (str): DuckDB SQL statement to run.
                More info: https://duckdb.org/docs/sql/introduction
        """
        try:
            result: list[t.Any] = self.connection.execute(query=query).fetchall()

            return result
        except duckdb.CatalogException as catalog_exc:
            log.error(f"[DUCKDB ERROR] Error executing query. Details: {catalog_exc}")
            return None
        except Exception as exc:
            msg = f"({type(exc)}) Unhandled exception execeuting query. Details: {exc}"
            log.error(msg)

            raise exc

    def execute_raw_sql(
        self, query: str, params: list[t.Any] | None = None
    ) -> list[duckdb.DuckDBPyRelation]:
        """Execute a raw SQL query.

        Params:
            query (str): The raw SQL to execute.
            params (list[t.Any]|None): Optional list of params to substitute into query.
        """
        result: list[t.Any] = self.connection.execute(
            query=query, parameters=params or []
        ).fetchall()
        return result

    def export_to_csv(self, table_name: str, output_path: str) -> None:
        """Export data in a table to a CSV file.

        Params:
            table_name (str): Name of database table to export. This table must exist.
            output_path (str): Path to .csv file where table data will be exported.
        """
        query: str = f"COPY {table_name} TO '{output_path}' WITH (FORMAT CSV, HEADER)"

        self.connection.execute(query)

    def create_table_from_csv(self, table_name: str, csv_file: str) -> str:
        """Create a table from a CSV file schema.

        Params:
            table_name (str): Name of database table to create from CSV data.
            csv_file (str): Path to .csv file where table data will be imported from.
        """
        # Read just the header to get the schema
        df = pd.read_csv(csv_file, nrows=0)
        # Create schema with VARCHAR types
        schema = ", ".join([f"{col} VARCHAR" for col in df.columns])

        query = f"CREATE TABLE {table_name} ({schema})"

        self.connection.execute(query)

        return schema

    def import_from_csv(self, table_name: str, csv_files: t.Union[str, list[str]]):
        """Import data from one or more CSV files into a table.

        Params:
            table_name (str): Name of database table to create from CSV data.
            csv_files (str|list[str]): Path(s) to .csv file(s) where table data will be imported from.
        """
        if isinstance(csv_files, str):
            csv_files = [csv_files]

        # Create the table schema if it doesn't exist
        if csv_files:
            self.create_table_from_csv(table_name=table_name, csv_file=csv_files[0])

        # Import data from each CSV file
        for file_path in csv_files:
            query: str = (
                f"COPY {table_name} FROM '{file_path}' WITH (FORMAT CSV, HEADER)"
            )

            try:
                self.connection.execute(query)
            except Exception as exc:
                msg: str = (
                    f"({type(exc)}) Unhandled exception importing data from CSV file '{file_path}'. Details: {exc}"
                )
                self.logger.error(msg)

                raise exc

    def backup_database(
        self,
        backup_dir: str,
        format: str = "CSV",
        csv_delimiter: str = ",",
        parquet_compression: str = "ZSTD",
        parquet_row_group_size: int = 100_000,
    ) -> None:
        """Create a backup of the DuckDB database.

        Params:
            backup_file (str): Output path for database backup file.
        """
        if format.upper() == "CSV":
            query = f"EXPORT DATABASE '{backup_dir}' (FORMAT CSV, DELIMITER '{csv_delimiter}');"
        elif format.upper() == "PARQUET":
            query = f"EXPORT DATABASE '{backup_dir}' (FORMAT PARQUET, COMPRESSION {parquet_compression}, ROW_GROUP_SIZE {parquet_row_group_size})"

        try:
            self.connection.execute(query)
            self.logger.info(f"Database dumped to '{backup_dir}' successfully.")

            return True
        except Exception as exc:
            msg = f"({type(exc)}) Unhandled exception dumping database to file '{backup_dir}'. Details: {exc}"
            self.logger.error(msg)

            raise exc

    def restore_database(self, backup_dir: str) -> None:
        """Restore the DuckDB database from a backup.

        Params:
            backup_file (sef): Path to database file to restore from.
        """
        # shutil.copy(backup_file, self.connection_str)
        try:
            self.connection.execute(f"IMPORT DATABASE '{backup_dir}';")
            self.logger.info(f"Database restored from '{backup_dir}' successfully.")
        except Exception as exc:
            msg = f"({type(exc)}) Unhandled exception restoring data from file '{backup_dir}'. Details: {exc}"
            self.logger.error(msg)

            raise exc

    def load_table_into_dataframe(self, table_name: str) -> pd.DataFrame:
        """Load a table from the database into a Pandas DataFrame.

        Params:
            table_name (str): Name of table to load into Pandas DataFrame.
        """
        query: str = f"SELECT * FROM {table_name}"
        df: pd.DataFrame = self.connection.execute(
            query=query
        ).fetchdf()  # DuckDB provides fetchdf() to return a DataFrame
        return df

    def import_from_parquet(
        self, table_name: str, parquet_files: t.Union[str, list[str]]
    ) -> None:
        """Load data from a .parquet file or a list of .parquet files into a table.

        Params:
            table_name (str): Name of table for imported data.
            parquet_files (str|list[str]): Path(s) to .parquet file(s) to load into database.

        """
        ## If a single file is passed, convert it to a list for consistent handling
        if isinstance(parquet_files, str):
            parquet_files = [parquet_files]

        ## Loop through all parquet files and create a table if it doesn't exist
        for parquet_file in parquet_files:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')"
            self.connection.execute(query)

    def export_to_parquet(self, table_name: str, output_path: str):
        """Dump data from a DuckDB table to a .parquet file.

        Params:
            table_name (str): Name of table to dump.
            output_path (str): Path to .parquet file where table data will be dumped.
        """
        if not output_path.endswith(".parquet"):
            output_path = output_path + ".parquet"
        query = f"COPY {table_name} TO '{output_path}' (FORMAT 'parquet')"
        self.connection.execute(query)

    def load_table_from_dataframe(self, df: pd.DataFrame, table_name: str):
        """Convert a Pandas DataFrame to a DuckDB table.

        Params:
            df (pandas.DataFrame): Existing DataFrame to load into table.
            table_name (str): Name of table to import data into.
        """
        ## Write the DataFrame to the specified table
        self.connection.register(view_name="temp_df", python_object=df)

        try:
            query: str = f"CREATE TABLE {table_name} AS SELECT * FROM temp_df"

            self.connection.execute(query)
        except Exception as exc:
            msg: str = (
                f"({type(exc)}) Unhandled exception loading Pandas DataFrame into table '{table_name}'. Details: {exc}"
            )
            self.logger.error(msg)

            raise exc
        finally:
            ## Optionally, unregister the DataFrame after the operation
            self.connection.unregister("temp_df")

    def vacuum_database(self) -> None:
        """Vacuum the database to reclaim space."""
        self.connection.execute(query="VACUUM")

    def delete_connection_str(self) -> None:
        """Deletes the DuckDB database file, if one exists."""
        if self.connection_str is None or self.connection_str == ":memory:":
            self.logger.warning("Database is in memory, file deletion does not apply.")

            return

        if not Path(self.connection_str).exists():
            raise FileNotFoundError(
                f"Database file '{self.connection_str}' does not exist."
            )

        if self.connection:
            self.connection.close()

        self.logger.info(f"Deleting database '{self.connection_str}'.")
        try:
            Path(self.connection_str).unlink(missing_ok=True)
        except Exception as exc:
            msg: str = (
                f"({type(exc)}) Unhandled exception deleting database file '{self.connection_str}'. Details: {exc}"
            )
            self.logger.error(msg)

            raise exc
