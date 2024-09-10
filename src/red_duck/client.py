import typing as t
import logging

log = logging.getLogger(__name__)

from .controllers import DuckDBController

def get_duckdb_controller(connection_str: str = ":memory:", read_only: bool = False, config: dict | None = None) -> DuckDBController:
    try:
        controller: DuckDBController = DuckDBController(connection_str=connection_str, read_only=read_only, config=config)
        return controller
    except Exception as exc:
        msg = f"({type(exc)}) Unhandled exception initializing red_duck.DuckDBController. Details: {exc}"
        log.error(msg)
        
        raise exc
    
