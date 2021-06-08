from dbt.adapters.base import available
from dbt.adapters.snowflake import SnowflakeAdapter

from dbt.adapters.openlineage_snowflake import OpenLineageConnectionManager


class OpenLineageSnowflakeAdapter(SnowflakeAdapter):
    ConnectionManager = OpenLineageConnectionManager

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sqlmap = {}


    @classmethod
    def type(cls) -> str:
        return 'openlineage_snowflake'

    @available.parse(lambda *a, **k: '')
    def emit_start(self, model, run_started_at):
        return self.connections.emit_start(model, run_started_at)

    @available.parse(lambda *a, **k: '')
    def emit_complete(self, run_id):
        return self.connections.emit_complete(run_id)
