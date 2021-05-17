from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from marquez_airflow.extractors.postgres_extractor import PostgresExtractor
from marquez_airflow.utils import get_connection_uri, get_connection


class SnowflakeExtractor(PostgresExtractor):
    operator_class = SnowflakeOperator
    source_type = 'SNOWFLAKE'

    def __init__(self, operator):
        super().__init__(operator)

    def _information_schema_query(self, table_names: str) -> str:
        return f"""
        SELECT table_schema,
               table_name,
               column_name,
               ordinal_position,
               data_type
          FROM {self.operator.database}.information_schema.columns
         WHERE table_name IN ({table_names});
        """

    def _get_scheme(self):
        return f'snowflake'

    def _get_authority(self, conn_id):
        return self.operator.get_hook()._get_conn_params()['account']

    def _get_hook(self):
        return self.operator.get_hook()

    def _conn_id(self):
        return self.operator.snowflake_conn_id

    def _get_connection_uri(self, conn_id):
        return get_connection_uri(conn_id)
