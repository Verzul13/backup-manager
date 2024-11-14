from manager.services.databases.clickhouse import ClickhouseService
from manager.services.databases.postgres import PostgresqlService

from manager.choices import DBType


DB_INTERFACE = {
    DBType.POSTGRESQL: PostgresqlService,
    DBType.CLICKHOUSE: ClickhouseService
}
