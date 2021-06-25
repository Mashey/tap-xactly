from typing import List, Dict
import jaydebeapi
from jaydebeapi import Connection


import singer

LOGGER = singer.get_logger()


class XactlyClient:
    def __init__(self, config):
        self._user = config["user"]
        self._password = config["password"]
        self._client_id = config["client_id"]
        self._consumer = config["consumer"]
        self._client = None
        self._sql = None

    def setup_connection(self) -> Connection:
        self._client = jaydebeapi.connect(
            "com.xactly.connect.jdbc.Driver",
            "jdbc:xactly://api.xactlycorp.com:443/api?"
            + f"sslVerifyServer=true&clientId={self._client_id}&consumer={self._consumer}",
            [self._user, self._password],
            "./xjdbc-1.8.0-RELEASE-jar-with-dependencies.jar",
        )
        self._sql = self._client.cursor()

    def close_connection(self) -> None:
        self._sql.close()
        self._client.close()

    @property
    def is_connected(self) -> bool:
        if self._client is None:
            return False
        return not self._client.jconn.isClosed()

    # pylint: disable=too-many-arguments
    def query_database(
        self,
        table_name: str,
        limit: int,
        primary_key: str,
        bkmrk_primary_key: int,
        replication_key: str,
        bkmrk_date: str,
    ) -> List[Dict]:

        row_count = 0

        LOGGER.info("Querying DB")
        self._sql.execute(
            "SELECT * "
            + f"FROM {table_name} "
            + f"WHERE {replication_key} >= '{bkmrk_date}' AND {primary_key} > {bkmrk_primary_key} "
            + f"ORDER BY {replication_key}, {primary_key} "
            + f"LIMIT {limit}"
        )

        LOGGER.info("Query Complete.  Starting rows")
        rows = []

        results = self._sql.fetchall()
        columns = [key[0] for key in self._sql.description]

        for row in results:
            record = {}
            for idx, val in enumerate(row):
                if "java.time" in type(val).__name__:
                    val = str(val)
                record[columns[idx]] = val
            rows.append(record)
            row_count += 1
        LOGGER.info(f"Row count = {row_count}")
        return rows
