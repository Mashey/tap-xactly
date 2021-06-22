import jaydebeapi
from jaydebeapi import Connection

# Build the Data Resource Service Here as a class with each endpoint as a function.
# Do not iterate over paginated endpoints in this file.  Below are just samples


class XactlyClient:
    def __init__(self, config):
        self._user = config["user"]
        self._password = config["password"]
        self._client_id = config["client_id"]
        self._consumer = config["consumer"]
        self._client = self.setup_connection()
        self._sql = self._client.cursor()

    def setup_connection(self) -> Connection:
        connection = jaydebeapi.connect(
            "com.xactly.connect.jdbc.Driver",
            "jdbc:xactly://api.xactlycorp.com:443/api?"
            + f"sslVerifyServer=true&clientId={self._client_id}&consumer={self._consumer}",
            [self._user, self._password],
            "./xjdbc-1.8.0-RELEASE-jar-with-dependencies.jar",
        )
        return connection

    @property
    def is_connected(self) -> bool:
        return not self._client.jconn.isClosed()
