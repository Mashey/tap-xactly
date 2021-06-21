import json
import requests
import jaydebeapi

# Build the Data Resource Service Here as a class with each endpoint as a function.
# Do not iterate over paginated endpoints in this file.  Below are just samples


class XactlyClient:
    def __init__(self, config):
        self.XACTLY_USER = config["XACTLY_USER"]
        self.XACTLY_PASSWORD = config["XACTLY_PASSWORD"]
        self.XACTLY_CLIENTID = config["XACTLY_CLIENTID"]
        self.XACTLY_CONSUMER = config["XACTLY_CONSUMER"]
        self.client = self.setup_connection()
        self.sql = self.client.cursor()

    def setup_connection(self):
        connection = jaydebeapi.connect(
            "com.xactly.connect.jdbc.Driver",
            f"jdbc:xactly://api.xactlycorp.com:443/api?sslVerifyServer=true&clientId={self.XACTLY_CLIENTID}&consumer={self.XACTLY_CONSUMER}",
            [self.XACTLY_USER, self.XACTLY_PASSWORD],
            "./xjdbc-1.8.0-RELEASE-jar-with-dependencies.jar",
        )

        return connection
