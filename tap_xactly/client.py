import json
import requests

# Build the Data Resource Service Here as a class with each endpoint as a function.
# Do not iterate over paginated endpoints in this file.  Below are just samples


class XactlyClient:
    BASE_URL = "https://api.xactlycorp.com"

    def __init__(self, username: str, password: str, client_id: str, consumer: str):
        self._client = requests.Session()
        self._access_token = self.fetch_access_token(
            username, password, client_id, consumer
        )
        self._client.headers.update(
            {"authorization": self._access_token}
        )

    def fetch_access_token(
        self, username: str, password: str, client_id: str, consumer: str
    ):
        url = f"{self.BASE_URL}/api/oauth2/token"
        payload_dict = {
            "client_id": client_id,
            "consumer": consumer,
            "api": "connect",
        }
        request_body = {
            "username": username,
            "password": password,
        }
        access_token = self._client.post(url, data=payload_dict, body=request_body).json()[
            "access_token"
        ]

        return access_token

    # def fetch_audits(self, page) -> str:
    #     url = f"{self.BASE_URL}/connect/v1/audits"
    #     param_payload = {
    #         "size": 100,
    #         "page": 0,
    #     }
    #     response = self._client.get(url, data=param_payload).json()

    #     return response

    # def fetch_cursors(self) -> str:
    #     url = f"{self.BASE_URL}/connect/v1/audits"
    #     param_payload = {
    #         "size": 100,
    #         "page": 0,
    #     }
    #     response = self._client.get(url, data=param_payload).json()

    #     return response

    # def fetch_ENDPOINT_1(self):
    #     url = f'{self.BASE_URL}/ADDITIONAL_URI_ADDRESS'
    #     param_payload = {
    #         'active': 'true',
    #         'pagesize': NUMBER,  # Max per page count
    #         'page': NUMBER  # Page will have to be iterated over in a range
    #     }
    #     return self._client.get(url, params=param_payload).json()

    # def fetch_ENDPOINT_2(self):
    #     url = f'{self.BASE_URL}/ADDITIONAL_URI_ADDRESS'
    #     return self._client.get(url).json()
