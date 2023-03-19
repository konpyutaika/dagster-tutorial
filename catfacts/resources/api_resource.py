from typing import Dict, Any, List

from dagster import ConfigurableResource
import requests

from .api_config import ApiConfig


class ApiResource(ConfigurableResource):
    host: str
    protocol: str
    endpoint: str

    def pagination(self, params: Dict[str, Any]):
        url = f"{self.protocol}://{self.host}/{self.endpoint}"
        page_num = 1
        while True:
            params["page"] = page_num
            response = requests.get(
                url=url,
                params=params
            )
            data = response.json()
            yield data

            if not data["next_page_url"]:
                break
            page_num += 1

    def request(self, config: ApiConfig) -> List[Dict[str, Any]]:
        responses = [item for item in self.pagination(params=config.params())]
        return responses
