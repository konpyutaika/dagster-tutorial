from typing import Dict, Any

from dagster import Config


class ApiConfig(Config):
    max_length: int
    limit: int

    def params(self) -> Dict[str, Any]:
        return dict(max_length=self.max_length, limit=self.limit)