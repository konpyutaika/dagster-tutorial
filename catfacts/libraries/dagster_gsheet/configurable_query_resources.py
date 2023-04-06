from typing import Dict, Any, Optional
from string import Formatter

from dagster import (
    ConfigurableResource
)


def _build_query(query_template: str, query_params: Dict[str, Any]) -> str:
    templated_variables = [key for _, key, _, _ in Formatter().parse(query_template)]
    params = {key: val for key, val in query_params.items() if key in templated_variables}
    if len(params):
        return query_template.format(**params)
    return query_template


class ConfigurableQueryResource(ConfigurableResource):
    schema_name: str
    table_name: Optional[str] = None
    extra_config: Optional[Dict[str, Any]] = None

    def build_query(self, query) -> str:
        object_config = self.dict()
        del(object_config['extra_config'])
        if self.extra_config is not None:
            object_config = object_config | self.extra_config
        return _build_query(
            query_template=query,
            query_params=object_config
        )


class ConfigurableQueryCopyResource(ConfigurableQueryResource):
    iam_role_arn: str

