from dagster import asset, Output, MetadataValue
import pandas as pd

from ...resources.api_config import ApiConfig
from ...resources.api_resource import ApiResource


@asset(io_manager_key="dataframe_io_manager")
def catfacts(config: ApiConfig, catfacts_client: ApiResource) -> Output[pd.DataFrame]:
    responses = catfacts_client.request(config=config)
    df = pd.DataFrame(responses)
    return Output(
        value=df,
        metadata={
            "num_records": len(responses),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )
