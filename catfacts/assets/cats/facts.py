from typing import List, Dict, Any

from dagster import (
    DynamicOut,
    DynamicOutput,
    graph_asset,
    MetadataValue,
    op,
    Output
)
import pandas as pd

from .catfacts import catfacts


@op(out=DynamicOut())
def get_data(responses: pd.DataFrame) -> List[DynamicOutput[Dict[str, Any]]]:

    outputs = []
    for idx, item in enumerate(responses.to_dict(orient="records")):
        outputs.append(DynamicOutput(item, mapping_key=str(idx)))
    return outputs


@op
def get_data_content(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [element for element in data["data"]]


@op
def parse_fact(contents: List[Dict[str, Any]]) -> Output[List[str]]:
    the_facts = [content["fact"] for content in contents]
    return Output(
        value=the_facts,
        metadata={
            "num_records": len(the_facts),
            "preview": MetadataValue.md(pd.DataFrame(the_facts).head().to_markdown())
        }
    )


@op
def flatten(to_flat: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return [item for sublist in to_flat for item in sublist]


@graph_asset
def facts(catfacts: pd.DataFrame) -> List[str]:
    data = get_data(catfacts)
    data_contents = data.map(get_data_content)
    return parse_fact(flatten(data_contents.collect()))
