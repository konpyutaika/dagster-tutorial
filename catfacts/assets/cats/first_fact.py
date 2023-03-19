from dagster import asset

from .facts import facts


@asset
def first_fact(facts) -> str:
    return facts[0]
