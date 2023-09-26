from typing import Dict, Any

from dagster import InputManager, InputContext, input_manager


class MetadataInputLoader(InputManager):
    def load_input(self, context: "InputContext") -> Dict[str, Dict[str, Any]]:
        asset_key = context.asset_key
        event_log_entry = context.step_context.instance.get_latest_materialization_event(asset_key)
        return event_log_entry.dagster_event.event_specific_data.materialization.metadata


@input_manager
def metadata_input_loader():
    return MetadataInputLoader()

