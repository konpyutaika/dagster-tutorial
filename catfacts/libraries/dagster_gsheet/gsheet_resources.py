import hashlib
from pydantic import BaseModel
import string
from typing import Optional, List, Tuple, Mapping

import pandas as pd
from dagster import ConfigurableResource, FreshnessPolicy
import pygsheets
from pygsheets import Worksheet, Spreadsheet
from pygsheets.client import Client


class SheetConfig(BaseModel):
    spreadsheet_id: str
    sheet_name: str
    description: Optional[str]
    columns_range: Optional[str]
    object_freshness_policy: Optional[FreshnessPolicy]
    table_freshness_policy: Optional[FreshnessPolicy]

    def metadata(self) -> Mapping[str, str]:
        metadata = {
            "spreadsheet_id": self.spreadsheet_id,
            "sheet_name": self.sheet_name,
        }
        if self.columns_range is not None:
            metadata["columns_range"] = self.columns_range
        return metadata

    def code_version(self) -> str:
        return hashlib.sha1(f"{self.spreadsheet_id}-{self.sheet_name}-${self.columns_range}".encode("utf-8")).hexdigest()


class GsheetResource(ConfigurableResource):
    """
    A Dagster resource that provides access to a Google Sheets document.

    Attributes:
        api_version (str): The version of the Google Sheets API to use.
        credentials (str): The authentication information for accessing the Google Sheets API.
    """
    api_version: str = "v4"
    credentials: str

    def get_client(self) -> Client:
        """Returns a Google Sheets client using the authentication information in the `credentials` attribute."""
        return pygsheets.authorize(service_account_json=self.credentials, scopes=(
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ))

    def get_spreadsheet(self, sheet_config: SheetConfig) -> pd.DataFrame:
        """
        Returns the data from a worksheet of a Google Sheets document as a Pandas DataFrame.

        Args:
            sheet_config (SheetConfig): The ID of the Google Sheets document, the name of the worksheet and optional range of columns in format <start_column>:<end_column>

        Returns:
            pd.DataFrame:Pandas DataFrames containing the data from the worksheet.
        """
        gsheet_client: Client = self.get_client()
        spread_sheet: Spreadsheet = gsheet_client.open_by_key(sheet_config.spreadsheet_id)
        worksheet = spread_sheet.worksheet_by_title(title=sheet_config.sheet_name)
        start_column, end_column = _get_worksheet_boundary(worksheet, sheet_config.columns_range)
        return worksheet.get_as_df(
            start=start_column,
            end=end_column,
            numerize=False,
            include_tailing_empty=True
        ).replace([None], ["NULL"])



def _get_worksheet_boundary(worksheet: Worksheet, columns_range: Optional[str]) -> List[Tuple[int, int]]:
    """
    Returns the boundaries of the worksheet as a list of tuples.

    Args:
        worksheet (Worksheet): A `Worksheet` object representing the worksheet.
        columns_range (Optional[str]): An optional range of columns.

    Returns:
        List[Tuple[int, int]]: A list of tuples representing the boundaries of the worksheet.
    """
    start_column: int = _column_position(columns_range.split(":")[0]) if columns_range else 1
    end_column: int = _column_position(columns_range.split(":")[1]) if columns_range else worksheet.cols
    return [(1, start_column), (worksheet.rows, end_column)]


def _column_position(cell) -> int:
    """
    Converts a column name to its position in the worksheet.

    Args:
        cell (str): The name of the column.

    Returns:
        int: The position of the column in the worksheet.
    """
    num = 0
    for character in cell:
        if character in string.ascii_letters:
            # Discard numbers at the end of cell coordinates
            num = num * 26 + (ord(character.upper()) - ord("A")) + 1
    return num

