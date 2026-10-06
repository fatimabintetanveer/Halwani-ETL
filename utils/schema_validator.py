import pandas as pd
import polars as pl


def validate_columns(
    df: pd.DataFrame | pl.DataFrame,
    required: list[str],
    file_path: str = "",
) -> tuple[bool, str | None]:
    """
    Check that all required columns exist in the DataFrame.

    Parameters:
        df:       pandas or polars DataFrame to validate
        required: list of column names that must be present
        file_path: source file path (for the error message)

    Returns:
        (True, None) if all required columns are present.
        (False, error_msg) listing which columns are missing.
    """
    # Get column names from either pandas or polars
    if isinstance(df, pd.DataFrame):
        actual = set(df.columns)
    elif isinstance(df, pl.DataFrame):
        actual = set(df.columns)
    else:
        return False, f"Unsupported DataFrame type: {type(df)}"

    required_set = set(required)
    missing = required_set - actual    # set difference: what's required but not present

    if not missing:
        return True, None

    location = f" in {file_path}" if file_path else ""
    error = (
        f"Missing essential columns{location}: "
        f"{', '.join(sorted(missing))}. "
        f"Found columns: {', '.join(sorted(actual))}"
    )
    return False, error