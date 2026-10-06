import re
import pandas as pd

def extract_grammage(
    df: pd.DataFrame,
    source_column: str,
    retailer: str,
    target_column: str = "SizeDesc",
) -> pd.DataFrame:
    """
    Extract grammage from a product description column using regex.
    Chooses pattern and preprocessing based on retailer.

    Othaim / Lulu: uppercase text, strip slashes, OZ→G conversion.
    Tamimi: lowercase units, fraction fallback
    """

    def _extract(text):
        if pd.isna(text):
            return ""

        if retailer in ("Othaim", "Lulu"):
            text = str(text).upper().replace("/", "")
            match = re.search(
                r"(\d+(?:\.\d+)?"
                r"(?:GAL|G|KG|K|L|ML|M|OZ"
                r"| GAL| G| KG| K| L| ML| M| OZ))",
                text,
            )
            if match:
                return match.group(1).strip()
            return ""

        if retailer == "Tamimi":
            text = str(text)
            for pat in (
                r"(\d+(?:\.\d+)?(?:kgs|kg|grm|gr|g|ml|ltr|oz|lbs))",
                r"(\d+/\d+\.?\d*\s*(?:kgs|grm|oz|lbs))",
            ):
                match = re.search(pat, text)
                if match:
                    return match.group(1).strip()
        return ""

    df[target_column] = df[source_column].apply(_extract)

    if retailer =="Lulu":
        df[target_column] = df[target_column].apply(_convert_oz)

    return df


def _convert_oz(value: str) -> str:
    """ "12OZ" → "340.2G". Non-OZ values pass through unchanged. """
    if not value or "OZ" not in value.upper():
        return value
    num = re.search(r"([\d.]+)", value)
    if num:
        return f"{round(float(num.group(1)) * 28.35, 2)}G"
    return value