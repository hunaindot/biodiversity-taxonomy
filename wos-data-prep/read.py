from pathlib import Path
import pandas as pd


DEFAULT_FOLDER = Path("wos-data-prep/data") 

RENAME_MAP = {
    "PT": "Publication Type",
    "AU": "Author",
    "TI": "Title",
    "SO": "Journal",
    "VL": "Volume",
    "IS": "Issue",
    "DI": "DOI",
    "PD": "Date",
    "PY": "Year",
    "AB": "Abstract",
    "C1": "Author Address",
    # TC and WC are both described as “Notes” in the snippet.
    # Map them to separate columns so we don’t overwrite data.
    "TC": "Notes_TC",
    "WC": "Notes_WC",
    "UT": "Accession Number",
}

def _find_header_line(path: Path, encoding: str = "utf-8") -> int:
    """
    Return the (zero-based) line number that starts the true WoS header,
    i.e. the first line that begins with 'PT<TAB>'.
    """
    with path.open(encoding=encoding, errors="ignore") as fh:
        for i, line in enumerate(fh):
            if line.startswith("PT\t"):
                return i
    raise ValueError(f"'PT' header not found in {path.name}")

def _repair_pt_au(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix rows where PT and AU were glued together with a space instead of a tab.
    """
    if "PT" not in df.columns:
        return df                     # nothing to do

    # rows where PT is longer than 1 char and starts with a single letter
    mask = df["PT"].str.len().gt(1) & df["PT"].str.match(r"^[JBSP] ")
    if not mask.any():
        return df

    # ensure AU column exists
    if "AU" not in df.columns:
        df["AU"] = ""

    # split at the FIRST space
    first, rest = zip(*df.loc[mask, "PT"].str.split(" ", 1, expand=True).values)
    df.loc[mask, "PT"] = first
    df.loc[mask, "AU"] = rest
    return df


def read_wos_exports(folder: Path | str = DEFAULT_FOLDER,
                     rename: dict[str, str] = RENAME_MAP,
                     encoding: str = "utf-8") -> pd.DataFrame:
    """
    Read every tab-separated .txt export in *folder*, concatenate them,
    rename key columns per Clarivate docs, and return a dataframe.

    Parameters
    ----------
    folder : Path | str
        Directory to search for .txt files (non-recursive).
    rename : dict
        Mapping of original → new column names.
    encoding : str
        Encoding used by the export files (UTF-8 by default, change if needed).

    Returns
    -------
    pd.DataFrame
        All records combined in a single dataframe.
    """
    folder = Path(folder)
    if not folder.exists():
        raise FileNotFoundError(f"{folder.resolve()} does not exist.")

    # Collect every .txt file in the folder
    txt_files = sorted(folder.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {folder.resolve()}")

    frames = []
    for f in txt_files:
        # Web of Science exports are classic tab-delimited with a header row
        # hdr = _find_header_line(f, encoding)
        frame = pd.read_csv(
            f,
            sep="\t",
            header=0,
            dtype=str,           # keep everything as strings
            # skiprows=hdr,
            encoding=encoding,
            keep_default_na=False  # leave empty strings as blank, not NaN
        )
        frame = _repair_pt_au(frame)   # ← NEW: fix mis-separated columns
        frames.append(frame)

    # Merge them into one df (rows stack vertically)
    df = pd.concat(frames, ignore_index=True)

    # Apply renaming only for columns that actually exist
    cols_to_rename = {old: new for old, new in rename.items() if old in df.columns}
    df = df.rename(columns=cols_to_rename)

    # Show a quick peek
    print("\nPreview of merged dataframe (top 5 rows):")
    print(df.info())
    print(df.head(5))
    

    return df


if __name__ == "__main__":
    df = read_wos_exports()
    df.to_excel('dataset.xlsx')
    print("Saved")
