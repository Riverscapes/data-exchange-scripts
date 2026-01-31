"""compare parquet files - ingoring column order"""
import hashlib
import pandas as pd


def hash_dataframe_content(df: pd.DataFrame) -> str:
    """
    Generate a hash for a DataFrame, normalizing column order and handling binary columns.
    - Columns are sorted alphabetically.
    - Binary columns (bytes/bytearray) are converted to hex strings.
    - All other columns are converted to string.
    - The index is ignored in the hash.

    Args:
        df (pd.DataFrame): The DataFrame to hash.

    Returns:
        str: The MD5 hash of the DataFrame content.
    """
    df_copy = df.reindex(sorted(df.columns), axis=1).copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == 'object':
            # Try to detect binary columns by checking the first non-null value
            sample = df_copy[col].dropna().iloc[0] if not df_copy[col].dropna().empty else None
            if isinstance(sample, (bytes, bytearray)):
                # Convert binary columns to hex string
                df_copy[col] = df_copy[col].apply(lambda x: x.hex() if isinstance(x, (bytes, bytearray)) else str(x))
            else:
                df_copy[col] = df_copy[col].astype(str)
        else:
            df_copy[col] = df_copy[col].astype(str)
    return hashlib.md5(pd.util.hash_pandas_object(df_copy, index=False).values).hexdigest()


def compare_parquet_files(file1: str, file2: str) -> None:
    """
    Compare two Parquet files for content equality, ignoring column order.
    Prints the hash of each file and whether their content matches.

    Args:
        file1 (str): Path to the first Parquet file.
        file2 (str): Path to the second Parquet file.
    """
    df1 = pd.read_parquet(file1)
    df2 = pd.read_parquet(file2)
    hash1 = hash_dataframe_content(df1)
    hash2 = hash_dataframe_content(df2)
    print(f"File 1: {file1}")
    print(f"File 2: {file2}")
    print("Hash 1:", hash1)
    print("Hash 2:", hash2)
    print("Files have the same content (ignoring column order):", hash1 == hash2)


def main():
    """main
    Replace with your file paths
    """
    file1 = r"C:\nardata\pydataroot\rme-athena\downloads\0713001203\Jan15_rme_0713001203.parquet"
    file2 = r"C:\nardata\pydataroot\rme-athena\downloads\0713001203\new_rme_0713001203.parquet"
    compare_parquet_files(file1, file2)


if __name__ == "__main__":
    main()
