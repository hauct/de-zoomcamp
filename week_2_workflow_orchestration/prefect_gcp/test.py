import os
import pandas as pd
from pathlib import Path

# current_working_directory = os.getcwd()

# unix_path = os.path.normpath(current_working_directory).replace('\\', '/')
# print(unix_path)

# color = "yellow"
# year = 2021
# month = 1
# dataset_file = f"{color}_tripdata_{year}-{month:02}"

# path = Path(unix_path + f"/data/{color}/{dataset_file}.parquet")
# print(path)

# Tạo một dictionary
data = {
    'Name': ['John', 'Anna', 'Peter', 'Linda'],
    'Age': [28, 22, 35, 32],
    'City': ['New York', 'Paris', 'Berlin', 'London']
}

# Chuyển đổi dictionary thành DataFrame
df = pd.DataFrame(data)


def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

write_local(df, 'yellow', dataset_file='test_df')