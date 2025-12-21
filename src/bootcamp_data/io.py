import pandas as pd
from pathlib import Path

NA = ["", "NA", "N/A", "null", "None" , "not_a_number"]


def read_orders_csv(path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype={"order_id":"string","user_id":"string"},
        na_values=NA,
        keep_defualt_na=True,
    )
    
    
def read_users_csv(path) -> pd.DataFrame:
    pass

def write_parquet(df, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=True)
    

def read_parquet(path) -> pd.DataFrame:
    pass



