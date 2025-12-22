import pandas as pd 
import re
def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
        created_at = pd.to_datetime(df["created_at"], errors="coerce"),
    )
    
    
def missingness_report(df:pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    
    return(
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda x:x["n_missing"]/n)
        .sort_values("p_missing", ascending=False)
    )
    
def add_missing_flag(df:pd.DataFrame, cols:list[str]) -> pd.DataFrame:
    out=df.copy()
    
    for c in cols:
        out[f"{out}_isna"]= out[c].isna()
        
    return out

def normalize_text(series:pd.Series)-> pd.Series:
    _ws = re.compile(r"\s+")
    return(
        series.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws," ",regex=True)
    )

def apply_mapping(series:pd.Series, mapping:dict) -> pd.Series :
    return series.map(lambda x: mapping.get(x,x))

def dedupe_keep_latest(df:pd.DataFrame, key_cols:list[str], ts_col:str)-> pd.DataFrame:
    return(
        df
        .sort_values(ts_col)
        .drop_duplicates(subset=key_cols, keep="last")
        .reset_index(drop=True)
    )
    
    