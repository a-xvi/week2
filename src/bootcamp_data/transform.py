import pandas as pd 
import re


def parse_datetime(df:pd.DataFrame,column:str,utc=False) -> pd.DataFrame:
    df[column]=pd.to_datetime(df[column],errors="coerce",utc=utc)
    return df

def add_time_parts (df:pd.DataFrame ,ts_column:str) -> pd.DataFrame:
    ts = df[ts_column]
    df["date"] = ts.dt.date
    df["year"] = ts.dt.year
    df["month"] = ts.dt.month
    df["day"] = ts.dt.day_name()
    df["hour"] = ts.dt.hour
    return df

def iqr_bounds(df:pd.DataFrame, col:str) -> tuple[float, float]:
    df = df.dropna(subset=[col])
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    
    IQR = q3-q1
    
    low= q1 - 1.5 * IQR
    high= q3 + 1.5 * IQR
    
    return q1,q3
  
def winsorize_iqr(s:pd.Series) -> pd.Series:
    low,high=iqr_bounds(pd.DataFrame({"x":s}),"x")
    
    return s.clip(lower=low,upper=high)

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
    
def add_missing_flags(df:pd.DataFrame, cols:list[str]) -> pd.DataFrame:
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
    
    



