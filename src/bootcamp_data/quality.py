import pandas as pd

def require_columns(df:pd.DataFrame , cols:list[str]) -> None:
    missing=[
        c for c in cols if c not in df.columns
    ]
    
    assert not missing, f"missing columns:{missing}"
   
   
def assert_non_empty(df:pd.DataFrame, name="df") -> None:
    assert len(df)> 0, f"{name} is empty"
    
def assert_unique_key(df:pd.DataFrame, key:str,allow_na=False) -> None:
    
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"# look for null values
    
    dup = df[key].duplicated(keep=False) & df[key].notna()
    assert not dup.any(), f"{key}not unique and has {dup.sum()}copies"
    
    
def assert_in_range(series:pd.Series, lo=None, hi=None, name:str ="value") -> None:
    x= series.dropna()
    
    if lo is not None:
        assert (x>=lo).all(), f"{x}is bigger than {lo}"
        
    if hi is not None:
        assert (x<=hi).all(), f"{x} is smaller than {hi}"
        
    