from __future__ import annotations
from pathlib import Path
import pandas as pd
from bootcamp_data.transform import parse_datetime,add_time_parts,iqr_bounds,winsorize_iqr, add_outlier_flag    
from bootcamp_data.joins import safe_left_join

orders = pd.read_csv("orders.csv")
orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce")


def main():
    print("*"*100)
    print(f"orders \n{orders}\n")
    print("*"*100)

    orders_parsed = parse_datetime(orders,"created_at",utc=True)
    orders_parsed = add_time_parts(orders_parsed,"created_at")
    orders_parsed = add_outlier_flag(orders_parsed, "amount")

    lower, upper = iqr_bounds(orders_parsed,"amount")
    print(f"\nIQR bounds for amount:\n {lower} - {upper}")
    print("*"*100)

        
    order_w = pd.DataFrame({
        "order_id": orders_parsed["order_id"],
        "amount_capped":winsorize_iqr(orders_parsed["amount"])
    })
    
    print(f"\nwinsorized DataFrame\n {order_w}")
    print("*"*100)

    
    orders_f = safe_left_join(left=orders_parsed,
                   right=order_w,
                   on="order_id",
                   validate="one_to_one")
        
        
    output_path = Path("orders_cleaned.parquet")
    orders_f.to_parquet(output_path, index=False)    
        
        
    print(f"\norder after cleaning:\n\n{orders_f}")
    print("*"*100)

        
    

if __name__ == "__main__":
    main()

