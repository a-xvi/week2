import pandas as pd 
from bootcamp_data.transform import enforce_schema,missingness_report,add_missing_flags,normalize_text
from bootcamp_data.quality import require_columns,assert_non_empty,assert_unique_key,assert_in_range
from pathlib import Path

reports_path = Path("reports")
reports_path.mkdir(parents=True, exist_ok=True)

orders = pd.read_csv("orders.csv")





def main():
    required_order_cols = ["order_id", "user_id", "amount", "quantity", "status"]
    require_columns(orders, required_order_cols)
    assert_non_empty(orders, "orders")

    orders = enforce_schema(orders)

    report = missingness_report(orders)
    report.to_csv("reports/orders_missingness.csv")

    orders["status_clean"] = normalize_text(orders["status"])

    orders = add_missing_flags(orders, ["amount", "quantity"])

    assert_in_range(orders["quantity"], lo=1, hi=1000, name="quantity")
    assert_in_range(orders["amount"], lo=0, name="amount")

    assert_unique_key(orders, "order_id")

    orders.to_parquet("data/orders_clean.parquet", index=False)
    report.to_csv(reports_path / "orders_missingness.csv")

if __name__ == "__main__":
    main()