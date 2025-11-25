# scripts/transform.py
import polars as pl
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def clean_and_transform(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean flattened product-level DataFrame.
    Fill missing customer_name, age with 'No Data'
    """
    df = df.with_columns(
        [
            pl.col("cart_id").cast(pl.Int64),
            pl.col("user_id").cast(pl.Int64),
            pl.col("product_id").cast(pl.Int64),
            pl.col("product_title").map_elements(lambda x: x.strip() if isinstance(x, str) else x),
            pl.col("product_price").cast(pl.Float64),
            pl.col("product_quantity").cast(pl.Int64),
            pl.col("product_total").cast(pl.Float64),

            # Clean customer fields
            pl.col("customer_name")
            .map_elements(lambda x: x.strip() if isinstance(x, str) else None)
            .fill_null("No Data")
            .alias("customer_name"),

            pl.col("first_name").fill_null("No Data"),
            pl.col("last_name").fill_null("No Data"),

            pl.col("email").fill_null("No Data"),
            pl.col("city").fill_null("No Data"),

            # Age → replace missing with No Data
            pl.col("age")
            .cast(pl.Utf8, strict=False)
            .fill_null("No Data")
            .alias("age"),
            pl.col("gender").fill_null("No Data"),

            pl.col("order_date"),
        ]
    )

    # compute total_amount
    df = df.with_columns(
        pl.when((pl.col("product_total").is_null()) | (pl.col("product_total") == 0))
        .then(pl.col("product_price") * pl.col("product_quantity"))
        .otherwise(pl.col("product_total"))
        .alias("total_amount")
    )

    # standardize product_title
    df = df.with_columns(
        pl.col("product_title")
        .str.to_lowercase()
        .str.strip_chars()
        .alias("product_title")
    )

    # drop rows missing product_id
    df = df.filter(pl.col("product_id").is_not_null())

    return df


def write_analytics(df: pl.DataFrame):
    """
    Write:
      - transformed_carts.csv
      - daily_sales.csv
      - customer_summary.csv (customer_id, customer_name, total_orders, total_spent)
      - revenue_by_product.csv
    """
    df = df.with_columns(
        pl.col("order_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    )

    transformed_path = PROCESSED_DIR / "transformed_carts.csv"
    df.write_csv(transformed_path)
    print("Saved:", transformed_path)

    # DuckDB for group-by
    con = duckdb.connect(database=":memory:")
    con.register("sales", df.to_pandas())

    # Revenue by product
    revenue_by_product = con.execute(
        """
        SELECT product_title, SUM(total_amount) AS total_revenue
        FROM sales
        GROUP BY product_title
        ORDER BY total_revenue DESC
        """
    ).fetchdf()

    # Updated CUSTOMER SUMMARY
    customer_summary = con.execute(
        """
        SELECT
            user_id AS customer_id,
            CASE
                WHEN customer_name IS NULL OR customer_name = '' THEN 'No Data'
                ELSE customer_name
            END AS customer_name,
            COUNT(DISTINCT cart_id) AS total_orders,
            SUM(total_amount) AS total_spent
        FROM sales
        GROUP BY customer_id, customer_name
        ORDER BY total_spent DESC
        """
    ).fetchdf()

    # Daily sales
    daily_sales = con.execute(
        """
        SELECT order_date, SUM(total_amount) AS daily_sales
        FROM sales
        GROUP BY order_date
        ORDER BY order_date
        """
    ).fetchdf()

    # Write results
    pl.from_pandas(revenue_by_product).write_csv(PROCESSED_DIR / "revenue_by_product.csv")
    pl.from_pandas(customer_summary).write_csv(PROCESSED_DIR / "customer_summary.csv")
    pl.from_pandas(daily_sales).write_csv(PROCESSED_DIR / "daily_sales.csv")

    print("Wrote analytics to:", PROCESSED_DIR)


if __name__ == "__main__":
    extracted_path = PROCESSED_DIR / "extracted.parquet"
    if extracted_path.exists():
        df = pl.read_parquet(extracted_path)
    else:
        raw = ROOT / "data" / "raw" / "sample_sales.csv"
        df = pl.read_csv(raw)

    df_clean = clean_and_transform(df)
    df_clean.write_parquet(PROCESSED_DIR / "clean_sales.parquet")
    write_analytics(df_clean)
