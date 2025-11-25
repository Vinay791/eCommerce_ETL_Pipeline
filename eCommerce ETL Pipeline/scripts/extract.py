# scripts/extract_api.py
import requests
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

API_CARTS = "https://dummyjson.com/carts?limit=100&skip=0"
API_USERS = "https://dummyjson.com/users?limit=100&skip=0"

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _fetch_json(url: str) -> dict:
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def extract_from_api(carts_url: str = API_CARTS, users_url: str = API_USERS) -> pl.DataFrame:
    """
    Fetch carts and users, flatten carts -> product rows, join user details,
    add customer_name and synthetic order_date, and return a Polars DataFrame.
    Also writes extracted.parquet for downstream use.
    """

    carts_payload = _fetch_json(carts_url)
    users_payload = _fetch_json(users_url)

    carts = carts_payload.get("carts", [])
    users = users_payload.get("users", [])

    # build user lookup dict
    user_map = {u["id"]: u for u in users}

    rows = []
    for cart in carts:
        cart_base = {
            "cart_id": cart.get("id"),
            "user_id": cart.get("userId"),
            "cart_total": cart.get("total"),
            "cart_discounted_total": cart.get("discountedTotal"),
            "cart_total_products": cart.get("totalProducts"),
            "cart_total_quantity": cart.get("totalQuantity"),
        }
        products = cart.get("products", []) or []

        for p in products:
            row = {
                **cart_base,
                "product_id": p.get("id"),
                "product_title": p.get("title"),
                "product_price": p.get("price"),
                "product_quantity": p.get("quantity"),
                "product_total": p.get("total"),
            }

            # Add user details
            user = user_map.get(cart_base["user_id"])
            if user:
                first = user.get("firstName") or ""
                last = user.get("lastName") or ""
                row["first_name"] = first
                row["last_name"] = last
                row["customer_name"] = f"{first} {last}".strip()

                row["email"] = user.get("email")
                addr = user.get("address") or {}
                row["city"] = addr.get("city")

                row["age"] = user.get("age")
                row["gender"] = user.get("gender")
            else:
                row["first_name"] = None
                row["last_name"] = None
                row["customer_name"] = None
                row["email"] = None
                row["city"] = None
                row["age"] = None
                row["gender"] = None

            rows.append(row)

    if not rows:
        return pl.DataFrame([])

    df = pl.DataFrame(rows)

    # ---------------------------------------------------------
    # ADD SYNTHETIC ORDER DATE (distributed across 30 days)
    # ---------------------------------------------------------
    unique_carts = df.select("cart_id").unique().to_series().to_list()
    date_map = {}
    today = datetime.utcnow().date()

    for i, cid in enumerate(sorted(unique_carts)):
        days_back = i % 30
        date_map[cid] = (today - timedelta(days=days_back)).isoformat()

    # Map cart_id → date
    df = df.with_columns(
        pl.col("cart_id")
        .map_elements(lambda x: date_map.get(x), return_dtype=pl.Utf8)
        .alias("order_date"),

        # CLEAN product_title
        pl.col("product_title")
        .str.to_lowercase()
        .str.strip_chars()
        .alias("product_title")
    )

    # ---------------------------------------------------------
    # NORMALIZE TYPES
    # ---------------------------------------------------------
    df = df.with_columns(
        [
            pl.col("cart_id").cast(pl.Int64),
            pl.col("user_id").cast(pl.Int64),
            pl.col("product_id").cast(pl.Int64),
            pl.col("product_title").cast(pl.Utf8),
            pl.col("product_price").cast(pl.Float64),
            pl.col("product_quantity").cast(pl.Int64),
            pl.col("product_total").cast(pl.Float64),
            pl.col("cart_total").cast(pl.Float64),
        ]
    )

    # Save extracted parquet
    out_path = OUT_DIR / "extracted.parquet"
    df.write_parquet(out_path)
    print("Wrote extracted parquet:", out_path)

    return df


if __name__ == "__main__":
    df = extract_from_api()
    print(df.head())
