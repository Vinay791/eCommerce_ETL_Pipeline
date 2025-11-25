import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"

def load_data():
    csv_path = PROCESSED_DIR / "transformed_data.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found. Run transform first.")

    df = pd.read_csv(csv_path)

    engine = create_engine(
        "mysql+pymysql://root:Admin%40123@localhost/retail_db",
        echo=False
    )

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS transformed_data (
      cart_id INT,
      user_id INT,
      product_id INT,
      product_title TEXT,
      product_price DOUBLE,
      product_quantity INT,
      product_total DOUBLE,
      total_amount DOUBLE,
      customer_name TEXT,
      email TEXT,
      city TEXT,
      order_date DATE
    );
    """

    insert_sql = text("""
        INSERT INTO transformed_data (
            cart_id, user_id, product_id, product_title, product_price,
            product_quantity, product_total, total_amount,
            customer_name, email, city, order_date
        ) VALUES (
            :cart_id, :user_id, :product_id, :product_title, :product_price,
            :product_quantity, :product_total, :total_amount,
            :customer_name, :email, :city, :order_date
        )
    """)

    try:
        rows = df.where(pd.notnull(df), None).to_dict(orient="records")

        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
            conn.execute(insert_sql, rows)

        print("Loaded transformed_data into MySQL successfully.")

    except SQLAlchemyError as e:
        print("Database error:", e)
        raise


if __name__ == "__main__":
    load_data()

