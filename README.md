# 🛒 eCommerce ETL Pipeline (Airflow + Polars + DuckDB + MySQL)

A production-grade ETL pipeline that extracts eCommerce cart data from a public API, transforms it using **Polars** and **DuckDB**, generates analytics reports, and loads the results into a **MySQL database**—automated daily using **Apache Airflow**.

This project was developed and tested on a **VMware Ubuntu Linux machine**.

---

## 🚀 Project Overview

This ETL pipeline pulls shopping cart & user data from:

- https://dummyjson.com/carts  
- https://dummyjson.com/users

### The workflow performs:

### ✔️ Extract
- Fetch cart + user data  
- Flatten cart → product-level rows  
- Add customer details  
- Assign synthetic 30-day `order_date`  
- Save **extracted.parquet**

### ✔️ Transform
- Clean & standardize fields using **Polars**  
- Generate analytics using **DuckDB**:

  - `daily_sales.csv`
  - `revenue_by_product.csv`
  - `customer_summary.csv`

- Save **clean_sales.parquet**

### ✔️ Load
- Load transformed results into MySQL table:  
  **retail_db.transformed_carts**

### ✔️ Orchestrate
- Daily **Apache Airflow DAG**  
  Workflow: **Extract → Transform → Load**

---

## 📁 Project Structure

```
ecommerce_etl/
│── dags/
│     └── retail_sales_api_etl.py
│
│── scripts/
│     ├── extract_api.py
│     ├── transform.py
│     └── load.py
│
│── data/
│     └── processed/
│           ├── extracted.parquet
│           ├── clean_sales.parquet
│           ├── transformed_carts.csv
│           ├── daily_sales.csv
│           ├── revenue_by_product.csv
│           └── customer_summary.csv
│
└── README.md
```

---

## 🛠️ Tech Stack

| Component        | Technology |
|------------------|------------|
| Programming      | Python 3.10+ |
| Orchestration    | Apache Airflow |
| Extraction       | REST API (requests) |
| Transformation   | Polars, DuckDB |
| Database         | MySQL 8 |
| Environment      | Ubuntu (VMware) |
| File Formats     | Parquet, CSV |

---

## 🧱 Architecture Diagram

```
                +--------------------+
                |   DummyJSON API    |
                | carts / users      |
                +---------+----------+
                          |
                 (Extract_from_api)
                          |
                          v
        +---------------------------------------+
        |     extracted.parquet (raw data)      |
        +--------------------+------------------+
                          |
                 (Polars Transform)
                          |
                          v
        +---------------------------------------+
        |         clean_sales.parquet           |
        +--------------------+------------------+
                          |
                (Analytics via DuckDB)
                          |
     +---------------------+----------------------------+
     |                |                 |               |
     v                v                 v               v
daily_sales.csv  revenue_by_product.csv customer_summary.csv
transformed_carts.csv  →  MySQL (Load Task)
```

---

# 🧩 Detailed ETL Steps

## 1️⃣ Extract — `extract_api.py`
- Calls API
- Builds user lookup map
- Explodes cart → product rows
- Adds user info (name, age, email, city, gender)
- Adds synthetic order_date
- Saves **extracted.parquet**

## 2️⃣ Transform — `transform.py`
- Cleans missing values  
- Standardizes product names  
- Calculates totals  
- Converts dates  
- Drops invalid rows  

### Creates analytics using DuckDB:
- `daily_sales.csv`
- `revenue_by_product.csv`
- `customer_summary.csv`

## 3️⃣ Load — `load.py`
- Connects to MySQL (SQLAlchemy + PyMySQL)
- Creates table if not exists
- Loads transformed data into:
  ```
  retail_db.transformed_carts
  ```

## 4️⃣ Airflow Orchestration
DAG file: `dags/retail_sales_api_etl.py`

Daily workflow:

```
extract → transform → load
```

---

# 🛢️ MySQL Table Schema

```sql
CREATE TABLE transformed_carts (
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
```

---

# ⚡ Running the ETL Manually (Without Airflow)

### Extract
```bash
python scripts/extract_api.py
```

### Transform
```bash
python scripts/transform.py
```

### Load
```bash
python scripts/load.py
```

---

# 🌬️ Running with Airflow (Recommended)

### Start Airflow
```bash
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com --firstname X --lastname Y
airflow webserver -p 8080
airflow scheduler
```

Visit:  
👉 **http://localhost:8080**

Enable DAG:  
**retail_sales_api_etl**

---

# ⚙️ Installation & Setup Guide (Ubuntu + VMware)

This guide installs everything required for the ETL pipeline.

---

## 1️⃣ Update Ubuntu

```bash
sudo apt update && sudo apt upgrade -y
```

---

## 2️⃣ Install System Dependencies

```bash
sudo apt install -y python3 python3-pip python3-venv \
    build-essential libssl-dev libffi-dev \
    libmysqlclient-dev default-libmysqlclient-dev \
    curl git
```

---

## 3️⃣ Create Python Virtual Environment

```bash
mkdir ecommerce_etl
cd ecommerce_etl
python3 -m venv venv
source venv/bin/activate
```

---

## 4️⃣ Install Apache Airflow (Inside venv)

```bash
AIRFLOW_VERSION=2.9.2
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

---

## 5️⃣ Install Project Python Packages

```bash
pip install pandas
pip install polars
pip install duckdb
pip install sqlalchemy
pip install pymysql
pip install requests
pip install python-dotenv
pip install apache-airflow-providers-mysql
pip install apache-airflow-providers-http
```

---

## 6️⃣ Initialize Airflow Database

```bash
airflow db init
```

Create admin user:

```bash
airflow users create \
    --username admin \
    --password admin \
    --firstname Shivay \
    --lastname Kumar \
    --role Admin \
    --email admin@example.com
```

---

## 7️⃣ Start Airflow Services

### Terminal 1:
```bash
source venv/bin/activate
airflow scheduler
```

### Terminal 2:
```bash
source venv/bin/activate
airflow webserver -p 8080
```

---

## 8️⃣ Install MySQL Server

```bash
sudo apt install mysql-server -y
```

Start service:

```bash
sudo systemctl start mysql
sudo systemctl enable mysql
```

---

## 9️⃣ Secure MySQL

```bash
sudo mysql_secure_installation
```

---

## 🔟 Create MySQL Database & User

```sql
sudo mysql -u root

CREATE DATABASE retail_db;

CREATE USER 'root'@'localhost' IDENTIFIED BY 'Admin@123';

GRANT ALL PRIVILEGES ON retail_db.* TO 'root'@'localhost';

FLUSH PRIVILEGES;
EXIT;
```

Your connection string:

```
mysql+pymysql://root:Admin%40123@localhost/retail_db
```

(%40 = @)

---

## 1️⃣1️⃣ Install MySQL Client

```bash
sudo apt install default-mysql-client -y
sudo apt install libmysqlclient-dev -y
```

---

## 1️⃣2️⃣ Place ETL Scripts

```
~/airflow/dags/retail_sales_api_etl.py
~/ecommerce_etl/scripts/
~/ecommerce_etl/data/
```

---

## 1️⃣3️⃣ Test Scripts Manually

```bash
python scripts/extract_api.py
python scripts/transform.py
python scripts/load.py
```

---

## 1️⃣4️⃣ Run via Airflow

Enable DAG in UI:

**retail_sales_api_etl**

---

# 📊 Output Files Explained

| File | Purpose |
|------|---------|
| extracted.parquet | Raw API data |
| clean_sales.parquet | Cleaned transformed data |
| transformed_carts.csv | Final load file |
| daily_sales.csv | Daily revenue |
| revenue_by_product.csv | Product analytics |
| customer_summary.csv | Customer spend summary |

---

# 🌱 Future Enhancements
- Add PostgreSQL & Snowflake targets  
- Add dbt models  
- Add S3 storage  
- Build Power BI dashboards  
- Add CI/CD with GitHub Actions  
- Dockerize entire ETL  

---

# 👨‍💻 Author
**Shivay Kumar**  
Built and tested on **Ubuntu (VMware)**.
