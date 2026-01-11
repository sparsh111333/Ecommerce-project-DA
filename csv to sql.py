import pandas as pd
import sqlalchemy as sa
import pyodbc
import os

# ============================
# SQL Server details
# ============================
SERVER = ".\\SQLEXPRESS"
DATABASE = "ecommerce"

# ============================
# CSV folder path
# ============================
CSV_FOLDER = r"C:/Users/sparsh sharma/OneDrive/Desktop/yt py+sql p3main"

# ============================
# Create SQL connection
# ============================
engine = sa.create_engine(
    f"mssql+pyodbc://@{SERVER}/{DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

print("✅ Connected to SQL Server")

# ============================
# CSV → Table mapping
# ============================
csv_table_map = {
    "customers (1).csv": "customers",
    "orders (1).csv": "orders",
    "products (1).csv": "products",
    "geolocation.csv": "geolocation",
    "order_items.csv": "order_items",
    "payments.csv": "payments",
    "sellers.csv": "sellers"
}

# ============================
# Load CSVs into SQL
# ============================
for csv_file, table_name in csv_table_map.items():
    csv_path = os.path.join(CSV_FOLDER, csv_file)

    print(f"\nLoading {csv_file} → {table_name}")

    df = pd.read_csv(csv_path)

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    print(f"✅ {table_name} loaded ({len(df)} rows)")

print("\n🎉 All tables transferred successfully")
