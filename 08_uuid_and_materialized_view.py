#!/usr/bin/env python3
r"""----------------------------------------------------------------------
	Basic PostgreSQL Commands (For CLI Interaction)

	0. 📆 Create a New Database
		# Connect using superuser and create DB
		set PGPASSWORD=1234
		psql -U postgres -c "CREATE DATABASE avengers;"

	1. 🔐 Login to PostgreSQL
		# General login
		set PGPASSWORD=1234
		psql -U postgres

		# Login to a specific DB
		set PGPASSWORD=1234
		psql -U postgres -d avengers

	2. 🔍 Investigate Databases or Schema Objects
		\l				-- List all databases
		\dt+			-- List all tables and details in current schema
		\d tablename	-- Describe structure of specific table

	3. 💣 Drop All Objects in Current Schema
		# WARNING: drops all tables, views, functions in 'public' schema
		DROP SCHEMA public CASCADE;
		CREATE SCHEMA public;

	4. 🩹 Drop an Entire Database (PostgreSQL ≥ 13)
		set PGPASSWORD=1234
		psql -U postgres -c "DROP DATABASE avengers WITH (FORCE);"
		
----------------------------------------------------------------------
    PostgreSQL UUID & MATERIALIZED VIEW Demonstration

    Parameter-Binding Styles
      • SQLAlchemy style (recommended)
          INSERT ... VALUES (:col)  + dict({"col": value})
      • psycopg2 style (low-level)
          INSERT ... VALUES (%s)     + tuple/list (value, )

    🗝 Key Insights
      1. PostgreSQL has a native UUID type for primary keys.
      2. MATERIALIZED VIEW stores data and must be refreshed manually,
         unlike a regular VIEW.

#........................................................................
	📘 VIEW = A SELECT macro
		- Acts like a reusable SQL snippet.
		- Re-injects the full SELECT every time you query the view.
	
	📦 MATERIALIZED VIEW = A SELECT result cache
		- Stores the query result physically.
		- Must be refreshed manually (REFRESH MATERIALIZED VIEW).
		- Much faster for expensive queries.

----------------------------------------------------------------------"""

# ── Core dependencies for SQL execution ─────────────────────────────
from sqlalchemy import create_engine, text

# ── Analysis / display ──────────────────────────────────────────────
import pandas as pd
from tabulate import tabulate

# ── Utilities ───────────────────────────────────────────────────────
import time, uuid, json
from typing import Tuple, Dict, Any, Optional, Union

# PostgreSQL engine (edit credentials as needed)
engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost/avengers")

# Pretty-print helper -------------------------------------------------
pp = lambda df, title: (
    print("\n" + title),
    print(tabulate(df, headers="keys", tablefmt="grid"))
)

# Central timing helper ----------------------------------------------

def timed_read_sql(conn,
		sql: Union[text, str],
		params: Optional[Dict[str, Any]] = None
	) -> Tuple[pd.DataFrame, float]:
    """Run *sql* via pandas.read_sql → (DataFrame, elapsed_sec)."""
    t0 = time.perf_counter()
    df = pd.read_sql(sql, conn, params=params or {})
    return df, time.perf_counter() - t0

try:
    # ===============================================================
    # 1. UUID Primary-Key demo
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[1-1] Recreate students_uuid …")
        conn.execute(text("DROP TABLE IF EXISTS students_uuid;"))
        conn.execute(text("""
            CREATE TABLE students_uuid (
                id    UUID PRIMARY KEY,
                name  TEXT NOT NULL,
                major TEXT
            );"""))

        print("[1-2] Insert 5 students with uuid4 … (executemany)")
        stu_rows = [
            {"id": uuid.uuid4(), "name": "Alice", "major": "Math"},
            {"id": uuid.uuid4(), "name": "Bob", "major": "CS"},
            {"id": uuid.uuid4(), "name": "Carol", "major": "Physics"},
            {"id": uuid.uuid4(), "name": "Dave", "major": "Chemistry"},
            {"id": uuid.uuid4(), "name": "Eve", "major": "Biology"}
        ]
        conn.execute(
            text("""
                INSERT INTO students_uuid (id, name, major)
                VALUES (:id, :name, :major);"""),
            stu_rows
        )

        df_stu, _ = timed_read_sql(conn, "SELECT * FROM students_uuid;")
        pp(df_stu, "[UUID] Students table")

    # ===============================================================
    # 2. MATERIALIZED VIEW demo
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[2-1] Recreate products & views …")
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS cheap_products_mv;"))
        conn.execute(text("DROP VIEW IF EXISTS cheap_products_view;"))
        conn.execute(text("DROP TABLE IF EXISTS products;"))

        conn.execute(text("""
            CREATE TABLE products (
                id    SERIAL PRIMARY KEY,
                name  TEXT,
                price INT
            );"""))

        print("[2-2] Bulk insert products via executemany() …")
        prod_rows = [
            ("Hammer", 50), ("Shield", 150), ("Suit", 500),
            ("Helmet", 90), ("Goggles", 40), ("Boots", 110)
        ]
        with conn.connection.cursor() as cur:
            cur.executemany(
                "INSERT INTO products (name, price) VALUES (%s, %s)",
                prod_rows
            )

        conn.execute(text("""
            CREATE VIEW cheap_products_view AS
            SELECT * FROM products WHERE price < 100;"""))

        conn.execute(text("""
            CREATE MATERIALIZED VIEW cheap_products_mv AS
            SELECT * FROM products WHERE price < 100;"""))

    # Helper to compare VIEW vs MV -----------------------------------
    def show_views(tag: str):
        with engine.connect() as c:
            df_v, _  = timed_read_sql(c, "SELECT * FROM cheap_products_view;")
            df_mv, _ = timed_read_sql(c, "SELECT * FROM cheap_products_mv;")
        pp(df_v, f"[VIEW] {tag}")
        pp(df_mv, f"[MV]   {tag}")

    show_views("Initial state")

    # Update underlying table ---------------------------------------
    with engine.begin() as conn:
        conn.execute(text("UPDATE products SET price = 120 WHERE name = 'Helmet';"))

    show_views("After price update (MV stale)")

    # Refresh materialized view -------------------------------------
    with engine.begin() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW cheap_products_mv;"))

    show_views("After REFRESH MATERIALIZED VIEW")

    # ===============================================================
    # Cleanup (re-runnable) -----------------------------------------
    with engine.begin() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS cheap_products_mv;"))
        conn.execute(text("DROP VIEW IF EXISTS cheap_products_view;"))
        conn.execute(text("DROP TABLE IF EXISTS products;"))
        conn.execute(text("DROP TABLE IF EXISTS students_uuid;"))

except Exception as err:
    print("Unexpected error:", err)
finally:
    engine.dispose()
    print("All operations completed. Connection closed.")
