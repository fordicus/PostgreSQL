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

    PostgreSQL Relational Modeling — Tables & Foreign Keys, Query Clauses

    DDL (Data Definition Language)
        Any SQL that *defines* schema objects—CREATE, ALTER, DROP. We run
        DDL inside `engine.begin()` so changes auto‑commit and remain
        idempotent across script reruns.

    Intent
        • Part 1: Department ↔ Employee foreign‑key actions.  
        • Part 2: Product catalog — projection, filtering, sorting, paging.

    Transaction strategy
        * DDL + bulk inserts   → `engine.begin()`   (auto‑commit)
        * Read‑only queries    → `engine.connect()` (no commit)

    Goals
        - Demonstrate ON DELETE CASCADE & ON UPDATE SET NULL actions.
        - Show parent table alongside child table for clarity.
        - Illustrate SELECT modifiers: WHERE, ORDER BY, LIMIT, OFFSET.
----------------------------------------------------------------------"""

# Core dependencies for SQL execution
from sqlalchemy import create_engine, text
# Result‑display helpers
import pandas as pd
from tabulate import tabulate

engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost/avengers"
)

try:
    # ===============================================================
    # 1. Department / Employee — FK actions demo
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[1-1] Recreate departments & employees tables...")
        conn.execute(text("DROP TABLE IF EXISTS employees;"))
        conn.execute(text("DROP TABLE IF EXISTS departments;"))

        conn.execute(text("""
            CREATE TABLE departments (
                department TEXT PRIMARY KEY
            );
        """))

        # FK constraint fk_dept listens to parent table events:
        #   • ON DELETE CASCADE → delete employees when department removed.
        #   • ON UPDATE SET NULL → set department field NULL when renamed.
        conn.execute(text("""
            CREATE TABLE employees (
                employee_id   SERIAL PRIMARY KEY,
                employee_name TEXT,
                department    TEXT,
                CONSTRAINT fk_dept FOREIGN KEY (department)
                    REFERENCES departments(department)
                    ON DELETE CASCADE
                    ON UPDATE SET NULL
            );
        """))

        print("[1-2] Insert demo rows...")
        conn.execute(text("INSERT INTO departments VALUES ('Engineering'),('Marketing');"))
        conn.execute(text("""
            INSERT INTO employees (employee_name, department) VALUES
            ('Alice','Engineering'), ('Bob','Engineering'), ('Eve','Marketing');
        """))

    with engine.connect() as conn:
        print("[1-3] Parent = departments")
        print(tabulate(pd.read_sql("SELECT * FROM departments;", conn), headers="keys", tablefmt="grid"))
        print("Child = employees (before actions)")
        print(tabulate(pd.read_sql("SELECT * FROM employees ORDER BY employee_id;", conn), headers="keys", tablefmt="grid"))

    with engine.begin() as conn:
        print("[1-4] DELETE Marketing (CASCADE)...")
        conn.execute(text("DELETE FROM departments WHERE department='Marketing';"))

    with engine.connect() as conn:
        print("Departments after delete")
        print(tabulate(pd.read_sql("SELECT * FROM departments;", conn), headers="keys", tablefmt="grid"))
        print("Employees after CASCADE delete — Eve removed")
        print(tabulate(pd.read_sql("SELECT * FROM employees ORDER BY employee_id;", conn), headers="keys", tablefmt="grid"))

    with engine.begin() as conn:
        print("[1-5] UPDATE Engineering → Tech (SET NULL)...")
        conn.execute(text("UPDATE departments SET department='Tech' WHERE department='Engineering';"))

    with engine.connect() as conn:
        print("Departments after rename")
        print(tabulate(pd.read_sql("SELECT * FROM departments;", conn), headers="keys", tablefmt="grid"))
        print("Employees after SET NULL")
        print(tabulate(pd.read_sql("SELECT * FROM employees ORDER BY employee_id;", conn), headers="keys", tablefmt="grid"))

    # ===============================================================
    # 2. Product catalog demo — SELECT clauses
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[2-1] Recreate products table...")
        conn.execute(text("DROP TABLE IF EXISTS products;"))
        conn.execute(text("""
            CREATE TABLE products (
                id       SERIAL PRIMARY KEY,
                name     TEXT,
                price    REAL,
                category TEXT
            );
        """))
        print("[2-2] Insert sample products via executemany()...")
        sql_prod = "INSERT INTO products (name, price, category) VALUES (%s, %s, %s)"
        rows = [
            ("Apple", 0.50, "Fruit"), ("Banana", 0.30, "Fruit"), ("Carrot", 0.20, "Vegetable"),
            ("Donut", 1.20, "Snack"), ("Eggplant", 0.90, "Vegetable"), ("Fig", 1.00, "Fruit"),
            ("Ginger", 1.50, "Spice"), ("Honey", 2.80, "Sweet"), ("Ice Cream", 3.00, "Dessert")
        ]
        with conn.connection.cursor() as cur:
            cur.executemany(sql_prod, rows)

    def show(label: str, sql: str):
        with engine.connect() as conn:
            print("\n" + label)
            df = pd.read_sql(sql, conn)
            print(tabulate(df, headers="keys", tablefmt="grid"))

    show("[SELECT] name, price from all products", "SELECT name, price FROM products;")
    show("[WHERE] price > 1.00", "SELECT name, price FROM products WHERE price > 1.00;")
    show("[ORDER BY] price DESC", "SELECT name, price FROM products ORDER BY price DESC;")
    show("[LIMIT] top 3 expensive items", "SELECT name, price FROM products ORDER BY price DESC LIMIT 3;")
    show("[LIMIT & OFFSET] page 2 (items 4-6)", "SELECT name, price FROM products ORDER BY price DESC LIMIT 3 OFFSET 3;")

    # ===============================================================
    # 3. Cleanup
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[3-1] Drop demo tables...")
        conn.execute(text("DROP TABLE IF EXISTS employees;"))
        conn.execute(text("DROP TABLE IF EXISTS departments;"))
        conn.execute(text("DROP TABLE IF EXISTS products;"))

except Exception as err:
    print("Unexpected error:", err)

finally:
    engine.dispose()
    print("All operations completed. Connection closed.")
