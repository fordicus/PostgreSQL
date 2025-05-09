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

	PostgreSQL Normalization & Schema Constraints

	Goals:
		- 🗝 Enforce *Composite Primary Key* integrity (many-to-many)
		- 🗝 Demonstrate **1 NF** fix by splitting multi-valued attributes
		- 🗝 Demonstrate **2 NF** fix by removing partial dependencies
		- 🗝 Demonstrate **3 NF** fix by removing transitive dependencies

	Key Concepts:
		- `executemany()` for efficient batch inserts
		- Explicit transaction block with manual `rollback()` control
		- Foreign-key usage for 3 NF
		- Pandas + tabulate for clear result display
----------------------------------------------------------------------"""

# 📦 Core dependencies for SQL execution
from sqlalchemy import create_engine, text

# 🧪 Used only for tabulated result visualization
import pandas as pd
from tabulate import tabulate

# PostgreSQL connection URI
engine = create_engine(
	"postgresql+psycopg2://postgres:1234@localhost/avengers"
)

try:
	# ──────────────────────────────────────────────────────────────────
	# 1. Composite Primary Key demonstration
	# ──────────────────────────────────────────────────────────────────
	with engine.begin() as conn:

		print("\n[1-1] Drop existing 'course_enrollments' table.")
		conn.execute(text("DROP TABLE IF EXISTS course_enrollments;"))

		print("[1-2] Create table with composite primary key.")
		conn.execute(text("""
			CREATE TABLE course_enrollments (
				student_id INTEGER,          -- INTEGER: 32-bit signed int
				course_id  TEXT,             -- TEXT: variable-length string
				enrolled_on DATE DEFAULT CURRENT_DATE,
				PRIMARY KEY (student_id, course_id)
			);
		"""))

		print("[1-3] Insert valid records via executemany().")
		sql_insert = """
			INSERT INTO course_enrollments (student_id, course_id)
			VALUES (%s, %s);
		"""
		values = [
			(101, "CS101"), (101, "MATH202"),
			(102, "CS101"), (103, "PHYS303")
		]
		with conn.connection.cursor() as cur:
			cur.executemany(sql_insert, values)

	# Manual transaction block to trigger rollback
	with engine.connect() as conn:
		trans = conn.begin()
		try:
			print("[1-4] Attempt duplicate insert (should fail).")
			conn.execute(text("""
				INSERT INTO course_enrollments (student_id, course_id)
				VALUES (101, 'CS101');
			"""))
			trans.commit()
		except Exception as e:
			print("\n[Constraint Violation Blocked]")
			print("Error:", e)
			trans.rollback()

	with engine.begin() as conn:
		print("\n[1-5] Final SELECT from course_enrollments.")
		res = conn.execute(text("""
			SELECT * FROM course_enrollments
			ORDER BY student_id;
		"""))
		df = pd.DataFrame(res.fetchall(), columns=res.keys())
		print(tabulate(df, headers="keys", tablefmt="grid"))

		print("[1-6] Drop table for cleanup.")
		conn.execute(text("DROP TABLE IF EXISTS course_enrollments;"))
		
	print(f"\n------------------------------------------------------"
		f"------------------------------------------------------"
	)

	# ──────────────────────────────────────────────────────────────────
	# 2. First-Normal-Form (1 NF) demonstration
	# ──────────────────────────────────────────────────────────────────
	with engine.begin() as conn:

		print("\n[2-1] Drop old order tables (if any).")
		conn.execute(text("DROP TABLE IF EXISTS orders_unnormalized;"))
		conn.execute(text("DROP TABLE IF EXISTS orders_1nf;"))

		print("[2-2] Create unnormalized table.")
		conn.execute(text("""
			CREATE TABLE orders_unnormalized (
				order_id SERIAL PRIMARY KEY, -- SERIAL: auto-increment int
				customer TEXT,               -- Customer name
				items    TEXT                -- CSV list ❌ violates 1 NF
			);
		"""))

		print("[2-3] Insert sample rows via executemany().")
		sql_insert_raw = """
			INSERT INTO orders_unnormalized (customer, items)
			VALUES (%s, %s);
		"""
		raw_orders = [
			("Tony",  "Arc Reactor, Suit, AI"),
			("Steve", "Shield"),
			("Peter", "Web-Shooter, Suit")
		]
		with conn.connection.cursor() as cur:
			cur.executemany(sql_insert_raw, raw_orders)

		print("[2-4] Show unnormalized data.")
		res_raw = conn.execute(text("SELECT * FROM orders_unnormalized;"))
		df_raw = pd.DataFrame(res_raw.fetchall(), columns=res_raw.keys())
		print(tabulate(df_raw, headers="keys", tablefmt="grid"))

		print("[2-5] Create 1 NF-compliant table.")
		conn.execute(text("""
			CREATE TABLE orders_1nf (
				order_id INTEGER,
				customer TEXT,
				item     TEXT,
				PRIMARY KEY (order_id, item)
			);
		"""))

		print("[2-6] Normalize data into orders_1nf.")
		records_1nf = []
		for _, row in df_raw.iterrows():
			items = [it.strip() for it in row["items"].split(",")]
			for item in items:
				records_1nf.append(
					dict(order_id=row["order_id"],
						 customer=row["customer"],
						 item=item)
				)

		conn.execute(
			text("""
				INSERT INTO orders_1nf (order_id, customer, item)
				VALUES (:order_id, :customer, :item);
			"""),
			records_1nf
		)

		print("[2-7] Display normalized orders.")
		res_norm = conn.execute(text("""
			SELECT * FROM orders_1nf
			ORDER BY order_id, item;
		"""))
		df_norm = pd.DataFrame(res_norm.fetchall(), columns=res_norm.keys())
		print(tabulate(df_norm, headers="keys", tablefmt="grid"))

		print("[2-8] Drop tables for cleanup.")
		conn.execute(text("DROP TABLE IF EXISTS orders_unnormalized;"))
		conn.execute(text("DROP TABLE IF EXISTS orders_1nf;"))
		
	print(f"\n------------------------------------------------------"
		f"------------------------------------------------------"
	)

	# ──────────────────────────────────────────────────────────────────
	# 3. Second-Normal-Form (2 NF) demonstration
	# ──────────────────────────────────────────────────────────────────
	with engine.begin() as conn:

		print("\n[3-1] Drop 2 NF demo tables (if any).")
		conn.execute(text("DROP TABLE IF EXISTS orders_unnorm_2nf;"))
		conn.execute(text("DROP TABLE IF EXISTS customers;"))
		conn.execute(text("DROP TABLE IF EXISTS orders_2nf;"))

		print("[3-2] Create 2 NF-violating table.")
		conn.execute(text("""
			CREATE TABLE orders_unnorm_2nf (
				order_id      INTEGER,       -- Order identifier
				product       TEXT,          -- Product name
				customer_name TEXT           -- ✖ partial dependency
			);
		"""))

		print("[3-3] Insert sample rows via executemany().")
		sql_insert_2nf = """
			INSERT INTO orders_unnorm_2nf
				(order_id, product, customer_name)
			VALUES (%s, %s, %s);
		"""
		rows_2nf = [
			(1, "apple",  "Alice"),
			(1, "banana", "Alice"),
			(2, "carrot", "Bob"),
			(3, "donut",  "Carol"),
			(3, "fig",    "Carol")
		]
		with conn.connection.cursor() as cur:
			cur.executemany(sql_insert_2nf, rows_2nf)

		print("[3-4] Show 2 NF-violating table.")
		df_2nf_raw = pd.read_sql(
			"SELECT * FROM orders_unnorm_2nf;", conn
		)
		print(tabulate(df_2nf_raw, headers="keys", tablefmt="grid"))

		print("[3-5] Create customers table.")
		conn.execute(text("""
			CREATE TABLE customers (
				order_id      INTEGER PRIMARY KEY,
				customer_name TEXT
			);
		"""))

		print("[3-6] Create orders_2nf table.")
		conn.execute(text("""
			CREATE TABLE orders_2nf (
				order_id INTEGER,
				product  TEXT,
				PRIMARY KEY (order_id, product)
			);
		"""))

		# 🗝 Split partial dependency → 2 NF compliant tables
		print("[3-7] Normalize into customers and orders_2nf.")

		# ── Pandas transformation (2 NF) ───────────────────────────────
		# The object `df_2nf_raw[["order_id", "customer_name"]]` is a
		# `pandas.DataFrame` selecting just two columns. We apply the
		# `DataFrame.drop_duplicates()` method to remove redundant rows
		# (e.g., "Alice" appearing multiple times for order_id 1).
		#
		# We then call `DataFrame.to_dict("records")`, which converts the
		# DataFrame into a list of dictionaries—each dictionary representing
		# a single row, such as: `{ "order_id": 1, "customer_name": "Alice" }`.
		#
		# This list of dictionaries is directly passed as the second argument
		# to `conn.execute(...)`. SQLAlchemy interprets this structure as a
		# batch-insert input, binding each dictionary's keys (like
		# `order_id`, `customer_name`, `product`) to the corresponding
		# `:named_placeholder` in the SQL `text(...)`. This is how multiple
		# rows are inserted efficiently into `customers` and `orders_2nf`
		# tables using a single `execute()` call per table.

		df_customers = (
			df_2nf_raw[["order_id", "customer_name"]]
			.drop_duplicates()
			.to_dict("records")
		)
		df_products = (
			df_2nf_raw[["order_id", "product"]]
			.to_dict("records")
		)
		# ────────────────────────────────────────────────────────────

		conn.execute(
			text("""
				INSERT INTO customers (order_id, customer_name)
				VALUES (:order_id, :customer_name);
			"""),
			df_customers
		)
		conn.execute(
			text("""
				INSERT INTO orders_2nf (order_id, product)
				VALUES (:order_id, :product);
			"""),
			df_products
		)

		print("[3-8] Display normalized tables.")
		df_cust = pd.read_sql(
			"SELECT * FROM customers ORDER BY order_id;", conn
		)
		df_prod = pd.read_sql(
			"SELECT * FROM orders_2nf ORDER BY order_id;", conn
		)
		print("\n-- customers (holds customer_name) --")
		print(tabulate(df_cust, headers="keys", tablefmt="grid"))
		print("\n-- orders_2nf (2 NF satisfied) --")
		print(tabulate(df_prod, headers="keys", tablefmt="grid"))

		print("[3-9] Drop tables for cleanup.")
		conn.execute(text("DROP TABLE IF EXISTS orders_unnorm_2nf;"))
		conn.execute(text("DROP TABLE IF EXISTS customers;"))
		conn.execute(text("DROP TABLE IF EXISTS orders_2nf;"))
		
	print(f"\n------------------------------------------------------"
		f"------------------------------------------------------"
	)

	# ──────────────────────────────────────────────────────────────────
	# 4. Third-Normal-Form (3 NF) demonstration
	# ──────────────────────────────────────────────────────────────────
	with engine.begin() as conn:

		print("\n[4-1] Drop 3 NF demo tables (if any).")
		conn.execute(text("DROP TABLE IF EXISTS employees_3nf;"))
		conn.execute(text("DROP TABLE IF EXISTS departments CASCADE;"))
		conn.execute(text("DROP TABLE IF EXISTS employees_unnormalized;"))

		print("[4-2] Create 3 NF-violating employees table.")
		conn.execute(text("""
			CREATE TABLE employees_unnormalized (
				emp_id     INTEGER,
				emp_name   TEXT,
				dept_id    INTEGER,
				dept_name  TEXT           -- ✖ transitive dependency
			);
		"""))

		print("[4-3] Insert sample rows via executemany().")
		sql_emp_raw = """
			INSERT INTO employees_unnormalized (
				emp_id, emp_name, dept_id, dept_name
			)
			VALUES (%s, %s, %s, %s);
		"""
		emp_rows = [
			(1, "Alice",  10, "Engineering"),
			(2, "Bob",    20, "Marketing"),
			(3, "Carol",  10, "Engineering"),
			(4, "David",  30, "Sales"),
			(5, "Eve",    20, "Marketing")
		]
		with conn.connection.cursor() as cur:
			cur.executemany(sql_emp_raw, emp_rows)

		print("[4-4] Show 3 NF-violating table.")
		df_emp_raw = pd.read_sql(
			"SELECT * FROM employees_unnormalized;", conn
		)
		print(tabulate(df_emp_raw, headers="keys", tablefmt="grid"))

		print("[4-5] Create departments table (dept_id → dept_name).")
		conn.execute(text("""
			CREATE TABLE departments (
				dept_id   INTEGER PRIMARY KEY,
				dept_name TEXT
			);
		"""))

		# ── Pandas transformation (3 NF) ────────────────────────────
		# Extract unique departments first.
		dept_records = (
			df_emp_raw[["dept_id", "dept_name"]]
			.drop_duplicates()
			.to_dict("records")
		)
		conn.execute(
			text("""
				INSERT INTO departments (dept_id, dept_name)
				VALUES (:dept_id, :dept_name);
			"""),
			dept_records
		)
		# Prepare employee rows without dept_name.
		emp_records = (
			df_emp_raw[["emp_id", "emp_name", "dept_id"]]
			.to_dict("records")
		)
		# ────────────────────────────────────────────────────────────

		print("[4-6] Create employees_3nf table with FK.")
		conn.execute(text("""
			CREATE TABLE employees_3nf (
				emp_id    INTEGER PRIMARY KEY,
				emp_name  TEXT,
				dept_id   INTEGER REFERENCES departments(dept_id)
			);
		"""))
		conn.execute(
			text("""
				INSERT INTO employees_3nf (
					emp_id, emp_name, dept_id
				)
				VALUES (:emp_id, :emp_name, :dept_id);
			"""),
			emp_records
		)

		print("[4-7] Display normalized tables.")
		df_dept = pd.read_sql(
			"SELECT * FROM departments ORDER BY dept_id;", conn
		)
		df_emp = pd.read_sql(
			"SELECT * FROM employees_3nf ORDER BY emp_id;", conn
		)
		print("\n-- departments (holds dept_name) --")
		print(tabulate(df_dept, headers="keys", tablefmt="grid"))
		print("\n-- employees_3nf (3 NF satisfied + FK) --")
		print(tabulate(df_emp, headers="keys", tablefmt="grid"))

		print("[4-8] Drop tables for cleanup.")
		conn.execute(text("DROP TABLE IF EXISTS employees_3nf;"))
		conn.execute(text("DROP TABLE IF EXISTS departments;"))
		conn.execute(text("DROP TABLE IF EXISTS employees_unnormalized;"))

except Exception as e:
	print("\nError occurred:", e)

finally:
	engine.dispose()
	print("\nAll operations completed. Connection closed.")
