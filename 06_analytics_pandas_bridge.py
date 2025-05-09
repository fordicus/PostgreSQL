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
	PostgreSQL Analytical Queries — Stats · Percentiles · Windows vs Subquery
	+ **NEW:** round-trip demo — edit a DataFrame in pandas and push it back
	  to PostgreSQL with `DataFrame.to_sql()` (SQLAlchemy engine).

	Key insights (🗝)
		1. *pandas* `read_sql()` offers rapid prototyping of statistics
		   directly in Python.  Memory overhead grows with result size—limit
		   via `LIMIT`, chunked reads, or pre-aggregating in SQL.
		2. `PERCENTILE_CONT` (interpolates) vs `PERCENTILE_DISC` (picks
		   nearest value) illustrate how percentile choice matters.
		3. Window functions **annotate** each row (retain all rows) whereas
		   subqueries **filter** rows.  Choose windows when the row detail +
		   group metric must coexist.
		4. Section 4 now shows the reverse path: **edited DataFrame → DB**
		   using `to_sql(if_exists='replace', index=False)`.

	Python 3.9 note: all typing uses `Optional` / `Union`; pipe syntax (`|`)
	is avoided for full 3.9.19 compatibility.
----------------------------------------------------------------------"""

# ── Core dependencies for SQL execution ─────────────────────────────
from sqlalchemy import create_engine, text

# ── Data analysis / display ─────────────────────────────────────────
import pandas as pd
from tabulate import tabulate

# ── Timing & typing utilities (Py 3.9-friendly) ─────────────────────
import time
from typing import Optional, Union, Dict, Tuple, Any

# ── PostgreSQL connection URI ───────────────────────────────────────
engine = create_engine(
	"postgresql+psycopg2://postgres:1234@localhost/avengers")

# Pretty-print helper -------------------------------------------------
def pp(df: pd.DataFrame, title: str) -> None:
	"""ASCII-safe tabulate output."""
	print("\n" + title)
	print(tabulate(df, headers="keys", tablefmt="grid"))

# Central timing helper ----------------------------------------------

def timed_read_sql(conn, sql: Union[text, str],
				   params: Optional[Dict[str, Any]] = None
				   ) -> Tuple[pd.DataFrame, float]:
	"""Execute *sql* via pandas.read_sql → (DataFrame, elapsed_sec)."""
	t0 = time.perf_counter()
	df = pd.read_sql(sql, conn, params=params or {})
	return df, time.perf_counter() - t0

# ---------------------------------------------------------------------
try:
	# ===========================================================
	# 1. Price statistics demo (pandas read_sql) 🗝
	# ===========================================================
	with engine.begin() as conn:
		conn.execute(text("DROP TABLE IF EXISTS products_stats;"))
		conn.execute(text("""
			CREATE TABLE products_stats (
				id SERIAL PRIMARY KEY,
				name TEXT,
				price REAL,
				category TEXT
			);"""))
		rows = [
			("Apple", 0.5, "Fruit"), ("Banana", 0.3, "Fruit"),
			("Carrot", 0.2, "Vegetable"), ("Donut", 1.2, "Snack"),
			("Eggplant", 0.9, "Vegetable"), ("Fig", 1.0, "Fruit"),
			("Ginger", 1.5, "Spice"), ("Honey", 2.8, "Sweet"),
			("Ice Cream", 3.0, "Dessert")
		]
		with conn.connection.cursor() as cur:
			cur.executemany(
				"INSERT INTO products_stats (name,price,category) VALUES (%s,%s,%s)",
				rows)

	with engine.connect() as conn:
		df_prod, _ = timed_read_sql(conn, "SELECT * FROM products_stats;")
	pp(df_prod, "[Stats] Full product table")

	# Quick numeric summary (⚠ big tables → aggregate first!)
	pp(df_prod["price"].describe().to_frame().T, "[Stats] price.describe()")
	pp(df_prod.groupby("category")["price"].mean().reset_index(),
	   "[Stats] Avg price by category")

	# ===========================================================
	# 2. Percentile & Top-N via window + CTE 🗝
	# ===========================================================
	with engine.begin() as conn:
		conn.execute(text("DROP TABLE IF EXISTS avengers_power;"))
		conn.execute(text("""
			CREATE TABLE avengers_power (
				emp_id INT,
				name   TEXT,
				alias  TEXT,
				power_strength INT
			);"""))
		with conn.connection.cursor() as cur:
			cur.executemany(
				"INSERT INTO avengers_power VALUES (%s,%s,%s,%s)",
				[
					(1, "Iron Man", "Genius Billionaire", 88),
					(2, "Captain America", "Super Soldier", 79),
					(3, "Hulk", "Green Giant", 92),
					(4, "Black Widow", "Spy", 65),
					(5, "Thor", "God of Thunder", 95),
					(6, "Hawkeye", "Archer", 64)
				])
	
	#............................................................................
	# A CTE (Common Table Expression) is a named temporary result set
	# that you define at the beginning of a SQL query using the WITH clause.
	# You can think of it like a mini table that you build first, and then
	# use right away in your main query.
	#............................................................................
	# Imagine we're looking at superhero power levels.
	# We want to know two things:
	#   - What's the "median" power (middle value)? We calculate this in 2 ways:
	#	 → PERCENTILE_CONT: "real average middle" using math between numbers.
	#	 → PERCENTILE_DISC: "actual value" that someone really has.
	#   - Who are the Top 3 strongest heroes? We'll rank them using RANK().
	#
	# This big query does 3 things at once:
	# 1. Makes a temporary table `p` that stores the two percentiles.
	# 2. Picks all the heroes and attaches their percentile values.
	# 3. RANK() puts them in order from strongest to weakest.
	#............................................................................
	# 🧠 Why do we need "WITHIN GROUP (ORDER BY ...)"?
	# Because percentile functions like PERCENTILE_CONT and PERCENTILE_DISC
	# need to know how to sort the data before calculating "middle values."
	# It's like asking: "Give me the median score" — but we must first say
	# what order to look at!
	#............................................................................
	
	pct_sql = text("""
		WITH p AS (
			SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY power_strength) AS pct_cont,
				   PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY power_strength) AS pct_disc
			FROM avengers_power)
		SELECT name, alias, power_strength,
			   RANK() OVER (ORDER BY power_strength DESC) AS rnk,
			   p.pct_cont, p.pct_disc
		FROM avengers_power, p
		ORDER BY power_strength DESC LIMIT 3;""")

	# Variables defined inside `with` blocks remain accessible outside.
	# `with` is for resource management, not variable scoping.

	with engine.connect() as conn:
		df_av = pd.read_sql("SELECT * FROM avengers_power;", conn)
		df_pct, _ = timed_read_sql(conn, pct_sql)
		
	pp(df_av,  "[Avengers] Full table")
	pp(df_pct, "[Avengers] Top-3 + percentiles")

	# ===========================================================
	# 3. Subquery vs window (age example) 🗝
	# ===========================================================
	with engine.begin() as conn:
		conn.execute(text("DROP TABLE IF EXISTS people_win;"))
		conn.execute(text("""
			CREATE TABLE people_win (
				id SERIAL PRIMARY KEY,
				name TEXT,
				age  INT,
				gender TEXT
			);"""))
		with conn.connection.cursor() as cur:
			cur.executemany(
				"INSERT INTO people_win (name,age,gender) VALUES (%s,%s,%s)",
				[("Alice", 30, "F"), ("Bob", 40, "M"), ("Carol", 35, "F"),
				 ("Dave", 50, "M"), ("Eve", 28, "F")])

	with engine.connect() as conn:
		
		#............................................................................
		# A subquery is a query inside another query that helps filter
		# or calculatedata for the main query.
		#
		# Now we're looking at people and their ages.
		# We want to know: who is older than average?
		#
		# Step-by-step:
		#   → First, find the average age of all people.
		#   → Then, keep only people whose age is greater than that.
		#
		# This query uses a subquery: (SELECT AVG(age) ...) is like asking
		# “what's the class average?” and comparing each person's age to it.
		#
		#............................................................................
		# A window function lets you keep each row, while also attaching
		# extra info (like group averages, ranks, etc.) based on other rows.
		#
		# Now we want to label each person with their group's average age.
		# Group = gender (M/F), so everyone in same gender group gets the same label.
		#
		# This is called a window function. It works like this:
		#   - Go through each row (one person at a time).
		#   - Look at others in their group (same gender).
		#   - Write down the average age of the group next to their row.
		#
		# Unlike the subquery example above, this doesn't throw away any rows.
		# We keep all of them and just “add a note” about their group.
		#............................................................................
		
		df_sub, _ = timed_read_sql(
			conn,
			"""SELECT name, age FROM people_win
				 WHERE age > (SELECT AVG(age) FROM people_win);""")
		df_win, _ = timed_read_sql(
			conn,
			"""SELECT name, age, gender,
					   ROUND(AVG(age) OVER (PARTITION BY gender),2) AS avg_by_gender
				 FROM people_win;""")
	pp(df_sub, "[People] Older than global avg (subquery)")
	pp(df_win, "[People] Avg age by gender (window)")

	# ===========================================================
	# 4. round-trip DataFrame → DB table via to_sql 🗝
	# ===========================================================
	df_discount = df_prod.copy()
	df_discount["discount_price"] = (df_discount["price"] * 0.9).round(2)

	with engine.begin() as conn:
		print("\n" + "-" * 70)
		print("[4-1] Write discounted DataFrame back via to_sql …")
		df_discount.to_sql(
			"products_discount", conn, if_exists="replace", index=False)

	with engine.connect() as conn:
		df_check, _ = timed_read_sql(conn, "SELECT * FROM products_discount;")
	pp(df_check, "[DB] products_discount via to_sql() round-trip")

	# ===========================================================
	# 5. Cleanup ------------------------------------------------
	with engine.begin() as conn:
		for tbl in ("products_stats", "avengers_power", "people_win",
					 "products_discount"):
			conn.execute(text(f"DROP TABLE IF EXISTS {tbl};"))

except Exception as e:
	print("Unexpected error:", e)
finally:
	engine.dispose()
	print("All operations completed. Connection closed.")
