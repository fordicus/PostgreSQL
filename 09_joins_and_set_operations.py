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
    PostgreSQL Joins & Set-Operations Demonstration (UNION / INTERSECT / EXCEPT)

    Parameter-Binding Styles (Python 3.9-safe)
      • SQLAlchemy style  :  VALUES (:name) + dict({"name": "Thor"})
      • psycopg2 style    :  VALUES (%s)    + tuple/list ("Thor", )

    🗝 Key Insights
      1. **RIGHT  JOIN** keeps all rows from the right table; unmatched left
         rows become NULL.
      2. **FULL  JOIN** keeps all rows from both tables, aligning by key.
      3. **UNION, INTERSECT, EXCEPT** perform set logic on query results.
      4. Centralised `timed_read_sql()` avoids duplicate timing code.
----------------------------------------------------------------------"""

# ── Core DB execution ───────────────────────────────────────────────
from sqlalchemy import create_engine, text

# ── Display helpers ─────────────────────────────────────────────────
import pandas as pd
from tabulate import tabulate

# ── Utility / typing ────────────────────────────────────────────────
import time
from typing import Optional, Union, Dict, Tuple, Any, List

# ── DB connection ───────────────────────────────────────────────────
engine = create_engine("postgresql+psycopg2://postgres:1234@localhost/avengers")

# Pretty-print helper (simple)
pp = lambda df, title: (print("\n" + title), print(tabulate(df, headers="keys", tablefmt="grid")))

# Central timing helper ------------------------------------------------

def timed_read_sql(
		conn, sql: Union[text, str],
		params: Optional[Dict[str, Any]] = None
	) -> Tuple[pd.DataFrame, float]:
    """Execute *sql* via pandas.read_sql → (DataFrame, elapsed_sec)."""
    t0 = time.perf_counter()
    df = pd.read_sql(sql, conn, params=params or {})
    return df, time.perf_counter() - t0

try:
    # ===============================================================
    # 1. Join Types demo (heroes ↔ abilities)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[1-1] Recreate heroes / abilities …")
        conn.execute(text("DROP TABLE IF EXISTS abilities;"))
        conn.execute(text("DROP TABLE IF EXISTS heroes;"))

        conn.execute(text("""
            CREATE TABLE heroes (
                id   SERIAL PRIMARY KEY,
                name TEXT,
                team TEXT
            );"""))

        conn.execute(text("""
            CREATE TABLE abilities (
                name    TEXT,
                ability TEXT
            );"""))

        print("[1-2] Insert sample rows (executemany) …")
        hero_rows: List[Tuple[str, str]] = [
            ("Iron Man",	"Avengers"),
            ("Thor",		"Avengers"),
            ("Spider-Man",	"Solo"),
            ("Hawkeye",		"Avengers")
        ]
        abil_rows: List[Tuple[str, str]] = [
            ("Iron Man",	"Powered Armor"),
            ("Thor",		"Thunder God"),
            ("Hulk",		"Super Strength"),
            ("Vision",		"Density Control")
        ]
        with conn.connection.cursor() as cur:
            cur.executemany("""
				INSERT INTO heroes (name, team)
				VALUES (%s, %s)
			""", hero_rows)
            cur.executemany("""
				INSERT INTO abilities (name, ability)
				VALUES (%s, %s)
			""", abil_rows)

        #........................................................................
        # 🧠 RIGHT JOIN includes all rows from the right table (abilities),
        # even if there is no matching hero in the left table (heroes).
        #
        # If an ability belongs to a hero not listed in 'heroes', the hero
        # name appears as NULL. This lets us detect unmatched abilities,
        # like Hulk or Vision.
        #
        # 🔍 SQL SYNTAX:
        #     SELECT ...
        #     FROM table_left alias1
        #     RIGHT JOIN table_right alias2
        #     ON alias1.key = alias2.key;
        #
        # 🧩 In this case:
        #   - 'heroes' is LEFT and aliased as h
        #   - 'abilities' is RIGHT and aliased as a
        #   - Aliases use shorthand (e.g., 'heroes h') — same as 'AS h'
        #   - Matching key: h.name = a.name
        #
        # 📌 If 'abilities' has names not in 'heroes', they still appear
        #     with NULLs in hero-related columns.
        #........................................................................

        # RIGHT JOIN: keep all abilities
        df_right, _ = timed_read_sql(conn, text("""
			SELECT		h.name AS hero,	a.ability
            FROM		heroes		h
            RIGHT JOIN	abilities	a
			ON			h.name = a.name;
		"""))
        pp(df_right, "[JOIN] RIGHT JOIN (all abilities)")

        #........................................................................
        # 🧠 FULL JOIN includes all rows from both tables: 'heroes' and
        # 'abilities'. If there's no match on one side, NULLs are used.
        #
        # 🔍 COALESCE defines the value to be picked when NULL.
		# COALESCE(h.name, a.name) picks the first non-NULL value.
        # In this case:
        #   - If h.name is not NULL → use it	 (matched hero)
        #   - If h.name is NULL		→ use a.name (unmatched ability only)
        #
        # ✅ So every row will show a name (from either table) as 'hero',
        # even when no match exists. This keeps the output readable and
        # prevents NULLs in the first column.
        #........................................................................

        # FULL JOIN: keep everything
        df_full, _ = timed_read_sql(conn, text("""
            SELECT COALESCE(h.name, a.name) AS hero, h.team, a.ability
            FROM		heroes		h
            FULL JOIN	abilities	a
			ON h.name = a.name;
		"""))
        pp(df_full, "[JOIN] FULL JOIN (everything)")

    # ===============================================================
    # 2. Set-Operation demo (group_a vs group_b)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[2-1] Recreate group_a / group_b …")
        conn.execute(text("DROP TABLE IF EXISTS group_a;"))
        conn.execute(text("DROP TABLE IF EXISTS group_b;"))

        conn.execute(text("CREATE TABLE group_a (name TEXT);"))
        conn.execute(text("CREATE TABLE group_b (name TEXT);"))

        a_rows = [("Iron Man",), ("Thor",),			("Hulk",),		("Vision",)]
        b_rows = [("Hulk",),	 ("Black Widow",),	("Iron Man",),	("Falcon",)]
        with conn.connection.cursor() as cur:
            cur.executemany("INSERT INTO group_a (name) VALUES (%s)", a_rows)
            cur.executemany("INSERT INTO group_b (name) VALUES (%s)", b_rows)

    with engine.connect() as conn:
        # UNION
        df_union, _ = timed_read_sql(conn,
		"""
            SELECT name FROM group_a
            UNION
            SELECT name FROM group_b;
		""")
        pp(df_union, "[SET] UNION (unique names)")

        # INTERSECT
        df_inter, _ = timed_read_sql(conn,
		"""
            SELECT name FROM group_a
            INTERSECT
            SELECT name FROM group_b;
		""")
        pp(df_inter, "[SET] INTERSECT (common names)")

        # EXCEPT
        df_except, _ = timed_read_sql(conn,
		"""
            SELECT name FROM group_a
            EXCEPT
            SELECT name FROM group_b;
		""")
        pp(df_except, "[SET] EXCEPT (in A not B)")

    # ===============================================================
    # 3. Aggregation demo (power_levels)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[3-1] Power-level aggregation …")
        conn.execute(text("DROP TABLE IF EXISTS power_levels;"))
        conn.execute(text("""
            CREATE TABLE power_levels (
                name  TEXT,
                level INT
            );"""))
        pow_rows = [
            ("Iron Man",	85),
			("Thor",		95),
			("Hulk",		90),
			("Hawkeye",		70)
        ]
        with conn.connection.cursor() as cur:
            cur.executemany(
			"""
				INSERT INTO power_levels (name, level)
				VALUES (%s, %s)
			""", pow_rows)

	#........................................................................
	# 🧠 COUNT(*) returns the total number of rows in the table,
	# including those with NULLs in any column.
	#
	# In this case, it tells us how many heroes exist in the
	# 'power_levels' table — regardless of their actual power level.
	#
	# ✅ This is useful for basic aggregation like getting totals,
	# averages (AVG), or maximum values (MAX).
	#........................................................................

    with engine.connect() as conn:
        df_agg, _ = timed_read_sql(conn, 
		"""
            SELECT
				COUNT(*) AS count,
				AVG(level) AS avg_power,
				MAX(level) AS max_power
            FROM power_levels;
		""")
        pp(df_agg, "[AGG] COUNT / AVG / MAX on power_levels")

    # ===============================================================
    # Cleanup (idempotent) -----------------------------------------
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS power_levels;"))
        conn.execute(text("DROP TABLE IF EXISTS group_a;"))
        conn.execute(text("DROP TABLE IF EXISTS group_b;"))
        conn.execute(text("DROP TABLE IF EXISTS abilities;"))
        conn.execute(text("DROP TABLE IF EXISTS heroes;"))

except Exception as err:
    print("Unexpected error:", err)
finally:
    engine.dispose()
    print("All operations completed. Connection closed.")
