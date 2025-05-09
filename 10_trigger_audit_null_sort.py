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
    PostgreSQL Trigger, NULL‑handling & Advanced Sorting Demonstration

    Parameter‑Binding Styles (Python 3.9‑safe)
      • SQLAlchemy style  : VALUES (:val)  + dict({"val": x})
      • psycopg2 style    : VALUES (%s)    + tuple/list (x,)

    🗝 Key Insights
      1. BEFORE INSERT triggers can auto‑fill audit columns.
      2. `COALESCE` and `NULLIF` help control NULL behaviour in queries.
      3. Custom sort orders via `ORDER BY … NULLS FIRST / LAST` and
         `CASE` expressions.

#........................................................................
	📘 VIEW = A SELECT macro
		- Acts like a reusable SQL snippet.
		- Re-injects the full SELECT every time you query the view.
	
	📦 MATERIALIZED VIEW = A SELECT result cache
		- Stores the query result physically.
		- Must be refreshed manually (REFRESH MATERIALIZED VIEW).
		- Much faster for expensive queries.
		
----------------------------------------------------------------------"""

# ── Core DB execution ───────────────────────────────────────────────
from sqlalchemy import create_engine, text

# ── Analysis / display ──────────────────────────────────────────────
import pandas as pd
from tabulate import tabulate

# ── Utility / typing ────────────────────────────────────────────────
import time, datetime
from typing import Optional, Union, Dict, Tuple, Any, List

engine = create_engine("postgresql+psycopg2://postgres:1234@localhost/avengers")

# Pretty‑print helper -------------------------------------------------
pp = lambda df, title: (print("\n" + title), print(tabulate(df, headers="keys", tablefmt="grid")))

# Central timing helper ----------------------------------------------

def timed_read_sql(conn, sql: Union[text, str],
                   params: Optional[Dict[str, Any]] = None
                   ) -> Tuple[pd.DataFrame, float]:
    """Execute *sql* via pandas.read_sql → (DataFrame, elapsed_sec)."""
    t0 = time.perf_counter()
    df = pd.read_sql(sql, conn, params=params or {})
    return df, time.perf_counter() - t0

try:
    # ===============================================================
    # 1. BEFORE INSERT Trigger demo (audit stamps)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[1-1] Recreate blog_posts & trigger …")
        conn.execute(text("DROP TABLE IF EXISTS blog_posts;"))
        conn.execute(text("DROP FUNCTION IF EXISTS trg_set_timestamp();"))

        conn.execute(text("""
            CREATE TABLE blog_posts (
                id          SERIAL PRIMARY KEY,
                title       TEXT,
                body        TEXT,
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP
            );"""))

        # Trigger function auto‑sets timestamps
        conn.execute(text("""
            CREATE OR REPLACE FUNCTION trg_set_timestamp()
			RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    NEW.created_at := NOW();
                END IF;
                NEW.updated_at := NOW();
                RETURN NEW;
            END$$ LANGUAGE plpgsql;"""))
        conn.execute(text("""
            CREATE TRIGGER ts_audit
			BEFORE INSERT OR UPDATE
			ON blog_posts
            FOR EACH ROW EXECUTE FUNCTION trg_set_timestamp();
		"""))

        print("[1-2] Insert sample posts (executemany) …")
        posts: List[Dict[str, str]] = [
            {"title": "Hello", "body": "First post"},
            {"title": "News",  "body": "Breaking"},
            {"title": "Tips",  "body": "Useful tips"}
        ]
        conn.execute(text("""
            INSERT INTO blog_posts (title, body)
            VALUES (:title, :body);
		"""), posts)

        df_posts, _ = timed_read_sql(conn, "SELECT * FROM blog_posts;")
        pp(df_posts, "[TRG] blog_posts with timestamps")

    # ===============================================================
    # 2. NULL handling demo (COALESCE, NULLIF)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[2-1] Recreate salaries …")
        conn.execute(text("DROP TABLE IF EXISTS salaries;"))
        conn.execute(text(
		"""
            CREATE TABLE salaries (
                name  TEXT,
                base  INT,
                bonus INT
            );
		"""))

        sal_rows = [
            ("Alice", 5000, 500),
            ("Bob",   4500, None),	# NULL bonus
            ("Carol", None, 700)	# NULL base
        ]
        with conn.connection.cursor() as cur:
            cur.executemany(
			"""
				INSERT INTO salaries
				VALUES (%s,%s,%s)
			""", sal_rows)

	# COALESCE defines the value to be picked when NULL.
	# NULLIF defines the value to be NULL upon the specified condition.

    with engine.connect() as conn:
        df_null, _ = timed_read_sql(conn,
		"""
            SELECT name,
				COALESCE(base, 0)	AS base_safe,
				COALESCE(bonus, 0)	AS bonus_safe,
				COALESCE(base, 0) + COALESCE(bonus, 0) AS total_pay,
				NULLIF(bonus, 0)	AS bonus_nullif
            FROM salaries;
		""")
        pp(df_null, "[NULL] COALESCE & NULLIF demo")

    # ===============================================================
    # 3. Custom sorting demo (NULLS LAST, CASE)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[3-1] Recreate tasks …")
        conn.execute(text("DROP TABLE IF EXISTS tasks;"))
        conn.execute(text("""
            CREATE TABLE tasks (
                id       SERIAL PRIMARY KEY,
                title    TEXT,
                priority INT     -- 1 = high, 2 = med, 3 = low, NULL = none
            );
		"""))

        task_rows = [
            ("Deploy", 1), ("Code Review", 2), ("Refactor", None),
            ("Write Docs", 3), ("Meeting", None)
        ]
        with conn.connection.cursor() as cur:
            cur.executemany("""
				INSERT INTO tasks (title, priority)
				VALUES (%s,%s)
			""", task_rows)

    with engine.connect() as conn:
        df_sort, _ = timed_read_sql(conn,
		"""
            SELECT		id, title, priority
            FROM		tasks
            ORDER BY	priority
				NULLS LAST, id;
		""")
        pp(df_sort, "[SORT] priority NULLS LAST")

        # Custom CASE sort: high > med > low > NULL
        df_case, _ = timed_read_sql(conn, """
            SELECT id, title, priority
            FROM tasks
            ORDER BY
				CASE
					WHEN priority = 1 THEN 0
					WHEN priority = 2 THEN 1
					WHEN priority = 3 THEN 2
					ELSE 3
				END, id;
		 """)
        pp(df_case, "[SORT] custom CASE priority")

    # ===============================================================
    # Cleanup (re‑runnable) -----------------------------------------
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS tasks;"))
        conn.execute(text("DROP TABLE IF EXISTS salaries;"))
        conn.execute(text("DROP TABLE IF EXISTS blog_posts;"))

except Exception as err:
    print("Unexpected error:", err)
finally:
    engine.dispose()
    print("All operations completed. Connection closed.")
