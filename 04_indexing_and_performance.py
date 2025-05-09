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

    PostgreSQL Indexing & Performance ­— One-Stop Demonstration

    ❖ Intent (Why this script exists)
        This example measures the effect of adding a B-Tree index on a
        selective lookup and shows where total query latency is spent.
        Step-by-step timing is captured **before** and **after** index
        creation, so learners can see the real-world performance gap.

    ❖ Transaction strategy
        • DDL and bulk-insert sections use ``engine.begin()`` so changes
          are committed atomically.
        • Read-only timing/profiling sections use ``engine.connect()``
          because no commit is required; avoiding an extra transaction
          keeps measurements clean.

    Goals:
        - 🗝 Show how a B-Tree index converts sequential scan to index scan
        - 🗝 Time query performance and use EXPLAIN ANALYZE to find
          bottlenecks
        - 🗝 Compare DB execution time vs client-side fetch time

    Key Concepts:
        - CREATE INDEX / DROP INDEX syntax and options
        - EXPLAIN ANALYZE output interpretation
        - ``time.perf_counter()`` for micro-timing in Python
        - pandas + tabulate for readable result display
----------------------------------------------------------------------"""

# 📦 Core dependencies for SQL execution
from sqlalchemy import create_engine, text, Connection

# 🧮 Timing utility
import time
from typing import Any, Dict, List, Tuple

# 🧪 Used only for tabulated result visualization
import pandas as pd
from tabulate import tabulate

# Added Optional for legacy Python type compatibility
from typing import Any, Dict, List, Tuple, Optional

# PostgreSQL engine (edit credentials as needed)
engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost/avengers"
)

# -------------------------------------------------------------------
# Helper for micro-timing
# -------------------------------------------------------------------

def timed(conn: Connection,
		sql: str,
		params: Optional[Dict[str, Any]] = None
	) -> Tuple[List[Tuple[Any, ...]], float]:
    """Return rows and elapsed time (ms) for *sql* executed with *params*.

    ``params or {}`` – if *params* is ``None`` we pass **an empty dict** to
    ``conn.execute``. SQLAlchemy still requires a mapping for positional
    replacement, so the logical *OR* provides a safe fallback.
    """
    start = time.perf_counter()
    res = conn.execute(text(sql), params or {})
    elapsed = (time.perf_counter() - start) * 1000  # milliseconds
    return res.fetchall(), elapsed

try:
    # ===============================================================
    # 1. Data Seed & Baseline Setup
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[1-1] Drop 'products' table & index if present …")
        conn.execute(text("DROP INDEX IF EXISTS idx_products_name;"))
        conn.execute(text("DROP TABLE IF EXISTS products;"))

        print("[1-2] Create 'products' table …")
        # TEXT vs VARCHAR(n): TEXT has no length check and is slightly
        # faster; VARCHAR enforces a max length and stores that limit in
        # the catalog.  For product names we choose TEXT for flexibility.
        conn.execute(text("""
            CREATE TABLE products (
                id       SERIAL PRIMARY KEY,
                name     TEXT,
                price    NUMERIC,
                in_stock BOOLEAN
            );
        """))

        print("[1-3] Bulk insert sample rows (50 000) …")
        batch = [(
            f"Widget-{i % 500}",          # 500 distinct names
            round(5 + (i % 100) * 0.1, 2),
            bool(i % 2)
        ) for i in range(50_000)]

        # Using a list-of-dicts with ``conn.execute`` performs a vectorized
        # bind—similar to executemany—*without* leaving Core context.  It
        # avoids string concatenation and is DB-api friendly.  Classic
        # ``cursor.executemany`` is slightly faster for huge batches but
        # ties you to psycopg2 API.
        conn.execute(
            text("""
                INSERT INTO products (name, price, in_stock)
                VALUES (:name, :price, :in_stock);
            """),
            [dict(name=n, price=p, in_stock=s) for n, p, s in batch]
        )

    # ===============================================================
    # 2. Query Performance BEFORE Index
    # ===============================================================
    with engine.connect() as conn:
        print("\n" + "-" * 70)
        target_name = "Widget-42"
        print(f"[2-1] Timing SELECT for name = '{target_name}' (no index) …")

        # ``timed`` returns (rows, elapsed_ms).  We ignore rows here.
        _, t_no_idx = timed(
            conn,
            "SELECT * FROM products WHERE name = :n;",  # SQL text
            {"n": target_name}                          # bind params
        )
        print(f"      -> Query time: {t_no_idx:.2f} ms")

        print("[2-2] EXPLAIN ANALYZE (no index) …")
        # EXPLAIN ANALYZE executes the query and returns the execution plan
        # with timing information for each node.  Useful for diagnosing
        # Seq Scan vs Index Scan and row estimates.
        plan, _ = timed(
            conn,
            "EXPLAIN ANALYZE SELECT * FROM products WHERE name = :n;",
            {"n": target_name}
        )
        for row in plan:
            print(row[0])

    # ===============================================================
    # 3. Create B-Tree Index & Re-test
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        # ``USING btree`` is optional (btree is default) but made explicit
        # for didactic clarity.
        print("[3-1] Create B-Tree index on products(name) …")
        conn.execute(text("""
            CREATE INDEX idx_products_name
            ON products USING btree (name);
        """))
		# 'name' here specifies the column on which the B-Tree index will
		# be built. This helps PostgreSQL optimize WHERE clauses filtering
		# by the 'name' column (e.g., WHERE name = '...').

    with engine.connect() as conn:
        print("[3-2] Timing SELECT after index …")
        _, t_idx = timed(
            conn,
            "SELECT * FROM products WHERE name = :n;",
            {"n": target_name}
        )
        print(f"      -> Query time: {t_idx:.2f} ms")

        print("[3-3] EXPLAIN ANALYZE (after index) …")
        plan_idx, _ = timed(
            conn,
            "EXPLAIN ANALYZE SELECT * FROM products WHERE name = :n;",
            {"n": target_name}
        )
        for row in plan_idx:
            print(row[0])

        ratio = t_no_idx / t_idx if t_idx else float("inf")
        print(f"\n🗝 Speed-up factor ~ {ratio:.1f}x (Seq Scan -> Index Scan)")

    # ===============================================================
    # 4. End-to-End Bottleneck Profiling
    # ===============================================================
    with engine.connect() as conn:
        print("\n" + "-" * 70)
        print("[4-1] Raw fetchall() timing …")
        _, t_fetch = timed(conn, "SELECT * FROM products;")
        print(f"      -> fetchall(): {t_fetch:.2f} ms")

        print("[4-2] pandas.read_sql() timing …")
        start = time.perf_counter()
        pd.read_sql("SELECT * FROM products;", conn)
        t_pandas = (time.perf_counter() - start) * 1000
        print(f"      -> pandas read: {t_pandas:.2f} ms")

        print("\n[4-3] Profiling summary:")
        df_profile = pd.DataFrame(
            [["Seq Scan (no idx)", f"{t_no_idx:.2f} ms"],
             ["Index Scan", f"{t_idx:.2f} ms"],
             ["fetchall() full", f"{t_fetch:.2f} ms"],
             ["pandas read_sql", f"{t_pandas:.2f} ms"]],
            columns=["Operation", "Time"]
        )
        print(tabulate(df_profile, headers="keys", tablefmt="grid"))

    # ===============================================================
    # 5. Cleanup (idempotent re-run)
    # ===============================================================
    with engine.begin() as conn:
        print("\n" + "-" * 70)
        print("[5-1] Drop index and table for cleanup …")
        conn.execute(text("DROP INDEX IF EXISTS idx_products_name;"))
        conn.execute(text("DROP TABLE IF EXISTS products;"))

except Exception as e:
    print("Unexpected error:", e)

finally:
    engine.dispose()
    print("All operations completed. Connection closed.")
