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

    PostgreSQL JSONB & Full‑Text Search (FTS) Demonstration

    🔄 Parameter Binding Styles

    • SQLAlchemy-style (recommended):
        Uses named parameters like `:key` inside SQL.
        Parameters are passed as a dictionary to `execute()`.

        Example:
            conn.execute(
                text("INSERT INTO table (a, b) VALUES (:a, :b)"),
                {"a": 1, "b": "text"}
            )

    • psycopg2-style (lower-level):
        Uses positional placeholders `%s`, with values as tuples or lists.
        Typically used with raw cursor objects.

        Example:
            cursor.executemany(
                "INSERT INTO table (a, b) VALUES (%s, %s)",
                [(1, "text"), (2, "more")]
            )

    ✅ SQLAlchemy's `:key` format improves readability, safety,
       and allows flexible data typing (e.g., JSON).
	
----------------------------------------------------------------------"""

# 📦 Core dependencies for SQL execution
from sqlalchemy import create_engine, text

# 🧪 Used only for tabulated result visualization
import pandas as pd
from tabulate import tabulate

# 🧮 Generic timer for read_sql
import time
from typing import Tuple, Dict, Any, Optional, Union

# JSONB compatibility
import json

engine = create_engine("postgresql+psycopg2://postgres:1234@localhost/avengers")

# Pretty-print helper
pp = lambda df, title: (
	print("\n"+title),
	print(tabulate(df, headers="keys", tablefmt="grid"))
)

def timed_read_sql(
		conn,
		sql: Union[text, str],
		params: Optional[Dict[str, Any]] = None
	) -> Tuple[pd.DataFrame, float]:
    """Run *sql* via pandas.read_sql and return (DataFrame, elapsed_sec)."""
    t0 = time.perf_counter()
    df = pd.read_sql(sql, conn, params=params or {})
    return df, time.perf_counter() - t0

try:
    # 1. JSONB demo -----------------------------------------------------------
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS avengers_json;"))
        conn.execute(text("""
			CREATE TABLE avengers_json
			(
				id SERIAL PRIMARY KEY,
				data JSONB
			);
		"""))

        rows = [
			(json.dumps(
				{
					"name":		f"Hero {i}",
					"age":		20+i%30,
					"skills":	[f"skill{i%5}"]
				}
			),) for i in range(1,10_001)
		]
        with conn.connection.cursor() as cur:
            cur.executemany(
				"""
					INSERT INTO avengers_json (data)
					VALUES (%s)
				""",
			rows)

		#............................................................................
		# 🧠 Explanation:
		# This query looks inside the JSONB column called 'data' and
		# finds how many rows have "skill2" listed under the "skills" key.
		#
		# The operator @> means "contains". It checks if
		# the JSON object on the left contains
		# the smaller JSON structure on the right.
		#
		# Example match:
		# If a row has: {
		# 	"name": "Hero X",
		#	"skills": ["skill1", "skill2"]
		# }
		# → This query will count it as a match.
		#............................................................................
		
        filter_sql = """
			SELECT COUNT(*) AS hits
			FROM avengers_json
			WHERE data @> '{\"skills\":[\"skill2\"]}';"""
        pre, t_no_idx = timed_read_sql(conn, filter_sql)
        pp(pre, "Rows matched (no index)")
        print(f"Time without index: {t_no_idx:.3f}s")

        conn.execute(text("""
			CREATE INDEX idx_aj_data
			ON avengers_json
			USING GIN (data jsonb_path_ops);
		"""))
        post, t_idx = timed_read_sql(conn, filter_sql)
        print(f"Time with index: {t_idx:.3f}s (speed-up ~{t_no_idx/t_idx:.1f}x)")

    # 2. FTS demo -------------------------------------------------------------
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS articles CASCADE;"))
        conn.execute(text("""
			CREATE TABLE articles
			(
				id SERIAL PRIMARY KEY,
				title TEXT,
				body TEXT,
				document TSVECTOR
			);
		"""))

        art_rows=[("AI in Avengers","Iron Man created JARVIS using artificial intelligence."),
                  ("The Hulk Explained","Bruce Banner transforms into the Hulk after gamma radiation."),
                  ("Wakanda Tech","Black Panther uses advanced technology in Wakanda."),
                  ("AI Ethics","Discussion around AI ethics and its role in society."),
                  ("Thor and Mythology","Thor is based on Norse mythology and wields a hammer.")]
				  
        with conn.connection.cursor() as cur:
            cur.executemany(
				"INSERT INTO articles (title,body) VALUES (%s,%s)",
				art_rows
			)

        conn.execute(text("""
			UPDATE articles
			SET document = to_tsvector('english', title||' '||body);
		"""))
        conn.execute(text("""
			CREATE INDEX idx_articles_doc
			ON articles
			USING GIN (document);
		"""))
        conn.execute(text("DROP FUNCTION IF EXISTS trg_update_document();"))
		
		#.................................................................................
		# 🧠 Below, we create a PostgreSQL trigger function named `trg_update_document`.
		# It automatically updates the `document` column whenever a new article
		# is added or changed.
		#
		# Here's what it does:
		# - `NEW.title` and `NEW.body` are the new values
		#	 from the row being inserted or updated.
		# - `title || ' ' || body` joins the two fields into one long text.
		# - `to_tsvector(...)` turns that text into a searchable token list.
		# - That result is stored into NEW.document, so it's ready for full-text search.
		#
		# This way, we don't forget to prepare the data for searching — it's automatic!
		#.................................................................................

        conn.execute(text("""
			CREATE FUNCTION trg_update_document()
			RETURNS trigger AS $$
            BEGIN
				NEW.document := to_tsvector('english', NEW.title||' '||NEW.body);
				RETURN NEW;
			END$$
			LANGUAGE plpgsql;
		"""))
		
        conn.execute(text("""
			CREATE TRIGGER tsvector_update
			BEFORE INSERT OR UPDATE ON articles
            FOR EACH ROW EXECUTE FUNCTION trg_update_document();
		"""))

    with engine.connect() as conn:
        pp(pd.read_sql("SELECT id,title FROM articles;", conn), "[FTS] Article list")

		#.................................................................................
		# 🧠 The FTS query below searches for the word "AI" in all articles.
		# It works as follows:
		# - `plainto_tsquery('english','AI')` converts the keyword 'AI'
		#	 into a tsquery (search token).
		# - The `@@` operator checks if the document column matches that search token.
		# - `ts_rank(...)` gives a relevance score: higher means better match.
		# - We sort the results so the most relevant articles come first.
		#.................................................................................
		
        search=text("""
			SELECT title, ts_rank(document,q) AS rank
            FROM articles, plainto_tsquery('english','AI') AS q
            WHERE document @@ q
			ORDER BY rank DESC;
		""")
        df_ai,_=timed_read_sql(conn,search)
        pp(df_ai, "[FTS] Search 'AI' results")

		#.................................................................................
		# Developer Note:
		# This INSERT statement uses SQLAlchemy's named parameter binding (:title, :body),
		# which improves code readability, ensures safe SQL execution (avoiding injection),
		# and flexibly supports complex data types like JSON.
		#
		# The associated BEFORE INSERT trigger `tsvector_update` automatically populates
		# the `document` TSVECTOR column using `title` and `body`, ensuring that
		# full-text search indices remain up-to-date without manual intervention.
		#.................................................................................

        with engine.begin() as c2:
            c2.execute(
				text("""
					INSERT INTO articles (title,body)
					VALUES (:title,:body)
				"""),
				{
					"title":"New AI breakthrough",
					"body":"Revolutionary AI surpasses human intelligence."
				}
			)

        df_ai2,_=timed_read_sql(conn,search)
        pp(df_ai2, "[FTS] Results after insert (trigger ran)")

    # 3. Cleanup --------------------------------------------------------------
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS avengers_json;"))
        conn.execute(text("DROP TABLE IF EXISTS articles;"))

except Exception as e:
    print("Unexpected error:", e)
finally:
    engine.dispose()
    print("All operations completed. Connection closed.")