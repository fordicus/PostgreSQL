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

	PostgreSQL CRUD Operations (Core Style)

	Goal:
		- Demonstrate basic SELECT, UPDATE, DELETE operations
		- Use SQLAlchemy Core with batch insert for setup

	Key Concepts:
		- Explicit SQL execution using text("...")
		- Executemany for batch insertion (efficient & readable)
		- Use text(...) for raw SQL execution
		- Verify each step via pandas + tabulate

	🔑 Raw SQL gives transparent control for learning and debugging.

----------------------------------------------------------------------"""

# 📦 Core dependencies for SQL execution
from sqlalchemy import create_engine, text

# 🧪 Used only for tabulated result visualization
import pandas as pd
from tabulate import tabulate

# password, certificate, LDAP, Kerberos, GSSAPI, Trust
engine = create_engine(
	'postgresql+psycopg2://postgres:1234@localhost:5432/avengers'
)

try:
	# 🔄 Begin a transaction block. Automatically commits/rolls back.
	with engine.begin() as conn:
		
		print("\n[Step 1] Drop table if exists...")
		conn.execute(text("DROP TABLE IF EXISTS heroes;"))

		print("[Step 2] Create table 'heroes'...")
		conn.execute(text("""
			CREATE TABLE heroes (
				id SERIAL PRIMARY KEY,
				name VARCHAR(50) NOT NULL,
				team VARCHAR(50)
			);
		"""))

		# Batch insert using executemany 🔑
		insert_sql = """
			INSERT INTO heroes (name, team)
			VALUES (%s, %s);
		"""
		values = [
			("Iron Man", "Avengers"),
			("Captain America", "Avengers"),
			("Wolverine", "X-Men")
		]
		print("[Step 3] Insert rows with executemany()...")
		with conn.connection.cursor() as cur:
			cur.executemany(insert_sql, values)

		print("[Step 4] SELECT before updates...")
		res = conn.execute(text("SELECT * FROM heroes;"))
		df = pd.DataFrame(res.fetchall(), columns=res.keys())
		print(tabulate(df, headers='keys', tablefmt='grid'))

		# UPDATE
		print("[Step 5] UPDATE 'Wolverine' to team 'Avengers'...")
		conn.execute(
			text("""
				UPDATE heroes SET team = 'Avengers'
				WHERE name = 'Wolverine';
			""")
		)

		# DELETE
		print("[Step 6] DELETE 'Iron Man'...")
		conn.execute(
			text("DELETE FROM heroes WHERE name = 'Iron Man';")
		)

		print("[Step 7] SELECT after update/delete...")
		res = conn.execute(text("SELECT * FROM heroes;"))
		df = pd.DataFrame(res.fetchall(), columns=res.keys())
		print(tabulate(df, headers='keys', tablefmt='grid'))

		print("[Step 8] Drop table for cleanup...")
		conn.execute(text("DROP TABLE IF EXISTS heroes;"))

except Exception as e:
	print("\nError occurred:", e)

finally:
	# engine.dispose() ensures all DB connections are closed and pool is
	# released. It helps avoid resource leaks when running multiple scripts
	# or after errors. Always include in 'finally' to guarantee safe and
	# clean teardown.

	engine.dispose()
	print("\nAll operations completed. Connection closed.")
