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

	PostgreSQL Constraints & Defaults ­— One-Stop Demonstration

	Goals:
		- 🗝 Showcase UNIQUE / PRIMARY KEY constraints with rollback
		- 🗝 Demonstrate DEFAULT + NOT NULL behaviour and sequence gaps
		- 🗝 Enforce COMPOSITE PRIMARY KEY and test integrity

	Key Concepts:
		- `executemany()` for efficient batch inserts
		- Manual vs automatic transactions (`trans.rollback()` vs begin)
		- Sequence non-transactionality after failed inserts
		- pandas + tabulate for human-readable output
----------------------------------------------------------------------"""

# 📦 Core dependencies for SQL execution
from sqlalchemy import create_engine, text

# 🧪 Used only for tabulated result visualization
import pandas as pd
from tabulate import tabulate

engine = create_engine(
	"postgresql+psycopg2://postgres:1234@localhost/avengers"
)

try:
# ════════════════════════════════════════════════════════════════════
# 1 UNIQUE / PRIMARY KEY Constraint Demo (users) 🗝
# ════════════════════════════════════════════════════════════════════
	with engine.begin() as conn:

		print("\n[1-1] Drop table 'users' if it exists.")
		conn.execute(text("DROP TABLE IF EXISTS users;"))

		print("[1-2] Create 'users' with PK + UNIQUE(email).")
		conn.execute(text("""
			CREATE TABLE users (
				user_id  SERIAL PRIMARY KEY,
				name     VARCHAR(100),
				email    VARCHAR(100) UNIQUE,
				password VARCHAR(100)
			);
		"""))

		print("[1-3] Insert Alice, Bob (valid).")
		conn.execute(text("""
			INSERT INTO users (name, email, password)
			VALUES  ('Alice', 'alice@example.com', 'pass123'),
			        ('Bob',   'bob@example.com',   'secret');
		"""))

	# manual transaction to trigger constraint failure
	with engine.connect() as conn:
		trans = conn.begin()
		try:
			print("[1-4] Attempt duplicate PK (user_id 1).")
			conn.execute(text("""
				INSERT INTO users (user_id, name, email, password)
				VALUES (1, 'Eve', 'eve@example.com', 'badpass');
			"""))
			trans.commit()
		except Exception as err:
			print("\n[PK Violation] rolled back:", err)
			trans.rollback()

	# duplicate UNIQUE(email) demo
	with engine.connect() as conn:
		trans = conn.begin()
		try:
			print("[1-5] Attempt duplicate email (bob@example.com).")
			conn.execute(text("""
				INSERT INTO users (name, email, password)
				VALUES ('Bob', 'bob@example.com', 'dupemail');
			"""))
			trans.commit()
		except Exception as err:
			print("\n[UNIQUE Violation] rolled back:", err)
			trans.rollback()

	with engine.begin() as conn:
		print("\n[1-6] Final rows in 'users'.")
		df_users = pd.read_sql("SELECT * FROM users ORDER BY user_id;", conn)
		print(tabulate(df_users, headers="keys", tablefmt="grid"))
		conn.execute(text("DROP TABLE IF EXISTS users;"))

	print("\n" + "-" * 70)

# ════════════════════════════════════════════════════════════════════
# 2 DEFAULT & NOT NULL Demo (test_defaults) 🗝
# ════════════════════════════════════════════════════════════════════
	with engine.begin() as conn:

		print("\n[2-1] Drop table 'test_defaults' if exists.")
		conn.execute(text("DROP TABLE IF EXISTS test_defaults;"))

		print("[2-2] Create table with defaults + NOT NULL.")
		conn.execute(text("""
			CREATE TABLE test_defaults (
				id         SERIAL PRIMARY KEY,
				-- If PRIMARY KEY is declared as a plain INTEGER (not SERIAL),
		        -- then values must be explicitly provided; no auto-increment occurs.
				name       TEXT DEFAULT 'Anonymous',
				department TEXT DEFAULT 'General',
				email      TEXT NOT NULL
			);
		"""))

		print("[2-3] Insert explicit row (override defaults).")
		conn.execute(text("""
			INSERT INTO test_defaults (name, department, email)
			VALUES ('Alice', 'Engineering', 'alice@example.com');
		"""))

		print("[2-4] Insert row using default department.")
		conn.execute(text("""
			INSERT INTO test_defaults (name, email)
			VALUES ('Bob', 'bob@example.com');
		"""))

		print("[2-5] Insert row using all defaults but NOT NULL email.")
		conn.execute(text("""
			INSERT INTO test_defaults (email)
			VALUES ('carol@example.com');
		"""))

	# attempt NULL violation
	with engine.connect() as conn:
		trans = conn.begin()
		try:
			print("[2-6] Attempt NULL into NOT NULL column.")
			conn.execute(text("""
				INSERT INTO test_defaults (name, department, email)
				VALUES ('Dave', 'Marketing', NULL);
			"""))
			trans.commit()
		except Exception as err:
			print("\n[NOT NULL Violation] rolled back:", err)
			trans.rollback()

	with engine.begin() as conn:
		print("[2-7] Rows after rollback (id sequence gap visible).")
		df_def = pd.read_sql(
			"SELECT * FROM test_defaults ORDER BY id;", conn
		)
		print(tabulate(df_def, headers="keys", tablefmt="grid"))

		print("[2-8] ALTER default for department ⇒ 'Support'.")
		conn.execute(text("""
			ALTER TABLE test_defaults
			ALTER COLUMN department SET DEFAULT 'Support';
		"""))

		print("[2-9] Insert row to use new default.")
		conn.execute(text("""
			INSERT INTO test_defaults (name, email)
			VALUES ('Eve', 'eve@example.com');
		"""))

	with engine.begin() as conn:
		print("[2-10] Final rows before cleanup.")
		df_final = pd.read_sql(
			"SELECT * FROM test_defaults ORDER BY id;", conn
		)
		print(tabulate(df_final, headers="keys", tablefmt="grid"))
		conn.execute(text("DROP TABLE IF EXISTS test_defaults;"))

	print("\n" + "-" * 70)

# ════════════════════════════════════════════════════════════════════
# 3 Composite Primary Key Demo (course_enrollments) 🗝
# --------------------------------------------------------------------
# email column is defined as NOT NULL but does not have a DEFAULT.
# If the INSERT statement omits 'email', PostgreSQL attempts to
# use NULL implicitly.
#
# This causes a NOT NULL constraint violation at runtime unless the
# field is explicitly provided in the INSERT command.
#
# Conclusion:
# Yes — if a NOT NULL column without DEFAULT is omitted,
# it defaults to NULL internally, leading to an error.
# ════════════════════════════════════════════════════════════════════

	with engine.begin() as conn:

		print("\n[3-1] Drop table 'course_enrollments' if exists.")
		conn.execute(text("DROP TABLE IF EXISTS course_enrollments;"))

		print("[3-2] Create table with composite PK.")
		conn.execute(text("""
			CREATE TABLE course_enrollments (
				student_id  INTEGER,
				course_id   TEXT,
				enrolled_on DATE DEFAULT CURRENT_DATE,
				PRIMARY KEY (student_id, course_id)
				-- Unless either (or both) of the composite key fields are declared as SERIAL,
				-- values must be explicitly provided in every INSERT.
			);
		"""))

		print("[3-3] Insert valid enrollments.")
		conn.execute(text("""
			INSERT INTO course_enrollments (student_id, course_id)
			VALUES (101,'CS101'),(101,'MATH202'),
			       (102,'CS101'),(103,'PHYS303');
		"""))

	# intentional duplicate to trigger rollback
	with engine.connect() as conn:
		trans = conn.begin()
		try:
			print("[3-4] Attempt duplicate composite key.")
			conn.execute(text("""
				INSERT INTO course_enrollments (student_id, course_id)
				VALUES (101,'CS101');
			"""))
			trans.commit()
		except Exception as err:
			print("\n[Composite PK Violation] rolled back:", err)
			trans.rollback()

	with engine.begin() as conn:
		print("[3-5] Final enrollments (composite PK enforced).")
		df_enr = pd.read_sql(
			"SELECT * FROM course_enrollments ORDER BY student_id;", conn
		)
		print(tabulate(df_enr, headers="keys", tablefmt="grid"))
		conn.execute(text("DROP TABLE IF EXISTS course_enrollments;"))

except Exception as e:
	print("\nUnexpected error:", e)

finally:
	engine.dispose()
	print("\nAll operations completed. Connection closed.")
