# 🐘 PostgreSQL Basic Examples

Practical, self‑contained scripts that showcase modern PostgreSQL + Python
patterns (SQLAlchemy Core) for teaching and rapid prototyping.

---

## ✅ Runtime Stack

* **Python 3.9+** (tested 3.9.19)
* **SQLAlchemy Core 2.0+** (tested 2.0.34)
* **PostgreSQL 17** (tested 17.4)

```cmd
:: Quick version check (Windows CMD)
python --version & ^
python -c "import sqlalchemy; print('SQLAlchemy:', sqlalchemy.__version__)" & ^
psql --version
```

Output example

```
Python 3.9.19
SQLAlchemy: 2.0.34
psql (PostgreSQL) 17.4
```

---

## 📜  Script Roster & Topics

| Python File                        | Core Topics                                                   |
| ---------------------------------- | ------------------------------------------------------------- |
| `01_crud_cycle.py`                 | CRUD basics, batch **executemany**, tabulated output          |
| `02_normalization_and_schema.py`   | 1 NF→3 NF walkthrough, composite PK, FK, rollback             |
| `03_constraints_and_defaults.py`   | UNIQUE / PK, DEFAULT + NOT NULL, seq gaps, comp PK            |
| `04_indexing_and_performance.py`   | **B‑Tree** vs seq scan, `EXPLAIN ANALYZE`, micro‑timing       |
| `05_relational_modeling.py`        | Parent/child, FK actions, filtering & paging                  |
| `06_analytics_pandas_bridge.py`    | `pandas.read_sql`, percentiles, windows vs subquery,          |
|                                    | DataFrame → DB `to_sql()` round‑trip                          |
| `07_json_and_fts.py`               | **JSONB @>**, GIN index, FTS, trigger upkeep                  |
| `08_uuid_and_materialized_view.py` | UUID PK, VIEW vs **MATERIALIZED VIEW**                        |
| `09_joins_and_set_operations.py`   | RIGHT / FULL JOIN, `COALESCE`, **UNION / INTERSECT / EXCEPT** |
| `10_trigger_audit_null_sort.py`    | BEFORE trigger audit, `COALESCE`/`NULLIF`, custom sort        |

---

## 📌 Reusable Snippets

### `executemany` via List‑of‑Dicts

Efficient batch‑insert with named placeholders.

```python
posts = [
    {"title": "Hello", "body": "First post"},
    {"title": "Tips",  "body": "Useful tips"}
]
conn.execute(text("""
    INSERT INTO blog_posts (title, body)
    VALUES (:title, :body);
"""), posts)
```

*Readable, injection‑safe, works as true `executemany`.*

### VIEW vs MATERIALIZED VIEW

* VIEW = reusable **SELECT macro** (executes fresh each time).
* MVIEW = **cached SELECT result** (`REFRESH` required).

### JSONB + GIN Index — Fast Document Queries

* **JSONB** stores nested docs; `@>` operator tests containment.
* **GIN** index accelerates these lookups by indexing internal keys.

---

## 📑 Guideline Excerpt (ChatGPT o3‑assisted)

> The full rules live in
> `PostgreSQL_educational_Python_script_guidelines.md` and were iteratively
> refined with ChatGPT o3. Key points:
>
> * 80‑char width, ASCII console
> * `executemany()` mandatory for ≥3 rows
> * Central `timed_read_sql()` helper reused across files
> * Python 3.9 typing (`Optional`, `Union`)—no `A | B` pipe syntax
> * Inline comments for every new SQL operator & keyword

See the guideline file for the complete table of rules.

---

All examples are fully runnable 🠚 clone, set `psql` creds, and `python` any
script. Enjoy exploring modern PostgreSQL patterns!


## 🛡️ License

This project is licensed under the  
✍️ [Creative Commons Attribution-NonCommercial 4.0 International License – Legal Code](https://creativecommons.org/licenses/by-nc/4.0/legalcode).  
🚫💰 Commercial use is prohibited.  
✨🛠️ Adaptation is permitted with attribution.  
⚠️ No warranty is provided.

[![License: CC BY-NC 4.0](https://licensebuttons.net/l/by-nc/4.0/88x31.png)](https://creativecommons.org/licenses/by-nc/4.0/legalcode)
