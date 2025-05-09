# 🐘 PostgreSQL Educational Examples — Key Patterns & Concepts

A summary of reusable techniques and patterns drawn from hands-on SQL + Python scripts.

---

## 📌 `executemany` via List-of-Dicts (SQLAlchemy)

Efficient, safe batch inserts using named placeholders and dictionary binding.

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

✅ **Benefits**

* Easy to read, secure against SQL injection.
* Executes as a true `executemany` under the hood.
* Avoids manual string interpolation or for-loops.

---

## 📘 VIEW vs 📦 MATERIALIZED VIEW

### VIEW = *a SELECT macro*

* Acts like a reusable SQL snippet.
* Internally expands into the original SELECT upon every access.

### MATERIALIZED VIEW = *a SELECT result cache*

* Physically stores the SELECT result in advance.
* Requires manual `REFRESH MATERIALIZED VIEW` to stay up to date.
* Excellent for precomputing expensive queries.

---

## 📦 JSONB + 🔍 GIN Index — Why They're Powerful Together

### ✅ JSONB (Binary JSON)

* Store structured documents with nested fields, arrays, or mixed types.
* Ideal for logs, dynamic schemas, or optional attributes.
* Enables key/value filtering within the JSON structure.

### 🔍 GIN Index (Generalized Inverted Index)

* Efficiently indexes elements **inside** arrays or JSONB fields.
* Enables fast search for expressions like `data @> '{"skills": ["skill1"]}'`.

### 🧠 Use them together when:

* Filtering by inner JSON structure is required.
* Query performance is critical on large datasets.

### 💡 Example

```sql
-- Slow without index:
SELECT * FROM avengers_json
WHERE data @> '{"skills": ["skill1"]}';

-- Faster with GIN index:
CREATE INDEX idx_aj_data
ON avengers_json
USING GIN (data jsonb_path_ops);
```

---

All examples are 100% runnable via Python 3.9+ with SQLAlchemy Core + PostgreSQL.
