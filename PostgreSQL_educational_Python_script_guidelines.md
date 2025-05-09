# ✅ PostgreSQL Educational Python Script Guidelines (Python 3.9‑compatible)

> **Baseline Interpreter** : All example scripts target **Python 3.9.19**.  
> Therefore, *PEP 604* union syntax (`A | B`) is **not allowed**; use
> `typing.Union` / `typing.Optional` instead.

---

## 🧱 1 Structure & Language

| Rule | Description |
|------|-------------|
| **Pure Python Only** | All scripts are `.py`, English‑only. |
| **Self‑Contained** | Depend exclusively on `sqlalchemy`, `psycopg2`, `pandas`, `tabulate`, `uuid`, `json`. |
| 📦 **Logical Import Groups** | Cluster imports by purpose (DB core, analysis, typing). |
| 🅰️ **Avoid Cryptic Lambdas** | Replace complex lambdas with `def`. |
| 🐍 **Typing (3.9)** | Always import `Optional`, `Union`, `Dict`, `Tuple`, `Any` and use them instead of the `|` operator. |

---

## 🧭 2 Educational Header Convention

| Rule | Description |
|------|-------------|
| 🔹 **Structured Docstring** | Begin file with `r"""` header (goals, key concepts). |
| 📚 **CLI Reference** | Include `psql` commands for DB create/login/drop. |
| **Separator** | Encapsulate docstring with `------` lines. |
| 📜 **Binding Showcase** | If both psycopg2 `%s` and SQLAlchemy `:key` are demonstrated, document the difference here. |
| 🐍 **3.9 Typing Note** | State that union pipe (`|`) is disallowed—use `Optional` / `Union`. |

---

## 🧠 3 Commenting & Conceptual Clarity

| Rule | Description |
|------|-------------|
| **Explain Types** | Inline SQL comments (`--`) for datatype meaning. |
| 📎 **Alt Options** | Mention variants (`JSONB`, `trust`, `md5`). |
| 🗝 **Highlight Core** | Mark 1–3 pedagogical insights with 🗝. |
| **Explain New SQL** | Add beginner comment above any new operator (`@@`, `@>`, `WITHIN GROUP`). |
| **Define Terms** | On first use, define **CTE**, **Window Function**, **Subquery**, etc. |

---

## 🔄 4 Lifecycle & Cleanup

| Rule | Description |
|------|-------------|
| 📌 **Explicit Teardown** | Use `DROP TABLE IF EXISTS`; avoid `drop_all()`. |
| **Schema Drops** | Keep `DROP SCHEMA` only in CLI block. |
| **Hard‑coded Names** | Avoid dynamic identifiers. |

---

## ⚙️ 5 Execution, Inserts & Flow

| Rule | Description |
|------|-------------|
| ✅ **Core Execution** | Use `conn.execute(text(...))`, never ORM. |
| 📋 **`executemany()` ≥ 3 rows** | Mandatory when inserting 3 or more rows. Use *tuple list* for psycopg2 or *dict list* for SQLAlchemy. |
| 🧠 **Cursor Allowed** | Raw cursor permitted for batch speed demos. |
| 📣 **Step Prints** | Print label before each major step. |
| 🧪 **Tabulated Output** | Present `SELECT` via `pandas` + `tabulate`. |
| ⚠ **Exception ASCII** | Wrap risky code in `try–except`; ASCII messages only. |
| ⛔ **Triple Quotes Safety** | Prefer `"""`; avoid escaped mess. |

---

## 🧾 6 Transaction Strategy

| Rule | Description |
|------|-------------|
| 🎯 **Manual Blocks** | Use `engine.connect()` + `trans = conn.begin()` for rollback demos. |
| 💡 **`engine.begin()`** | Use for atomic, failure‑safe segments. |

---

## 📐 7 Code Style & Readability

| Rule | Description |
|------|-------------|
| 📐 **80‑char Width** | Keep code & comments ≤ 80 chars. |
| 🚫 **No Emoji in Exceptions** | Error text plain ASCII. |
| **ASCII Console Symbols** | Replace en/em dashes → `-`, ellipsis → `...`, arrows → `->`. |
| **Section Bars** | Runtime output separated by `"-" * 70`. |
| **Helper Typing** | Type‑hint utilities with `Tuple`, `Union`, etc. |
| ⏱ **Central Timing** | One `timed_*` helper at top; reuse it. |

---

## 🗄 8 Indexes, JSONB & FTS

| Rule | Description |
|------|-------------|
| **JSONB Insert** | Convert dict → JSON text with `json.dumps` when using `%s`. |
| **FTS Triggers** | Document purpose & meaning of `NEW`/`OLD`. |
| **GIN/GIST Index** | Comment *why* (containment, proximity). |

---

> **Reminder (3.9 Typing)** — always prefer:
>
> ```python
> from typing import Optional, Union, Dict, Tuple
>
> def timed_read_sql(conn, sql: Union[text, str],
>                    params: Optional[Dict[str, Any]] = None
>                   ) -> Tuple[pd.DataFrame, float]:
>     ...
> ```
>
> instead of using the `|` operator, which requires Python 3.10+. This keeps
> every sample fully runnable on **Python 3.9.19**.
