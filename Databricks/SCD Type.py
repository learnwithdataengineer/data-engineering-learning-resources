# Databricks notebook source
# DBTITLE 1,SCD Type 1 & 2 Demo
# MAGIC %md
# MAGIC # SCD Types 1 & 2 Demo (Slowly Changing Dimensions)
# MAGIC
# MAGIC Demonstrates SCD Type 1 and SCD Type 2 on an **employee table** (`emp_id`, `emp_name`, `address`, `skills`, `phone`). Type 1: changes **overwrite** the row (no history). Type 2: changes are **tracked as new versions** with `start_date` / `end_date` / `is_current`. Both use a simple Delta `MERGE` pattern.

# COMMAND ----------

# DBTITLE 1,1. Dummy source data (Day 1)
# Cell 1: Create dummy source data (Day-1 snapshot from the HR source system)
# Columns: id, name, address, skills, phone
initial_batch = spark.createDataFrame(
    [
        (101, "A", "Hyderabad, TS", "PySpark, SQL", "999999999"),
        (102, "B", "Chennai, TN", "SQL, Power BI", "988888888"),
        (103, "C", "Bengaluru, KA", "Selenium, Java", "966666666"),
    ],
    schema="emp_id INT, emp_name STRING, address STRING, skills STRING, phone STRING",
)

display(initial_batch)

# COMMAND ----------

# DBTITLE 1,2. Create target table
# Cell 2: Create the target Delta table (dim_employee) and load the Day-1 snapshot
# (DROP ... IF EXISTS only so the demo can be re-run cleanly)
spark.sql("DROP TABLE IF EXISTS workspace.practice_db.scd1_employees")

initial_batch.write.saveAsTable("workspace.practice_db.scd1_employees")

display(spark.table("workspace.practice_db.scd1_employees"))

# COMMAND ----------

# DBTITLE 1,3. New incoming batch (Day 2)
# Cell 3: Simulate a new incoming batch (Day 2)
#   - A modified their skill domain (PySpark, SQL -> Databricks, PySpark) -> UPDATE
#   - B relocated (Chennai -> Mumbai)                          -> UPDATE
#   - C modified skills + phone (Selenium, Java -> Selenium, Scala) -> UPDATE
#   - D is a newly hired employee                               -> INSERT
new_batch = spark.createDataFrame(
    [
        (101, "A", "Hyderabad, TS", "Databricks, PySpark", "999999999"),
        (102, "B", "Mumbai, MH", "SQL, Power BI", "988888888"),
        (103, "C", "Bengaluru, KA", "Selenium, Scala", "9898989898"),
        (104, "D", "Pune, MH", "Python, Spark", "9765432109"),
    ],
    schema="emp_id INT, emp_name STRING, address STRING, skills STRING, phone STRING",
)
new_batch.createOrReplaceTempView("new_batch")

display(new_batch)

# COMMAND ----------

# DBTITLE 1,4. SCD Type 1 MERGE
# Cell 4: The SCD Type 1 merge — update matching rows in place, insert new ones
spark.sql("""
MERGE INTO workspace.practice_db.scd1_employees AS t
USING new_batch AS s
  ON t.emp_id = s.emp_id
WHEN MATCHED AND (t.emp_name <> s.emp_name OR t.address <> s.address OR t.skills <> s.skills OR t.phone <> s.phone)
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
""")

print("MERGE complete.")

# COMMAND ----------

# DBTITLE 1,5. How it works
# MAGIC %md
# MAGIC ### Key points (SCD Type 1)
# MAGIC
# MAGIC * **Update in place**: A's skills (`PySpark, SQL` → `Databricks, PySpark`), B's address (`Chennai, TN` → `Mumbai, MH`), and C's skills (`Selenium, Java` → `Selenium, Scala`) were overwritten — the old values are *gone* from the table (that's what makes it Type 1).
# MAGIC * **No history columns**: no `start_date`, `end_date`, `is_current` — those belong to Type 2. You can't tell *when* B relocated or C changed skills, only the *current* values.
# MAGIC * The `WHEN MATCHED ... AND <changed>` guard skips pointless rewrites when nothing changed — unchanged rows are left exactly as-is (no new Delta row version is written).
# MAGIC * `WHEN NOT MATCHED THEN INSERT *` brings in new hires like D.
# MAGIC * Real-world fit: employee master sync from an HR system (address / skills / phone changes) where you only ever need the *current* state.

# COMMAND ----------

# DBTITLE 1,6. Final target table
# Cell 5: Final target — the current state of each employee, history overwritten
display(spark.table("workspace.practice_db.scd1_employees"))

# COMMAND ----------

# DBTITLE 1,7. SCD2 header (one MERGE)
# MAGIC %md
# MAGIC # SCD Type 2 (with history) — in ONE MERGE
# MAGIC
# MAGIC Fresh dummy data, same employees — every row carries its own **`day`** (effective date), so no separate date variables are needed. Every change is kept as **history**: the old version gets **closed** (`end_date` set, `is_current = false`) and the new values arrive as a **new open row** — all handled by a **single MERGE** statement.
# MAGIC

# COMMAND ----------

# DBTITLE 1,8. SCD2: dummy data + target table
# SCD2 step 1: Dummy data with a `day` column (the date each row became effective) + create the target table
from pyspark.sql import functions as F

scd2_day1 = (
    spark.createDataFrame(
        [
            (101, "A", "Hyderabad, TS", "PySpark, SQL", "999999999", "2026-10-01"),
            (102, "B", "Chennai, TN", "SQL, Power BI", "988888888", "2026-10-01"),
            (103, "C", "Bengaluru, KA", "Selenium, Java", "966666666", "2026-10-01"),
        ],
        schema="emp_id INT, emp_name STRING, address STRING, skills STRING, phone STRING, day STRING",
    )
    .withColumn("day", F.col("day").cast("date"))
    .withColumnRenamed("day", "start_date")
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)

spark.sql("DROP TABLE IF EXISTS workspace.practice_db.scd2_employees")
scd2_day1.write.saveAsTable("workspace.practice_db.scd2_employees")

display(spark.table("workspace.practice_db.scd2_employees"))


# COMMAND ----------

# SCD2 step 2: Day-2 batch — every row carries its own `day` (A: skills changed, B: relocated, C: skills changed, D: new hire)
scd2_batch = spark.createDataFrame(
    [
        (101, "A", "Hyderabad, TS", "Databricks, PySpark", "999999999", "2026-10-21"),
        (102, "B", "Mumbai, MH", "SQL, Power BI", "988888888", "2026-10-21"),
        (103, "C", "Bengaluru, KA", "Selenium, Scala", "9898989898", "2026-10-21"),
        (104, "D", "Pune, MH", "Python, Spark", "9765432109", "2026-10-21"),
    ],
    schema="emp_id INT, emp_name STRING, address STRING, skills STRING, phone STRING, day STRING",
).withColumn("day", F.col("day").cast("date"))
scd2_batch.createOrReplaceTempView("scd2_batch")

# COMMAND ----------

# DBTITLE 1,9. SCD2: ONE merge (update + insert)

# ONE MERGE — UPDATE (expire old versions) and INSERT (new versions + new hires) together.
# The trick: stage the batch TWICE with UNION ALL and a merge_key:
#   branch 1: merge_key = emp_id -> matches the CURRENT row of A/B/C and expires it;
#                                 new hire D matches nothing -> INSERTed as his first row
#   branch 2: merge_key = NULL   -> can never match (NULL = nothing is equal in SQL)
#                                 -> INSERTed as the NEW current version of A/B/C
spark.sql("""
MERGE INTO workspace.practice_db.scd2_employees AS t
USING (
    SELECT s.emp_id AS merge_key, s.*
    FROM scd2_batch s --4
    UNION ALL
    SELECT CAST(NULL AS INT) AS merge_key, s.*  --3
    FROM scd2_batch s
    JOIN workspace.practice_db.scd2_employees tt
      ON tt.emp_id = s.emp_id AND tt.is_current = true
    WHERE tt.emp_name <> s.emp_name OR tt.address <> s.address OR tt.skills <> s.skills OR tt.phone <> s.phone
) AS s
ON t.emp_id = s.merge_key AND t.is_current = true
WHEN MATCHED AND (t.emp_name <> s.emp_name OR t.address <> s.address OR t.skills <> s.skills OR t.phone <> s.phone)
  THEN UPDATE SET t.end_date = s.day, t.is_current = false
WHEN NOT MATCHED
  THEN INSERT (emp_id, emp_name, address, skills, phone, start_date, end_date, is_current)
       VALUES (s.emp_id, s.emp_name, s.address, s.skills, s.phone, s.day, NULL, true)
""")

print("Merge Completed.")


# COMMAND ----------

# DBTITLE 1,10. SCD2: final result
# SCD2 step 3: Final result — full history per employee
# Expected: 7 rows — A/B/C each have a CLOSED old row (end_date = 2026-10-21) + an OPEN new row,
# and D has a single OPEN row starting 2026-10-21.
display(
    spark.table("workspace.practice_db.scd2_employees").orderBy("emp_id", "start_date")
)
