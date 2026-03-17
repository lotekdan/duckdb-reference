import duckdb

# Single file
duckdb.sql("""
    SELECT 
        country_iso3,
        COUNT(*) AS cnt,
        AVG(amount_usd) AS avg_amount
    FROM 's3://my-bucket/events/year=2026/month=03/*.parquet'
    WHERE event_time >= '2026-03-01'
      AND is_premium = true
    GROUP BY country_iso3
    ORDER BY cnt DESC
    LIMIT 20
""").show()

# ── Or register & reuse ──
con = duckdb.connect()           # or :memory: is default
con.sql("CREATE VIEW events AS SELECT * FROM read_parquet('path/to/*.parquet')")

# Then normal SQL
con.sql("SELECT ... FROM events WHERE ...").df()   # → pandas
con.sql("...").pl()                                # → polars
con.sql("...").arrow()                             # → arrow table
