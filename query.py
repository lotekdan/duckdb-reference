import duckdb

duckdb.sql("""
    SELECT
        country_iso3,
        COUNT(*) AS cnt,
        AVG(amount_usd) AS avg_amount
    FROM 'mock_events_2026_03.parquet'
    WHERE event_time >= '2026-03-01'
      AND is_premium = true
    GROUP BY country_iso3
    ORDER BY cnt DESC
    LIMIT 20
""").show()
