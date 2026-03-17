import pandas as pd
import pyarrow.parquet as pq

df = pd.DataFrame([
    {"event_time": "2026-03-01 10:15:00+00:00", "country_iso3": "USA", "amount_usd": 19.99,  "is_premium": True},
    {"event_time": "2026-03-01 10:22:00+00:00", "country_iso3": "USA", "amount_usd": 49.50,  "is_premium": True},
    {"event_time": "2026-03-02 14:12:00+00:00", "country_iso3": "USA", "amount_usd": 99.00,  "is_premium": True},
    {"event_time": "2026-03-05 17:45:00+00:00", "country_iso3": "USA", "amount_usd": 24.99,  "is_premium": True},
    {"event_time": "2026-03-03 15:55:00+00:00", "country_iso3": "USA", "amount_usd": 14.99,  "is_premium": False},
    {"event_time": "2026-03-01 11:05:00+00:00", "country_iso3": "DEU", "amount_usd": 29.99,  "is_premium": True},
    {"event_time": "2026-03-05 13:10:00+00:00", "country_iso3": "DEU", "amount_usd": 79.00,  "is_premium": True},
    {"event_time": "2026-03-03 08:30:00+00:00", "country_iso3": "FRA", "amount_usd": 39.50,  "is_premium": True},
    {"event_time": "2026-03-04 11:20:00+00:00", "country_iso3": "CAN", "amount_usd": 59.99,  "is_premium": True},
    {"event_time": "2026-03-02 09:40:00+00:00", "country_iso3": "GBR", "amount_usd":  9.99,  "is_premium": False},
])

df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
df["amount_usd"] = df["amount_usd"].astype(pd.ArrowDtype(pa.decimal128(12,4)))

df.to_parquet(
    "mock_events_2026_03.parquet",
    engine="pyarrow",
    compression="snappy",
    index=False
)

print("Mock file written (pandas way)")
