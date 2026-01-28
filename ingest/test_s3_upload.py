from ingest.s3_io import upload_csv_to_raw

# CHANGE THIS to your actual local file path:
local_path = r"C:\Projects-2025-2026\instacart-market-basket-analysis\orders.csv"

key, size = upload_csv_to_raw("orders", local_path)
print("uploaded:", key, "bytes:", size)