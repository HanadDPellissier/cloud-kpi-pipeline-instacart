from ingest.s3_io import upload_csv_to_raw

# CHANGE THESE PATHS to your local unzipped Instacart folder
AISLES_CSV = r"C:\Projects-2025-2026\instacart-market-basket-analysis\aisles.csv"
DEPARTMENTS_CSV = r"C:\Projects-2025-2026\instacart-market-basket-analysis\departments.csv"
PRODUCTS_CSV = r"C:\Projects-2025-2026\instacart-market-basket-analysis\products.csv"

def main():
    for table, path in [
        ("aisles", AISLES_CSV),
        ("departments", DEPARTMENTS_CSV),
        ("products", PRODUCTS_CSV),
    ]:
        key, size = upload_csv_to_raw(table, path)
        print(f"{table}: uploaded {key} bytes={size}")

if __name__ == "__main__":
    main()