from ingest.s3_io import upload_csv_to_raw

# CHANGE THIS to your actual local file path:
PRIOR_CSV = r"C:\Projects-2025-2026\instacart-market-basket-analysis\order_products__prior.csv"

def main():
    key, size = upload_csv_to_raw("order_products_prior", PRIOR_CSV)
    print("uploaded:", key, "bytes:", size)

if __name__ == "__main__":
    main()