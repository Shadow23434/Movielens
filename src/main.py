# Trong src/main.py

import os
from dotenv import load_dotenv
import traceback

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

from database.database import get_connection, loadratings
from partitioning.partitioning import rangepartition, roundrobinpartition, rangeinsert, roundrobininsert
from utils.utils import download_movielens_dataset

def main():
    conn = None
    ratings_table_name = "ratings" 

    try:
        conn = get_connection()
        print("Database connection established.")

        ratings_path = download_movielens_dataset()
        if ratings_path:
            print(f"MovieLens dataset ready at: {ratings_path}")
        else:
            print("Failed to ensure dataset is available. Exiting.")
            return

        print("\nStarting data loading into main 'ratings' table...")
        loadratings(ratings_table_name, ratings_path, conn)
        print("Data loading completed successfully!")

        print("\nStarting Round Robin Partitioning...")
        num_rr_partitions = 3
        roundrobinpartition(ratings_table_name, num_rr_partitions, conn)
        print("Round Robin Partitioning completed.")

        print("\nTesting Round Robin Inserts with example records...")
        roundrobininsert(ratings_table_name, 10000000, 10000000, 4.5, conn)
        roundrobininsert(ratings_table_name, 10000001, 10000001, 3.0, conn)
        roundrobininsert(ratings_table_name, 10000002, 10000002, 5.0, conn)
        print("Round Robin Inserts testing completed.")

        # --- BỎ QUA RANGE PARTITION BẰNG CÁCH COMMENT NHƯ SAU ---
        # print("\nStarting Range Partitioning...")
        # num_range_partitions = 5
        # rangepartition(ratings_table_name, num_range_partitions, conn)
        # print("Range Partitioning completed.")

        # print("\nTesting Range Inserts with example records...")
        # rangeinsert(ratings_table_name, 10000003, 10000003, 2.5, conn)
        # rangeinsert(ratings_table_name, 10000004, 10000004, 4.8, conn)
        # print("Range Inserts testing completed.")
        # --- KẾT THÚC PHẦN BỎ QUA ---

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
