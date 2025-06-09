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
    num_rr_partitions = 3

    try:
        conn = get_connection()

        ratings_path = download_movielens_dataset()

        loadratings(ratings_table_name, ratings_path, conn)

        roundrobinpartition(ratings_table_name, num_rr_partitions, conn)

        print("\nRound Robin Inserts with example records...")
        roundrobininsert(ratings_table_name, 10000000, 10000000, 4.5, conn)
        roundrobininsert(ratings_table_name, 10000001, 10000001, 3.0, conn)
        roundrobininsert(ratings_table_name, 10000002, 10000002, 5.0, conn)
        print("Round Robin Inserts completed.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
