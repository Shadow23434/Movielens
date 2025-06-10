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
    ratings_table_name = "ratings"

    try:
        conn = get_connection()

        ratings_path = download_movielens_dataset()

        loadratings(ratings_table_name, ratings_path, conn)

        # Input number of partitions for range partitioning
        number_of_partitions_range = int(input("Enter number of partitions for RANGE partitioning: "))
        rangepartition(ratings_table_name, number_of_partitions_range, conn)

        # Allow multiple Range Inserts
        while True:
            print("\nRange Insert with user input record...")
            userid_range = int(input("Enter userid for range insert: "))
            movieid_range = int(input("Enter movieid for range insert: "))
            rating_range = float(input("Enter rating for range insert: "))
            rangeinsert(ratings_table_name, userid_range, movieid_range, rating_range, conn)
            print("Range Insert completed.")
            cont = input("Do you want to insert another record with RANGE partitioning? (y/n): ").strip().lower()
            if cont != 'y':
                break

        # Input number of partitions for round robin partitioning
        number_of_partitions_rr = int(input("Enter number of partitions for ROUND ROBIN partitioning: "))
        roundrobinpartition(ratings_table_name, number_of_partitions_rr, conn)

        # Allow multiple Round Robin Inserts
        while True:
            print("\nRound Robin Insert with user input record...")
            userid_rr = int(input("Enter userid for round robin insert: "))
            movieid_rr = int(input("Enter movieid for round robin insert: "))
            rating_rr = float(input("Enter rating for round robin insert: "))
            roundrobininsert(ratings_table_name, userid_rr, movieid_rr, rating_rr, conn)
            print("Round Robin Insert completed.")
            cont = input("Do you want to insert another record with ROUND ROBIN partitioning? (y/n): ").strip().lower()
            if cont != 'y':
                break

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()