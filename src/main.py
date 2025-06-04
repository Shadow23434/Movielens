from dotenv import load_dotenv
from database.database import get_connection, loadratings
from partitioning.partitioning import rangepartition, roundrobinpartition, rangeinsert, roundrobininsert
from utils.utils import download_movielens_dataset, verify_ratings_load

# Load environment variables
load_dotenv()

def main():
    # Download and get path to ratings.dat
    ratings_path = download_movielens_dataset()
    
    # Get database connection
    conn = get_connection()
    
    try:
        # Load ratings data
        loadratings("ratings", ratings_path, conn)
        
        # Verify ratings load
        if not verify_ratings_load(conn):
            print("Error: Ratings were not loaded successfully!")
            return
            
        # Create range partitions
        rangepartition("ratings", 5, conn)
        
        # Create round robin partitions
        roundrobinpartition("ratings", 5, conn)
        
        # Insert new ratings
        rangeinsert("ratings", 1, 1, 4.5, conn)
        roundrobininsert("ratings", 2, 2, 3.5, conn)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()