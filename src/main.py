import os
from dotenv import load_dotenv

# Load environment variables at the very start
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

from database.database import get_connection, loadratings
from partitioning.partitioning import rangepartition, roundrobinpartition, rangeinsert, roundrobininsert
from utils.utils import download_movielens_dataset

def main():
    # Download and get path to ratings.dat
    ratings_path = download_movielens_dataset()
    
    # Get database connection
    conn = get_connection()
    
    try:
        # Load ratings data
        loadratings("ratings", ratings_path, conn)
            
        # Create range partitions
        # rangepartition("ratings", 5, conn)
        
        # Create round robin partitions
        # roundrobinpartition("ratings", 5, conn)
        
        # Insert new ratings
        # rangeinsert("ratings", 1, 1, 4.5, conn)
        # roundrobininsert("ratings", 2, 2, 3.5, conn)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()