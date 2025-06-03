import psycopg2
from database.database import loadratings
from partitioning.partitioning import rangepartition, roundrobinpartition, rangeinsert, roundrobininsert
from utils.utils import get_partition_stats

def get_connection():
    """Tạo kết nối đến PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='dds_assgn1',
            user='postgres',
            password='1234',
            port='5432'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Lỗi kết nối database: {e}")
        raise

def main():
    # Example usage
    ratings_path = "tests/test_data.dat"  # Path to test data file
    
    # Get database connection
    conn = get_connection()
    
    try:
        # Load ratings data
        loadratings("ratings", ratings_path, conn)
        
        # Create range partitions
        rangepartition("ratings", 5, conn)
        
        # Create round robin partitions
        roundrobinpartition("ratings", 5, conn)
        
        # Insert new ratings
        rangeinsert("ratings", 1, 1, 4.5, conn)
        roundrobininsert("ratings", 2, 2, 3.5, conn)
        
        # Get partition statistics
        get_partition_stats()
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()