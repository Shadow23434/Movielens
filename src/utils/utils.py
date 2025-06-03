import os
import requests
import zipfile
import psycopg2
from config.config import DatabaseConfig


def download_movielens_dataset():
    """Download and extract MovieLens dataset"""
    dataset_url = "http://files.grouplens.org/datasets/movielens/ml-10m.zip"
    zip_path = os.path.join("data", "ml-10m.zip")
    extract_path = os.path.join("data", "ml-10M100K")
    
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Download the dataset if it doesn't exist
    if not os.path.exists(zip_path):
        print("Downloading MovieLens dataset...")
        response = requests.get(dataset_url, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download completed.")
    
    # Extract the dataset if it hasn't been extracted
    if not os.path.exists(extract_path):
        print("Extracting dataset...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract only the ml-10M100K directory
            for file in zip_ref.namelist():
                if file.startswith('ml-10M100K/'):
                    zip_ref.extract(file, "data")
        print("Extraction completed.")
    
    # Return path to ratings.dat file
    return os.path.join(extract_path, "ratings.dat")

def get_partition_stats():
    """Get statistics about the partitions"""
    conn = psycopg2.connect(**DatabaseConfig.get_connection_params())
    cursor = conn.cursor()
    
    try:
        # Get range partition statistics
        print("\nRange Partition Statistics:")
        cursor.execute("""
            SELECT table_name, COUNT(*) as record_count
            FROM information_schema.tables 
            WHERE table_name LIKE 'ratings_range_%'
            GROUP BY table_name
            ORDER BY table_name
        """)
        
        for table_name, count in cursor.fetchall():
            print(f"{table_name}: {count} records")
        
        # Get round-robin partition statistics
        print("\nRound-Robin Partition Statistics:")
        cursor.execute("""
            SELECT table_name, COUNT(*) as record_count
            FROM information_schema.tables 
            WHERE table_name LIKE 'ratings_roundrobin_%'
            GROUP BY table_name
            ORDER BY table_name
        """)
        
        for table_name, count in cursor.fetchall():
            print(f"{table_name}: {count} records")
            
    except Exception as e:
        print(f"Error getting partition statistics: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def verify_data_integrity():
    """Verify data integrity across all tables"""
    conn = psycopg2.connect(**DatabaseConfig.get_connection_params())
    cursor = conn.cursor()
    
    try:
        # Check total records in main table
        cursor.execute("SELECT COUNT(*) FROM Ratings")
        main_count = cursor.fetchone()[0]
        
        # Check total records in range partitions
        cursor.execute("""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) as cnt FROM information_schema.tables t
                JOIN pg_class c ON c.relname = t.table_name
                WHERE t.table_name LIKE 'ratings_range_%' AND t.table_schema = 'public'
            ) as counts
        """)
        range_count = cursor.fetchone()[0] or 0
        
        # Check total records in round robin partitions
        cursor.execute("""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) as cnt FROM information_schema.tables t
                JOIN pg_class c ON c.relname = t.table_name
                WHERE t.table_name LIKE 'ratings_roundrobin_%' AND t.table_schema = 'public'
            ) as counts
        """)
        rrobin_count = cursor.fetchone()[0] or 0
        
        # Check data consistency
        print("\n=== DATA INTEGRITY CHECK ===")
        print(f"Total records in main table: {main_count}")
        print(f"Total records in range partitions: {range_count}")
        print(f"Total records in round robin partitions: {rrobin_count}")
        
        # Check for missing or extra records
        if main_count != range_count or main_count != rrobin_count:
            print("WARNING: Record counts don't match between tables!")
            
            # Check for missing records in detail
            cursor.execute("""
                SELECT UserID, MovieID, Rating FROM Ratings
                EXCEPT
                SELECT UserID, MovieID, Rating FROM ratings_range_0
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_range_1
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_range_2
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_range_3
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_range_4
            """)
            missing_in_range = cursor.fetchall()
            if missing_in_range:
                print(f"Records missing from range partitions: {len(missing_in_range)}")
                
            cursor.execute("""
                SELECT UserID, MovieID, Rating FROM Ratings
                EXCEPT
                SELECT UserID, MovieID, Rating FROM ratings_roundrobin_0
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_roundrobin_1
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_roundrobin_2
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_roundrobin_3
                UNION ALL
                SELECT UserID, MovieID, Rating FROM ratings_roundrobin_4
            """)
            missing_in_rrobin = cursor.fetchall()
            if missing_in_rrobin:
                print(f"Records missing from round robin partitions: {len(missing_in_rrobin)}")
        else:
            print("Data is consistent across all tables.")
            
    except Exception as e:
        print(f"Error checking data integrity: {e}")
    finally:
        cursor.close()
        conn.close() 