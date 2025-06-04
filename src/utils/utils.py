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

    """Verify if ratings were loaded successfully by checking record counts"""
    cursor = conn.cursor()
    
    try:
        # Get total count from ratings table
        cursor.execute("SELECT COUNT(*) FROM ratings")
        total_count = cursor.fetchone()[0]
        
        print("\n=== RATINGS LOAD VERIFICATION ===")
        print(f"Total ratings loaded: {total_count:,} records")
        
        # Get some sample data to verify content
        cursor.execute("""
            SELECT userid, movieid, rating 
            FROM ratings 
            LIMIT 5
        """)
        sample_data = cursor.fetchall()
        
        print("\nSample ratings data:")
        for user_id, movie_id, rating in sample_data:
            print(f"User {user_id} rated Movie {movie_id}: {rating} stars")
            
        return total_count > 0
        
    except Exception as e:
        print(f"Error verifying ratings load: {e}")
        return False
    finally:
        cursor.close() 