import psycopg2
from config.config import DatabaseConfig

def get_connection():
    """Create connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DatabaseConfig.get_connection_params())
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    
    Args:
        ratingstablename: Name of the ratings table
        ratingsfilepath: Path to the ratings.dat file
        openconnection: Database connection
    """
    cursor = openconnection.cursor()
    
    try:
        # Create table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {ratingstablename} (
                userid INT,
                movieid INT,
                rating FLOAT,
                PRIMARY KEY (userid, movieid)
            )
        """)
        
        # Clear existing data if any
        cursor.execute(f"DELETE FROM {ratingstablename}")
        
        # Read and insert data from file
        with open(ratingsfilepath, 'r') as f:
            batch_size = 10000
            batch = []
            count = 0
            
            for line in f:
                # Parse data from file
                userid, movieid, rating, _ = line.strip().split('::')
                batch.append((int(userid), int(movieid), float(rating)))
                count += 1
                
                # Insert in batches for better performance
                if len(batch) >= batch_size:
                    cursor.executemany(f"""
                        INSERT INTO {ratingstablename} (userid, movieid, rating)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (userid, movieid) DO UPDATE 
                        SET rating = EXCLUDED.rating
                    """, batch)
                    batch = []
            
            # Insert remaining batch if any
            if batch:
                cursor.executemany(f"""
                    INSERT INTO {ratingstablename} (userid, movieid, rating)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (userid, movieid) DO UPDATE 
                    SET rating = EXCLUDED.rating
                """, batch)
        
        openconnection.commit()
        print(f"Loaded {count} records into table {ratingstablename}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Error loading ratings: {e}")
        raise
    finally:
        cursor.close() 