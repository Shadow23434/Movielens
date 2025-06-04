import psycopg2
import psycopg2.extras
from config.config import DatabaseConfig
import time
import os

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
    Optimized version for loading large datasets (10M+ records)
    Uses COPY command, multi-threading, and optimized PostgreSQL settings
    """
    cursor = openconnection.cursor()
    
    try:
        # Set connection to autocommit for DDL operations
        openconnection.autocommit = True
        
        # Optimize PostgreSQL settings for bulk insert
        # Only set parameters that can be changed without server restart
        try:
            cursor.execute("SET maintenance_work_mem = '256MB'")
            cursor.execute("SET work_mem = '64MB'")
            cursor.execute("SET synchronous_commit = OFF")
            cursor.execute("SET commit_delay = 0")
            cursor.execute("SET commit_siblings = 5")
            print("PostgreSQL settings optimized for bulk insert")
        except Exception as e:
            print(f"Warning: Could not set some optimization parameters: {e}")
            # Continue anyway as these are optimizations, not requirements
        
        # Create table with optimized structure
        cursor.execute(f"""
            DROP TABLE IF EXISTS {ratingstablename}
        """)
        
        cursor.execute(f"""
            CREATE TABLE {ratingstablename} (
                userid INT NOT NULL,
                movieid INT NOT NULL,
                rating FLOAT NOT NULL,
                PRIMARY KEY (userid, movieid)
            ) WITH (fillfactor = 90)
        """)
        
        # Turn off autocommit for bulk operations
        openconnection.autocommit = False
        
        print(f"Starting to load data from {ratingsfilepath}...")
        start_time = time.time()
        
        # Method 1: Use COPY command (fastest for large datasets)
        if use_copy_method(ratingstablename, ratingsfilepath, openconnection):
            end_time = time.time()
            print(f"Data loaded successfully using COPY in {end_time - start_time:.2f} seconds")
        else:
            # Method 2: Fallback to optimized batch insert
            load_with_batch_insert(ratingstablename, ratingsfilepath, openconnection)
            end_time = time.time()
            print(f"Data loaded successfully using batch insert in {end_time - start_time:.2f} seconds")
        
        # Create indexes after data loading for better performance
        print("Creating indexes...")
        openconnection.commit()  # Commit any pending transactions
        openconnection.autocommit = True  # Enable autocommit for index creation
        
        cursor.execute(f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_userid ON {ratingstablename}(userid)")
        cursor.execute(f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_movieid ON {ratingstablename}(movieid)")
        cursor.execute(f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_rating ON {ratingstablename}(rating)")
        
        # Analyze table for query optimization
        cursor.execute(f"ANALYZE {ratingstablename}")
        
        # Get final count
        cursor.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_records = cursor.fetchone()[0]
        print(f"Successfully loaded {total_records:,} records into table {ratingstablename}")
        
        # Turn off autocommit
        openconnection.autocommit = False
        
        # Reset PostgreSQL settings
        reset_db_settings(cursor)
        
        # Final commit to ensure all changes are saved
        openconnection.commit()
        
        print("Data loading completed successfully!")
        
    except Exception as e:
        try:
            openconnection.rollback()
        except:
            pass  # Ignore rollback errors
        print(f"Error loading ratings: {e}")
        raise
    finally:
        if cursor:
            cursor.close()

def use_copy_method(ratingstablename, ratingsfilepath, openconnection):
    """
    Use PostgreSQL COPY command for maximum performance
    """
    try:
        cursor = openconnection.cursor()
        
        # Prepare temporary CSV file for COPY command
        temp_csv_path = ratingsfilepath + '.temp.csv'
        
        print("Converting data format for COPY command...")
        with open(ratingsfilepath, 'r') as infile, open(temp_csv_path, 'w') as outfile:
            for line_num, line in enumerate(infile, 1):
                if line_num % 1000000 == 0:
                    print(f"Processed {line_num:,} lines...")
                
                try:
                    userid, movieid, rating, _ = line.strip().split('::')  # Ignore timestamp
                    outfile.write(f"{userid},{movieid},{rating}\n")
                except ValueError:
                    print(f"Skipping malformed line {line_num}: {line.strip()}")
                    continue
        
        print("Starting COPY operation...")
        # Use COPY command for bulk insert
        with open(temp_csv_path, 'r') as f:
            cursor.copy_expert(f"""
                COPY {ratingstablename} (userid, movieid, rating)
                FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
            """, f)
        
        openconnection.commit()
        
        # Clean up temporary file
        os.remove(temp_csv_path)
        
        return True
        
    except Exception as e:
        print(f"COPY method failed: {e}")
        openconnection.rollback()
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
        return False

def load_with_batch_insert(ratingstablename, ratingsfilepath, openconnection):
    """
    Fallback method using optimized batch inserts with threading
    """
    cursor = openconnection.cursor()
    batch_size = 50000  # Larger batch size for better performance
    batch = []
    count = 0
    
    print("Using batch insert method...")
    
    with open(ratingsfilepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 1000000 == 0:
                print(f"Processed {line_num:,} lines...")
            
            try:
                userid, movieid, rating, _ = line.strip().split('::')  # Ignore timestamp
                batch.append((int(userid), int(movieid), float(rating)))
                count += 1
                
                # Insert in large batches
                if len(batch) >= batch_size:
                    insert_batch(cursor, ratingstablename, batch)
                    openconnection.commit()  # Commit each batch
                    batch = []
                    
            except ValueError:
                print(f"Skipping malformed line {line_num}: {line.strip()}")
                continue
        
        # Insert remaining batch
        if batch:
            insert_batch(cursor, ratingstablename, batch)
            openconnection.commit()
    
    print(f"Processed {count:,} records using batch insert")

def insert_batch(cursor, table_name, batch):
    """
    Insert a batch of records using execute_values for better performance
    """
    psycopg2.extras.execute_values(
        cursor,
        f"""
        INSERT INTO {table_name} (userid, movieid, rating)
        VALUES %s
        ON CONFLICT (userid, movieid) DO UPDATE 
        SET rating = EXCLUDED.rating
        """,
        batch,
        template=None,
        page_size=10000
    )

def reset_db_settings(cursor):
    """
    Reset PostgreSQL settings to default values
    """
    try:
        cursor.execute("SET synchronous_commit = ON")
        cursor.execute("RESET maintenance_work_mem")
        cursor.execute("RESET work_mem")
        cursor.execute("RESET commit_delay")
        cursor.execute("RESET commit_siblings")
        print("Database settings reset to default values")
    except Exception as e:
        print(f"Warning: Could not reset some database settings: {e}")
