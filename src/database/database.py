import psycopg2
import psycopg2.extras
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from config.config import DatabaseConfig

def get_connection():
    """Create connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DatabaseConfig.get_connection_params())
        print("Database connection established.")
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
    print("\nStarting data loading into main 'ratings' table...")
    
    try:
        # Optimize PostgreSQL settings for bulk insert - only runtime changeable parameters
        optimization_settings = [
            "SET maintenance_work_mem = '512MB'",
            "SET work_mem = '128MB'", 
            "SET synchronous_commit = OFF",
            "SET commit_delay = 0",
            "SET commit_siblings = 5",
            "SET temp_buffers = '32MB'",
            "SET effective_cache_size = '512MB'"
        ]
        
        # Apply settings one by one with individual error handling
        for setting in optimization_settings:
            try:
                cursor.execute(setting)
            except Exception as e:
                print(f"Warning: Could not apply '{setting}': {e}")
                # Rollback the failed command and continue
                openconnection.rollback()
                cursor = openconnection.cursor()  # Get a fresh cursor
        
        print("PostgreSQL settings optimization completed")
        
        # Create table with optimized structure
        cursor.execute(f"""
            DROP TABLE IF EXISTS {ratingstablename}
        """)
        
        cursor.execute(f"""
            CREATE UNLOGGED TABLE {ratingstablename} (
                userid INT NOT NULL,
                movieid INT NOT NULL,
                rating FLOAT NOT NULL,
                PRIMARY KEY (userid, movieid)
            ) WITH (fillfactor = 90)
        """)
        
        print(f"Starting to load data from {ratingsfilepath}...")
        start_time = time.time()
        
        # Determine file size to choose optimal method
        file_size = os.path.getsize(ratingsfilepath)
        
        # Method 1: Use COPY command (fastest for large datasets)
        if file_size > 50 * 1024 * 1024:  # Files larger than 50MB
            if use_copy_method(ratingstablename, ratingsfilepath, openconnection):
                end_time = time.time()
                print(f"Data loaded successfully using COPY in {end_time - start_time:.2f} seconds")
            else:
                # Method 2: Fallback to parallel batch insert
                load_with_parallel_insert(ratingstablename, ratingsfilepath, openconnection)
                end_time = time.time()
                print(f"Data loaded successfully using parallel batch insert in {end_time - start_time:.2f} seconds")
        else:
            # Method 3: Optimized batch insert for smaller files
            load_with_batch_insert(ratingstablename, ratingsfilepath, openconnection)
            end_time = time.time()
            print(f"Data loaded successfully using batch insert in {end_time - start_time:.2f} seconds")
        
        # Convert UNLOGGED table to LOGGED after data loading
        cursor.execute(f"ALTER TABLE {ratingstablename} SET LOGGED")

        # Commit current transaction before creating indexes, only if not autocommit
        if not getattr(openconnection, 'autocommit', False):
            openconnection.commit()

        # Create indexes after data loading for better performance
        print("Creating indexes...")
        create_indexes_safely(ratingstablename, openconnection)

        # Analyze table for query optimization
        cursor = openconnection.cursor()  # Get fresh cursor after index creation
        cursor.execute(f"ANALYZE {ratingstablename}")
        
        # Get final count
        cursor.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_records = cursor.fetchone()[0]
        print(f"Successfully loaded {total_records:,} records into table {ratingstablename}")
        
        # Reset PostgreSQL settings
        reset_db_settings(cursor)
        
        # Final commit to ensure all changes are saved - Tester will handle this.
        # openconnection.commit()
        
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
    Use PostgreSQL COPY command for maximum performance with streaming
    """
    try:
        cursor = openconnection.cursor()
        
        print("Starting optimized COPY operation...")
        
        # Create a generator for streaming data processing
        def data_generator():
            with open(ratingsfilepath, 'r', encoding='utf-8', buffering=8192*8) as infile:
                line_count = 0
                for line in infile:
                    line_count += 1
                    if line_count % 1000000 == 0:
                        print(f"Processed {line_count:,} lines...")
                    
                    try:
                        parts = line.strip().split('::')
                        if len(parts) >= 3:
                            userid, movieid, rating = parts[0], parts[1], parts[2]
                            yield f"{userid},{movieid},{rating}\n"
                    except (ValueError, IndexError):
                        continue  # Skip malformed lines
        
        # Use copy_expert with generator-based streaming
        import io
        
        def generate_csv_data():
            return ''.join(data_generator())
        
        csv_data = generate_csv_data()
        csv_buffer = io.StringIO(csv_data)
        
        # Use COPY command for bulk insert
        cursor.copy_expert(f"""
            COPY {ratingstablename} (userid, movieid, rating)
            FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """, csv_buffer)
        
        openconnection.commit()
        return True
        
    except Exception as e:
        print(f"COPY method failed: {e}")
        try:
            openconnection.rollback()
        except:
            pass
        return False

def load_with_parallel_insert(ratingstablename, ratingsfilepath, openconnection):
    """
    Parallel processing method using multiple threads for batch inserts
    """
    print("Using parallel batch insert method...")
    
    # Determine optimal number of threads based on CPU cores
    num_threads = min(multiprocessing.cpu_count(), 4)  # Limit to 4 threads to avoid overwhelming DB
    batch_size = 100000  # Larger batch size for parallel processing
    
    # First, read file and split into chunks
    chunks = []
    current_chunk = []
    
    with open(ratingsfilepath, 'r', encoding='utf-8', buffering=8192*8) as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 1000000 == 0:
                print(f"Reading: {line_num:,} lines...")
            
            try:
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    userid, movieid, rating = parts[0], parts[1], parts[2]
                    current_chunk.append((int(userid), int(movieid), float(rating)))
                    
                    if len(current_chunk) >= batch_size:
                        chunks.append(current_chunk)
                        current_chunk = []
            except (ValueError, IndexError):
                continue  # Skip malformed lines
    
    # Add remaining chunk
    if current_chunk:
        chunks.append(current_chunk)
    
    print(f"Created {len(chunks)} chunks for parallel processing")
    
    # Process chunks in parallel
    def process_chunk(chunk_data):
        try:
            # Create a new connection for this thread
            conn_params = openconnection.get_dsn_parameters()
            thread_conn = psycopg2.connect(**conn_params)
            thread_cursor = thread_conn.cursor()
            
            # Insert the chunk
            psycopg2.extras.execute_values(
                thread_cursor,
                f"""
                INSERT INTO {ratingstablename} (userid, movieid, rating)
                VALUES %s
                ON CONFLICT (userid, movieid) DO UPDATE 
                SET rating = EXCLUDED.rating
                """,
                chunk_data,
                template=None,
                page_size=10000
            )
            
            thread_conn.commit()
            thread_cursor.close()
            thread_conn.close()
            return len(chunk_data)
            
        except Exception as e:
            print(f"Error in thread processing: {e}")
            return 0
    
    # Execute parallel processing
    total_processed = 0
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        
        for i, future in enumerate(futures):
            processed = future.result()
            total_processed += processed
            print(f"Chunk {i+1}/{len(chunks)} processed: {processed:,} records")
    
    print(f"Parallel processing completed: {total_processed:,} records")

def load_with_batch_insert(ratingstablename, ratingsfilepath, openconnection):
    """
    Optimized batch insert method with improved buffer management
    """
    cursor = openconnection.cursor()
    batch_size = 75000  # Optimized batch size
    batch = []
    count = 0
    
    print("Using optimized batch insert method...")
    
    with open(ratingsfilepath, 'r', encoding='utf-8', buffering=8192*8) as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 500000 == 0:
                print(f"Processed {line_num:,} lines...")
            
            try:
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    userid, movieid, rating = parts[0], parts[1], parts[2]
                    batch.append((int(userid), int(movieid), float(rating)))
                    count += 1
                    
                    # Insert in optimized batches
                    if len(batch) >= batch_size:
                        insert_batch_optimized(cursor, ratingstablename, batch)
                        openconnection.commit()  # Commit each batch
                        batch = []
                        
            except (ValueError, IndexError):
                continue  # Skip malformed lines
        
        # Insert remaining batch
        if batch:
            insert_batch_optimized(cursor, ratingstablename, batch)
            openconnection.commit()
    
    print(f"Processed {count:,} records using optimized batch insert")

def insert_batch_optimized(cursor, table_name, batch):
    """
    Optimized batch insert using execute_values with larger page size
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
        page_size=15000  # Larger page size for better performance
    )

def create_indexes_safely(ratingstablename, openconnection):
    """
    Create indexes safely, handling CONCURRENTLY requirement
    """
    old_autocommit = getattr(openconnection, 'autocommit', False)
    try:
        # Set autocommit mode for CONCURRENT index creation
        openconnection.autocommit = True
        cursor = openconnection.cursor()
        indexes = [
            f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_userid ON {ratingstablename}(userid)",
            f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_movieid ON {ratingstablename}(movieid)", 
            f"CREATE INDEX CONCURRENTLY idx_{ratingstablename}_rating ON {ratingstablename}(rating)"
        ]
        for idx_sql in indexes:
            print(f"Creating index: {idx_sql}")
            try:
                cursor.execute(idx_sql)
            except Exception as e:
                print(f"Concurrent index creation failed: {e}")
                # Fallback: try regular index creation (not concurrently)
                try:
                    fallback_sql = idx_sql.replace("CONCURRENTLY ", "")
                    cursor.execute(fallback_sql)
                    print(f"Regular index creation succeeded: {fallback_sql}")
                except Exception as e2:
                    print(f"Regular index creation also failed: {e2}")
        cursor.close()
    except Exception as e:
        print(f"Error in index creation: {e}")
        print(f"Fallback index creation failed: {e}")
    finally:
        try:
            openconnection.autocommit = old_autocommit
        except Exception:
            pass

def reset_db_settings(cursor):
    """
    Reset PostgreSQL settings to default values
    """
    reset_settings = [
        "SET synchronous_commit = ON",
        "RESET maintenance_work_mem",
        "RESET work_mem",
        "RESET commit_delay", 
        "RESET commit_siblings",
        "RESET temp_buffers",
        "RESET effective_cache_size"
    ]
    
    for setting in reset_settings:
        try:
            cursor.execute(setting)
        except Exception as e:
            print(f"Warning: Could not reset '{setting}': {e}")
    
    print("Database settings reset completed")
