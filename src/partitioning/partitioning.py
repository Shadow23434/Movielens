import psycopg2
import traceback
import time

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Create range partitions for the ratings table.
    """
    print(f"\n--- Starting RANGE partitioning with {numberofpartitions} partitions ---")
    cursor = openconnection.cursor()

    try:
        # Drop old partitions safely
        for i in range(numberofpartitions):
            partition_name = f"{RANGE_TABLE_PREFIX}{i}"
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
            except Exception as e:
                print(f"Warning: Could not drop table {partition_name}: {e}")
                openconnection.rollback()

        # Reset transaction
        openconnection.commit()

        # Get min and max rating
        cursor.execute(f"SELECT MIN(rating), MAX(rating) FROM {ratingstablename}")
        min_rating, max_rating = cursor.fetchone()

        # Calculate partition size
        range_size = (max_rating - min_rating) / numberofpartitions

        for i in range (numberofpartitions):
            partition_name = f"{RANGE_TABLE_PREFIX}{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {partition_name} CASCADE;")

        for i in range(numberofpartitions):
            partition_name = f"{RANGE_TABLE_PREFIX}{i}"
            lower_bound = min_rating + i * range_size
            upper_bound = min_rating + (i + 1) * range_size

            # Handle last partition with inclusive upper bound
            if i == numberofpartitions - 1:
                condition = f"rating >= {lower_bound} AND rating <= {upper_bound}"
            else:
                condition = f"rating >= {lower_bound} AND rating < {upper_bound}"

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {partition_name} (
                    userid INT,
                    movieid INT,
                    rating FLOAT
                    
                )
            """)
            cursor.execute(f"""
                INSERT INTO {partition_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE {condition}
            """)

        openconnection.commit()
        print(f"Created {numberofpartitions} range partitions")
        print(f"--- Finished RANGE partitioning ---\n")

    except Exception as e:
        openconnection.rollback()
        print(f"Error creating range partitions: {e}")
        raise

    finally:
        cursor.close()

def roundrobinpartition(ratingstablename: str, N: int, open_connection):
    print(f"\n--- Starting ROUND ROBIN partitioning with {N} partitions ---")
    start_time = time.time()

    if not isinstance(N, int) or N <= 0:
        print(f"Error: Number of partitions N ({N}) must be a positive integer (N >= 1).")
        return

    cursor = open_connection.cursor()
    print("\nStarting Round Robin Partitioning...")

    try:
        # Drop old Round Robin partition tables and metadata table (if they exist)
        for i in range(N):
            partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {partition_name};")
        cursor.execute("DROP TABLE IF EXISTS rrobin_metadata;")
        open_connection.commit()

        # Create metadata table to store insertion index and number of partitions
        cursor.execute("""
            CREATE TABLE rrobin_metadata (
                id SERIAL PRIMARY KEY,
                current_insert_index BIGINT NOT NULL DEFAULT 0,
                num_partitions INT NOT NULL
            );
        """)
        cursor.execute("INSERT INTO rrobin_metadata (num_partitions) VALUES (%s);", (N,))
        open_connection.commit()

        # Create N child tables (partitions) with schema similar to Ratings
        for i in range(N):
            partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cursor.execute(f"""
                CREATE TABLE {partition_name} (
                    UserID INT,
                    MovieID INT,
                    Rating FLOAT,
                    PRIMARY KEY (UserID, MovieID, Rating)
                );
            """)
        open_connection.commit()

        # Insert data into partitions using SQL directly (optimized)
        total_records_processed = 0
        for i in range(N):
            partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cursor.execute(f"""
                INSERT INTO {partition_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
                SELECT UserID, MovieID, Rating
                FROM (
                    SELECT UserID, MovieID, Rating,
                           ROW_NUMBER() OVER (ORDER BY UserID, MovieID, Rating) as rn
                    FROM {ratingstablename}
                ) AS numbered_ratings
                WHERE (rn - 1) % {N} = {i};
            """)
            rows_inserted = cursor.rowcount
            total_records_processed += rows_inserted
        
        # Update the final insertion index in the metadata table
        cursor.execute("UPDATE rrobin_metadata SET current_insert_index = %s WHERE id = 1;", (total_records_processed,))
        
        # Reset current_insert_index to 0 for subsequent single inserts by tester
        cursor.execute("UPDATE rrobin_metadata SET current_insert_index = 0 WHERE id = 1;")

        open_connection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[{ratingstablename}] RoundRobinPartition completed in {elapsed_time:.2f} seconds.")
        print("Round Robin Partitioning completed.")
        print(f"--- Finished ROUND ROBIN partitioning ---\n")

    except psycopg2.Error as e:
        print(f"PostgreSQL error during RoundRobin_Partition: {e}")
        if open_connection: open_connection.rollback()
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General error during RoundRobin_Partition: {e}")
        if open_connection: open_connection.rollback()
        traceback.print_exc()
        raise
    finally:
        if cursor and not cursor.closed:
            cursor.close()

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    """
    Insert a new rating using range partitioning.
    
    Args:
        ratingstablename: Name of the ratings table
        userid: User ID
        movieid: Movie ID
        rating: Rating value
        openconnection: Database connection
    """
    cursor = openconnection.cursor()
    
    try:
        # Get min and max ratings
        cursor.execute(f"SELECT MIN(rating), MAX(rating) FROM {ratingstablename}")
        min_rating, max_rating = cursor.fetchone()

        # Get number of partitions from existing range partition tables
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name LIKE '{RANGE_TABLE_PREFIX}%'
        """)
        numberofpartitions = cursor.fetchone()[0]

        if numberofpartitions == 0:
            raise Exception("No range partitions found. Please run rangepartition first.")

        # Calculate partition number
        range_size = (max_rating - min_rating) / numberofpartitions
        partition_num = int((rating - min_rating) / range_size)
        partition_name = f"{RANGE_TABLE_PREFIX}{partition_num}"

        # Check if partition exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{partition_name}'
            )
        """)
        if not cursor.fetchone()[0]:
            raise Exception(f"Partition {partition_name} does not exist")

        # Insert into appropriate partition
        cursor.execute(
            f"INSERT INTO {partition_name} (userid,movieid,rating) VALUES (%s, %s, %s)", (userid, movieid, rating)
        )

        openconnection.commit()
        print(f"Inserted rating into range partition {partition_num}")

    except Exception as e:
        openconnection.rollback()
        print(f"Error inserting rating: {e}")
        raise
    finally:
        cursor.close()

def roundrobininsert(ratingstablename, UserID: int, MovieID: int, Rating: float, openconnection):
    """
    Insert a new record into the correct Round Robin partition.
    """
    start_time = time.time()
    cursor = openconnection.cursor()
    try:
        # Get current insertion index and number of partitions from metadata table
        cursor.execute("SELECT current_insert_index, num_partitions FROM rrobin_metadata WHERE id = 1 FOR UPDATE;")
        metadata = cursor.fetchone()

        if not metadata:
            print("Error: Round Robin metadata not found. Please run RoundRobin_Partition() first.")
            openconnection.rollback()
            return

        current_insert_index, N = metadata[0], metadata[1]

        if N <= 0:
            print(f"Error: Number of partitions N in metadata ({N}) is invalid.")
            openconnection.rollback()
            return

        # Determine the target partition table name
        partition_index = current_insert_index % N
        target_table = f"{RROBIN_TABLE_PREFIX}{partition_index}"

        # Insert the new record into the target partition table
        cursor.execute(f"""
            INSERT INTO {target_table} (UserID, MovieID, Rating)
            VALUES (%s, %s, %s);
        """, (UserID, MovieID, Rating))

        # Update insertion index in the metadata table
        new_insert_index = current_insert_index + 1
        cursor.execute("UPDATE rrobin_metadata SET current_insert_index = %s WHERE id = 1;", (new_insert_index,))

        openconnection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[Round Robin Insert] Insert ({UserID}, {MovieID}, {Rating}) into {target_table} completed in {elapsed_time:.4f} seconds. (Index: {new_insert_index - 1})")

    except psycopg2.Error as e:
        print(f"PostgreSQL error during Round Robin Partition insert: {e}")
        if openconnection: openconnection.rollback()
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General error during Round Robin Partition insert: {e}")
        if openconnection: openconnection.rollback()
        traceback.print_exc()
        raise
    finally:
        if cursor:
            cursor.close()
