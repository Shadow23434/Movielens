import psycopg2
from psycopg2.extras import execute_values
import traceback
import time

# Đã bỏ DEBUG_MODE

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Create range partitions for the ratings table.
    
    Args:
        ratingstablename: Name of the ratings table
        numberofpartitions: Number of partitions to create
        openconnection: Database connection
    """
    cursor = openconnection.cursor()
    start_time = time.time() # Giữ lại để tính tổng thời gian

    try:
        # Drop existing partition tables
        for i in range(numberofpartitions):
            partition_name = f"{RANGE_TABLE_PREFIX}{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {partition_name};")
        
        # Xóa bảng metadata cho range partition
        cursor.execute("DROP TABLE IF EXISTS range_metadata;") 
        openconnection.commit()

        # Get min and max ratings
        cursor.execute(f"SELECT MIN(rating), MAX(rating) FROM {ratingstablename}")
        min_rating, max_rating = cursor.fetchone()

        if min_rating is None or max_rating is None:
            raise Exception(f"Bảng {ratingstablename} trống hoặc không tìm thấy min/max rating. Vui lòng tải dữ liệu trước.")

        # Calculate partition range
        range_size = (max_rating - min_rating) / numberofpartitions

        # Tạo metadata cho Range Partitioning
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS range_metadata (
                id SERIAL PRIMARY KEY,
                min_rating FLOAT NOT NULL,
                max_rating FLOAT NOT NULL,
                num_partitions INT NOT NULL
            );
        """)
        cursor.execute("INSERT INTO range_metadata (min_rating, max_rating, num_partitions) VALUES (%s, %s, %s);",
                       (min_rating, max_rating, numberofpartitions))
        openconnection.commit()

        # Create partition tables and insert data
        for i in range(numberofpartitions):
            partition_name = f"{RANGE_TABLE_PREFIX}{i}"
            lower_bound = min_rating + (i * range_size)
            upper_bound = min_rating + ((i + 1) * range_size)
            
            # Special handling for the last partition to include max_rating
            if i == numberofpartitions - 1:
                where_clause = f"rating >= {lower_bound} AND rating <= {upper_bound}"
            else:
                where_clause = f"rating >= {lower_bound} AND rating < {upper_bound}"

            # Create partition table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {partition_name} (
                    userid INT,
                    movieid INT,
                    rating FLOAT,
                    PRIMARY KEY (userid, movieid, Rating)
                )
            """)
            
            # Insert data into partition
            cursor.execute(f"""
                INSERT INTO {partition_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE {where_clause}
            """)
        
        openconnection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[{ratingstablename}] RangePartition hoàn thành trong {elapsed_time:.2f} giây.")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Error creating range partitions: {e}")
        traceback.print_exc()
        raise
    finally:
        cursor.close()


## RoundRobin_Partition Function
def roundrobinpartition(ratingstablename: str, N: int, open_connection):
    start_time = time.time()

    if not isinstance(N, int) or N <= 0:
        print(f"Lỗi: Số phân mảnh N ({N}) phải là số nguyên dương (N >= 1).")
        return

    cursor = open_connection.cursor()

    try:
        # Step 1: Drop old Round Robin partition tables and metadata table (if they exist)
        for i in range(N):
            partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {partition_name};")
        cursor.execute("DROP TABLE IF EXISTS rrobin_metadata;")
        open_connection.commit()

        # Step 2: Create metadata table to store insertion index and number of partitions
        cursor.execute("""
            CREATE TABLE rrobin_metadata (
                id SERIAL PRIMARY KEY,
                current_insert_index BIGINT NOT NULL DEFAULT 0,
                num_partitions INT NOT NULL
            );
        """)
        cursor.execute("INSERT INTO rrobin_metadata (num_partitions) VALUES (%s);", (N,))
        open_connection.commit()

        # Step 3: Create N child tables (partitions) with schema similar to Ratings
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

        # Step 4: Insert data into partitions using SQL directly (OPTIMIZED!)
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
        
        # Step 5: Update the final insertion index in the metadata table
        cursor.execute("UPDATE rrobin_metadata SET current_insert_index = %s WHERE id = 1;", (total_records_processed,))
        
        # Reset current_insert_index to 0 for subsequent single inserts by tester
        cursor.execute("UPDATE rrobin_metadata SET current_insert_index = 0 WHERE id = 1;")

        open_connection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[{ratingstablename}] RoundRobinPartition hoàn thành trong {elapsed_time:.2f} giây.")

    except psycopg2.Error as e:
        print(f"Lỗi PostgreSQL khi thực hiện RoundRobin_Partition: {e}")
        if open_connection: open_connection.rollback()
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"Lỗi chung khi thực hiện RoundRobin_Partition: {e}")
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
        cursor.execute(f"""
            INSERT INTO {partition_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))
        
        openconnection.commit()
        print(f"Inserted rating into range partition {partition_num}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Error inserting rating: {e}")
        raise
    finally:
        cursor.close()

## RoundRobin_Insert Function
def roundrobininsert(ratingstablename, UserID: int, MovieID: int, Rating: float, openconnection):
    """
    Chèn một bộ dữ liệu mới vào đúng phân mảnh Round Robin.
    """
    start_time = time.time()
    cursor = openconnection.cursor()
    try:
        # Step 1: Get current insertion index and number of partitions from metadata table
        cursor.execute("SELECT current_insert_index, num_partitions FROM rrobin_metadata WHERE id = 1 FOR UPDATE;")
        metadata = cursor.fetchone()

        if not metadata:
            print("Lỗi: Không tìm thấy thông tin metadata Round Robin. Vui lòng chạy RoundRobin_Partition() trước.")
            openconnection.rollback()
            return

        current_insert_index, N = metadata[0], metadata[1]

        if N <= 0:
            print(f"Lỗi: Số lượng phân mảnh N trong metadata ({N}) không hợp lệ.")
            openconnection.rollback()
            return

        # Step 2: Determine the target partition table name
        partition_index = current_insert_index % N
        target_table = f"{RROBIN_TABLE_PREFIX}{partition_index}"

        # Step 3: Insert the new record into the target partition table
        cursor.execute(f"""
            INSERT INTO {target_table} (UserID, MovieID, Rating)
            VALUES (%s, %s, %s);
        """, (UserID, MovieID, Rating))

        # Step 4: Update insertion index in the metadata table
        new_insert_index = current_insert_index + 1
        cursor.execute("UPDATE rrobin_metadata SET current_insert_index = %s WHERE id = 1;", (new_insert_index,))

        openconnection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[Round Robin Insert] Insert ({UserID}, {MovieID}, {Rating}) vào {target_table} hoàn thành trong {elapsed_time:.4f} giây. (Index: {new_insert_index - 1})")

    except psycopg2.Error as e:
        print(f"Lỗi PostgreSQL khi chèn dữ liệu vào Round Robin Partition: {e}")
        if openconnection: openconnection.rollback()
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"Lỗi chung khi chèn dữ liệu vào Round Robin Partition: {e}")
        if openconnection: openconnection.rollback()
        traceback.print_exc()
        raise
    finally:
        if cursor:
            cursor.close()
