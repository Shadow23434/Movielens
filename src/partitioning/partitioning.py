import psycopg2
from config.config import DatabaseConfig

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Create range partitions for the ratings table.
    
    Args:
        ratingstablename: Name of the ratings table
        numberofpartitions: Number of partitions to create
        openconnection: Database connection
    """
    cursor = openconnection.cursor()
    
    try:
        # Get min and max ratings
        cursor.execute(f"SELECT MIN(rating), MAX(rating) FROM {ratingstablename}")
        min_rating, max_rating = cursor.fetchone()
        
        # Calculate partition range
        range_size = (max_rating - min_rating) / numberofpartitions
        
        # Create partition tables
        for i in range(numberofpartitions):
            partition_name = f"{RANGE_TABLE_PREFIX}{i}"
            lower_bound = min_rating + (i * range_size)
            upper_bound = min_rating + ((i + 1) * range_size)
            
            # Create partition table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {partition_name} (
                    userid INT,
                    movieid INT,
                    rating FLOAT
                )
            """)
            
            # Insert data into partition
            cursor.execute(f"""
                INSERT INTO {partition_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating >= {lower_bound} AND rating < {upper_bound}
            """)
            
        openconnection.commit()
        print(f"Created {numberofpartitions} range partitions")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Error creating range partitions: {e}")
        raise
    finally:
        cursor.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Create round-robin partitions for the ratings table.
    
    Args:
        ratingstablename: Name of the ratings table
        numberofpartitions: Number of partitions to create
        openconnection: Database connection
    """
    cursor = openconnection.cursor()
    
    try:
        # Create partition tables
        for i in range(numberofpartitions):
            partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
            
            # Create partition table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {partition_name} (
                    userid INT,
                    movieid INT,
                    rating FLOAT
                )
            """)
            
            # Insert data into partition using modulo
            cursor.execute(f"""
                INSERT INTO {partition_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE MOD(userid, {numberofpartitions}) = {i}
            """)
            
        openconnection.commit()
        print(f"Created {numberofpartitions} round-robin partitions")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Error creating round-robin partitions: {e}")
        raise
    finally:
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
        
        # Calculate partition number
        range_size = (max_rating - min_rating) / 5  # Assuming 5 partitions
        partition_num = int((rating - min_rating) / range_size)
        partition_name = f"{RANGE_TABLE_PREFIX}{partition_num}"
        
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

def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    """
    Insert a new rating using round-robin partitioning.
    
    Args:
        ratingstablename: Name of the ratings table
        userid: User ID
        movieid: Movie ID
        rating: Rating value
        openconnection: Database connection
    """
    cursor = openconnection.cursor()
    
    try:
        # Calculate partition number
        partition_num = userid % 5  # Assuming 5 partitions
        partition_name = f"{RROBIN_TABLE_PREFIX}{partition_num}"
        
        # Insert into appropriate partition
        cursor.execute(f"""
            INSERT INTO {partition_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))
        
        openconnection.commit()
        print(f"Inserted rating into round-robin partition {partition_num}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Error inserting rating: {e}")
        raise
    finally:
        cursor.close() 