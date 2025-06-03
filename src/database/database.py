import psycopg2
from config.config import DatabaseConfig

def get_connection():
    """Tạo kết nối đến PostgreSQL database"""
    try:
        db_config = DatabaseConfig.get_instance()
        conn = psycopg2.connect(
            host=db_config.host,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
            port=db_config.port
        )
        return conn
    except psycopg2.Error as e:
        print(f"Lỗi kết nối database: {e}")
        raise

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    
    Args:
        ratingstablename: Tên bảng ratings
        ratingsfilepath: Đường dẫn đến file ratings.dat
        openconnection: Kết nối database
    """
    cursor = openconnection.cursor()
    
    try:
        # Tạo bảng nếu chưa tồn tại
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {ratingstablename} (
                userid INT,
                movieid INT,
                rating FLOAT,
                PRIMARY KEY (userid, movieid)
            )
        """)
        
        # Xóa dữ liệu cũ nếu có
        cursor.execute(f"DELETE FROM {ratingstablename}")
        
        # Đọc và chèn dữ liệu từ file
        with open(ratingsfilepath, 'r') as f:
            batch_size = 10000
            batch = []
            count = 0
            
            for line in f:
                # Parse dữ liệu từ file
                userid, movieid, rating, _ = line.strip().split('::')
                batch.append((int(userid), int(movieid), float(rating)))
                count += 1
                
                # Chèn theo batch để tối ưu hiệu năng
                if len(batch) >= batch_size:
                    cursor.executemany(f"""
                        INSERT INTO {ratingstablename} (userid, movieid, rating)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (userid, movieid) DO UPDATE 
                        SET rating = EXCLUDED.rating
                    """, batch)
                    batch = []
            
            # Chèn batch cuối cùng nếu còn
            if batch:
                cursor.executemany(f"""
                    INSERT INTO {ratingstablename} (userid, movieid, rating)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (userid, movieid) DO UPDATE 
                    SET rating = EXCLUDED.rating
                """, batch)
        
        openconnection.commit()
        print(f"Đã load {count} records vào bảng {ratingstablename}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi load ratings: {e}")
        raise
    finally:
        cursor.close() 