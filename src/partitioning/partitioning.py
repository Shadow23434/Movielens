from database import get_connection

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    
    Args:
        ratingstablename: Tên bảng ratings cần phân mảnh
        numberofpartitions: Số phân mảnh cần tạo
        openconnection: Kết nối database
    """
    cursor = openconnection.cursor()
    
    try:
        print(f"Bắt đầu phân mảnh Range với {numberofpartitions} partitions...")
        
        # Xóa các bảng partition cũ nếu có
        for i in range(numberofpartitions):
            table_name = f"range_part{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Tính khoảng chia đều
        min_rating = 0.0
        max_rating = 5.0
        range_size = (max_rating - min_rating) / numberofpartitions
        
        print(f"Khoảng chia: {range_size} cho mỗi partition")
        
        # Tạo các bảng phân mảnh và chèn dữ liệu
        for i in range(numberofpartitions):
            table_name = f"range_part{i}"
            
            # Tạo bảng mới
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    userid INT,
                    movieid INT,
                    rating FLOAT,
                    PRIMARY KEY (userid, movieid)
                )
            """)
            
            # Tính khoảng giá trị cho partition i
            if i == 0:
                # Partition đầu tiên: [min_rating, min_rating + range_size]
                lower_bound = min_rating
                upper_bound = min_rating + range_size
                condition = f"rating >= {lower_bound} AND rating <= {upper_bound}"
            else:
                # Các partition khác: (min_rating + i*range_size, min_rating + (i+1)*range_size]
                lower_bound = min_rating + i * range_size
                upper_bound = min_rating + (i + 1) * range_size if i < numberofpartitions - 1 else max_rating
                condition = f"rating > {lower_bound} AND rating <= {upper_bound}"
            
            # Chèn dữ liệu vào partition
            cursor.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE {condition}
                ON CONFLICT (userid, movieid) DO UPDATE 
                SET rating = EXCLUDED.rating
            """)
            
            # Kiểm tra số lượng record trong partition
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Partition {i} ({lower_bound:.2f}, {upper_bound:.2f}]: {count} records")
        
        openconnection.commit()
        print("Hoàn thành Range Partition!")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi trong quá trình Range Partition: {e}")
        raise
    finally:
        cursor.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    
    Args:
        ratingstablename: Tên bảng ratings cần phân mảnh
        numberofpartitions: Số phân mảnh cần tạo
        openconnection: Kết nối database
    """
    cursor = openconnection.cursor()
    
    try:
        print(f"Bắt đầu phân mảnh Round Robin với {numberofpartitions} partitions...")
        
        # Xóa các bảng partition cũ nếu có
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    userid INT,
                    movieid INT,
                    rating FLOAT,
                    PRIMARY KEY (userid, movieid)
                )
            """)
        
        # Tạo hoặc reset bảng metadata cho Round Robin
        cursor.execute("DROP TABLE IF EXISTS rrobin_metadata")
        cursor.execute("""
            CREATE TABLE rrobin_metadata (
                next_partition INT DEFAULT 0
            )
        """)
        cursor.execute("INSERT INTO rrobin_metadata (next_partition) VALUES (0)")
        
        # Lấy tất cả dữ liệu từ bảng gốc
        cursor.execute(f"SELECT userid, movieid, rating FROM {ratingstablename}")
        
        # Phân phối dữ liệu theo Round Robin
        batch_size = 10000
        row_count = 0
        partition_counts = [0] * numberofpartitions
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            
            # Chuẩn bị batch data cho mỗi partition
            partition_batches = [[] for _ in range(numberofpartitions)]
            
            for userid, movieid, rating in rows:
                partition_idx = row_count % numberofpartitions
                partition_batches[partition_idx].append((userid, movieid, rating))
                partition_counts[partition_idx] += 1
                row_count += 1
            
            # Chèn batch data vào các partition
            for i, batch_data in enumerate(partition_batches):
                if batch_data:
                    table_name = f"rrobin_part{i}"
                    cursor.executemany(f"""
                        INSERT INTO {table_name} (userid, movieid, rating)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (userid, movieid) DO UPDATE 
                        SET rating = EXCLUDED.rating
                    """, batch_data)
            
            print(f"Đã xử lý {row_count} records...")
        
        openconnection.commit()
        
        # In thống kê
        for i, count in enumerate(partition_counts):
            print(f"Partition {i}: {count} records")
        
        print("Hoàn thành Round Robin Partition!")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi trong quá trình Round Robin Partition: {e}")
        raise
    finally:
        cursor.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    
    Args:
        ratingstablename: Tên bảng ratings chính
        userid: ID người dùng
        itemid: ID phim (movieid)
        rating: Điểm đánh giá
        openconnection: Kết nối database
    """
    cursor = openconnection.cursor()
    
    try:
        # Tìm số lượng partition hiện có
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE 'range_part%' AND table_schema = 'public'
            ORDER BY table_name
        """)
        
        partitions = cursor.fetchall()
        N = len(partitions)
        
        if N == 0:
            raise ValueError("Không tìm thấy range partition nào. Hãy chạy rangepartition() trước.")
        
        # Tính toán partition phù hợp
        min_rating = 0.0
        max_rating = 5.0
        range_size = (max_rating - min_rating) / N
        
        # Tìm partition index
        if rating <= min_rating:
            partition_idx = 0
        elif rating > max_rating:
            partition_idx = N - 1
        else:
            # Tính partition dựa trên logic tương tự rangepartition
            if rating <= min_rating + range_size:
                partition_idx = 0
            else:
                partition_idx = min(int((rating - min_rating) / range_size), N - 1)
                # Điều chỉnh cho boundary case
                if rating <= min_rating + partition_idx * range_size:
                    partition_idx = max(0, partition_idx - 1)
        
        # Chèn vào bảng chính
        cursor.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (userid, movieid) DO UPDATE 
            SET rating = EXCLUDED.rating
        """, (userid, itemid, rating))
        
        # Chèn vào partition tương ứng
        table_name = f"range_part{partition_idx}"
        cursor.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (userid, movieid) DO UPDATE 
            SET rating = EXCLUDED.rating
        """, (userid, itemid, rating))
        
        openconnection.commit()
        print(f"Đã chèn record (userid: {userid}, movieid: {itemid}, rating: {rating}) vào {table_name}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi trong quá trình Range Insert: {e}")
        raise
    finally:
        cursor.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    
    Args:
        ratingstablename: Tên bảng ratings chính
        userid: ID người dùng
        itemid: ID phim (movieid)
        rating: Điểm đánh giá
        openconnection: Kết nối database
    """
    cursor = openconnection.cursor()
    
    try:
        # Kiểm tra và tạo bảng metadata nếu chưa có
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rrobin_metadata (
                next_partition INT DEFAULT 0
            )
        """)
        
        # Lấy partition tiếp theo
        cursor.execute("SELECT next_partition FROM rrobin_metadata")
        result = cursor.fetchone()
        
        if result is None:
            cursor.execute("INSERT INTO rrobin_metadata (next_partition) VALUES (0)")
            next_partition = 0
        else:
            next_partition = result[0]
        
        # Tìm số lượng partition
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name LIKE 'rrobin_part%' AND table_schema = 'public'
        """)
        N = cursor.fetchone()[0]
        
        if N == 0:
            raise ValueError("Không tìm thấy round robin partition nào. Hãy chạy roundrobinpartition() trước.")
        
        # Chèn vào bảng chính
        cursor.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (userid, movieid) DO UPDATE 
            SET rating = EXCLUDED.rating
        """, (userid, itemid, rating))
        
        # Chèn vào partition tương ứng
        table_name = f"rrobin_part{next_partition}"
        cursor.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (userid, movieid) DO UPDATE 
            SET rating = EXCLUDED.rating
        """, (userid, itemid, rating))
        
        # Cập nhật partition tiếp theo
        cursor.execute("""
            UPDATE rrobin_metadata 
            SET next_partition = %s
        """, ((next_partition + 1) % N,))
        
        openconnection.commit()
        print(f"Đã chèn record (userid: {userid}, movieid: {itemid}, rating: {rating}) vào {table_name}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi trong quá trình Round Robin Insert: {e}")
        raise
    finally:
        cursor.close() 