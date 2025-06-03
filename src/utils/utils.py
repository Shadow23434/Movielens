from database import get_connection

def get_partition_stats():
    """Utility function để xem thống kê các partition"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== THỐNG KÊ PARTITION ===")
        
        # Range partitions
        print("\nRange Partitions:")
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE 'range_part%' AND table_schema = 'public'
            ORDER BY table_name
        """)
        
        for (table_name,) in cursor.fetchall():
            cursor.execute(f"SELECT COUNT(*), MIN(rating), MAX(rating) FROM {table_name}")
            count, min_rating, max_rating = cursor.fetchone()
            print(f"  {table_name}: {count} records, Rating range: [{min_rating}, {max_rating}]")
        
        # Round Robin partitions  
        print("\nRound Robin Partitions:")
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE 'rrobin_part%' AND table_schema = 'public'
            ORDER BY table_name
        """)
        
        for (table_name,) in cursor.fetchall():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  {table_name}: {count} records")
            
        # Round Robin metadata
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'rrobin_metadata' AND table_schema = 'public'
            )
        """)
        
        if cursor.fetchone()[0]:
            cursor.execute("SELECT next_partition FROM rrobin_metadata")
            next_partition = cursor.fetchone()[0]
            print(f"  Next partition for insert: rrobin_part{next_partition}")
        
    except Exception as e:
        print(f"Lỗi khi lấy thống kê: {e}")
    finally:
        cursor.close()

def verify_data_integrity():
    """Kiểm tra tính toàn vẹn của dữ liệu trong các bảng"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Kiểm tra tổng số records trong bảng chính
        cursor.execute("SELECT COUNT(*) FROM Ratings")
        main_count = cursor.fetchone()[0]
        
        # Kiểm tra tổng số records trong range partitions
        cursor.execute("""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) as cnt FROM information_schema.tables t
                JOIN pg_class c ON c.relname = t.table_name
                WHERE t.table_name LIKE 'range_part%' AND t.table_schema = 'public'
            ) as counts
        """)
        range_count = cursor.fetchone()[0] or 0
        
        # Kiểm tra tổng số records trong round robin partitions
        cursor.execute("""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) as cnt FROM information_schema.tables t
                JOIN pg_class c ON c.relname = t.table_name
                WHERE t.table_name LIKE 'rrobin_part%' AND t.table_schema = 'public'
            ) as counts
        """)
        rrobin_count = cursor.fetchone()[0] or 0
        
        # Kiểm tra tính nhất quán của dữ liệu
        print("\n=== KIỂM TRA TÍNH TOÀN VẸN DỮ LIỆU ===")
        print(f"Tổng số records trong bảng chính: {main_count}")
        print(f"Tổng số records trong range partitions: {range_count}")
        print(f"Tổng số records trong round robin partitions: {rrobin_count}")
        
        # Kiểm tra xem có records nào bị thiếu hoặc thừa không
        if main_count != range_count or main_count != rrobin_count:
            print("CẢNH BÁO: Số lượng records không khớp giữa các bảng!")
            
            # Kiểm tra chi tiết các records không khớp
            cursor.execute("""
                SELECT UserID, MovieID, Rating FROM Ratings
                EXCEPT
                SELECT UserID, MovieID, Rating FROM range_part0
                UNION ALL
                SELECT UserID, MovieID, Rating FROM range_part1
                UNION ALL
                SELECT UserID, MovieID, Rating FROM range_part2
                UNION ALL
                SELECT UserID, MovieID, Rating FROM range_part3
                UNION ALL
                SELECT UserID, MovieID, Rating FROM range_part4
            """)
            missing_in_range = cursor.fetchall()
            if missing_in_range:
                print(f"Records không có trong range partitions: {len(missing_in_range)}")
                
            cursor.execute("""
                SELECT UserID, MovieID, Rating FROM Ratings
                EXCEPT
                SELECT UserID, MovieID, Rating FROM rrobin_part0
                UNION ALL
                SELECT UserID, MovieID, Rating FROM rrobin_part1
                UNION ALL
                SELECT UserID, MovieID, Rating FROM rrobin_part2
                UNION ALL
                SELECT UserID, MovieID, Rating FROM rrobin_part3
                UNION ALL
                SELECT UserID, MovieID, Rating FROM rrobin_part4
            """)
            missing_in_rrobin = cursor.fetchall()
            if missing_in_rrobin:
                print(f"Records không có trong round robin partitions: {len(missing_in_rrobin)}")
        else:
            print("Dữ liệu nhất quán giữa các bảng.")
            
    except Exception as e:
        print(f"Lỗi khi kiểm tra tính toàn vẹn dữ liệu: {e}")
    finally:
        cursor.close() 