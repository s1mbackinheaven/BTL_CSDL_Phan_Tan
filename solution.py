#!/usr/bin/env python3
"""
BTL Cơ sở dữ liệu phân tán - Phân mảnh dữ liệu
Nhóm: [Tên nhóm]
Thành viên: [Tên thành viên]
"""

import psycopg2
import os


def create_database_if_not_exists():
    """
    Tạo database dds_assgn1 nếu chưa có
    """
    try:
        # Kết nối đến database mặc định (postgres)
        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='postgres',
            password='1234'
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Kiểm tra database đã tồn tại chưa
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'dds_assgn1'")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute("CREATE DATABASE dds_assgn1")
            print("✅ Đã tạo database dds_assgn1")
        else:
            print("ℹ️ Database dds_assgn1 đã tồn tại")
            
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Lỗi khi tạo database: {e}")
        raise


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1', host='localhost'):
    """
    Tạo kết nối đến PostgreSQL database
    """
    try:
        # Tạo database trước nếu chưa có
        if dbname == 'dds_assgn1':
            create_database_if_not_exists()
            
        connection = psycopg2.connect(
            host=host,
            database=dbname,
            user=user,
            password=password
        )
        print(f"✅ Kết nối thành công đến {dbname}")
        return connection
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        raise


def loadratings(ratingsfilepath, openconnection):
    """
    Hàm 1: Tải dữ liệu từ file ratings.dat vào bảng Ratings
    
    Args:
        ratingsfilepath (str): Đường dẫn tuyệt đối đến file ratings.dat
        openconnection: Kết nối PostgreSQL
    """
    cur = openconnection.cursor()
    
    try:
        # 1. Xóa bảng nếu đã tồn tại
        cur.execute("DROP TABLE IF EXISTS ratings CASCADE;")
        
        # 2. Tạo bảng ratings với schema yêu cầu (tên cột chữ thường)
        create_table_query = """
        CREATE TABLE ratings (
            userid INTEGER,
            movieid INTEGER, 
            rating FLOAT
        );
        """
        cur.execute(create_table_query)
        
        # 3. Đọc file và insert dữ liệu
        with open(ratingsfilepath, 'r', encoding='utf-8') as file:
            for line in file:
                # Parse dòng: UserID::MovieID::Rating::Timestamp
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    user_id = int(parts[0])
                    movie_id = int(parts[1]) 
                    rating_val = float(parts[2])
                    # Bỏ timestamp (parts[3])
                    
                    # Insert vào bảng
                    insert_query = """
                    INSERT INTO ratings (userid, movieid, rating) 
                    VALUES (%s, %s, %s);
                    """
                    cur.execute(insert_query, (user_id, movie_id, rating_val))
        
        # 4. Commit transaction
        openconnection.commit()
        print(f"✅ Đã tải dữ liệu thành công từ {ratingsfilepath}")
        
        # 5. Kiểm tra số lượng records
        cur.execute("SELECT COUNT(*) FROM ratings;")
        count = cur.fetchone()[0]
        print(f"📊 Tổng số records: {count}")
        
    except Exception as e:
        openconnection.rollback()
        print(f"❌ Lỗi khi tải dữ liệu: {e}")
        raise
    finally:
        cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm 2: Phân mảnh bảng theo khoảng Rating đồng đều
    
    Args:
        ratingstablename (str): Tên bảng gốc (ratings)
        numberofpartitions (int): Số phân mảnh cần tạo
        openconnection: Kết nối PostgreSQL
    """
    cur = openconnection.cursor()
    
    try:
        # 1. Xóa các bảng phân mảnh cũ nếu có
        for i in range(20):  # Xóa tối đa 20 partitions cũ
            try:
                cur.execute(f"DROP TABLE IF EXISTS range_part{i} CASCADE;")
            except:
                pass
        
        # 2. Tính toán khoảng phân chia đồng đều
        # Rating từ 0 -> 5, chia thành numberofpartitions phần
        range_size = 5.0 / numberofpartitions
        
        # 3. Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            table_name = f"range_part{i}"
            
            # Tạo bảng con với tên cột chữ thường
            create_table_query = f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
            """
            cur.execute(create_table_query)
            
            # Tính khoảng giá trị cho partition này
            min_rating = i * range_size
            max_rating = (i + 1) * range_size
            
            # Insert dữ liệu vào partition
            if i == 0:
                # Partition đầu tiên: [min_rating, max_rating]
                insert_query = f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating >= %s AND rating <= %s;
                """
                cur.execute(insert_query, (min_rating, max_rating))
            else:
                # Các partition khác: (min_rating, max_rating]
                insert_query = f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating > %s AND rating <= %s;
                """
                cur.execute(insert_query, (min_rating, max_rating))
            
            # Kiểm tra số lượng records
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cur.fetchone()[0]
            print(f"📊 {table_name}: [{min_rating:.2f}, {max_rating:.2f}] - {count} records")
        
        openconnection.commit()
        print(f"✅ Đã tạo {numberofpartitions} phân mảnh Range thành công!")
        
    except Exception as e:
        openconnection.rollback()
        print(f"❌ Lỗi khi tạo Range Partition: {e}")
        raise
    finally:
        cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm 3: Phân mảnh bảng theo Round Robin
    
    Args:
        ratingstablename (str): Tên bảng gốc (ratings)
        numberofpartitions (int): Số phân mảnh cần tạo
        openconnection: Kết nối PostgreSQL
    """
    cur = openconnection.cursor()
    
    try:
        # 1. Xóa các bảng phân mảnh cũ nếu có
        for i in range(20):  # Xóa tối đa 20 partitions cũ
            try:
                cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i} CASCADE;")
            except:
                pass
        
        # 2. Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            
            create_table_query = f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
            """
            cur.execute(create_table_query)
        
        # 3. Phân phối dữ liệu theo Round Robin
        # Sử dụng ROW_NUMBER() để đánh số thứ tự records
        cur.execute(f"""
            SELECT userid, movieid, rating, 
                   ROW_NUMBER() OVER() as row_num
            FROM {ratingstablename}
            ORDER BY userid, movieid;
        """)
        
        records = cur.fetchall()
        
        # 4. Phân phối từng record vào partition tương ứng
        for idx, (user_id, movie_id, rating_val, row_num) in enumerate(records):
            partition_index = idx % numberofpartitions
            table_name = f"rrobin_part{partition_index}"
            
            insert_query = f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
            """
            cur.execute(insert_query, (user_id, movie_id, rating_val))
        
        # 5. Kiểm tra kết quả
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cur.fetchone()[0]
            print(f"📊 {table_name}: {count} records")
        
        openconnection.commit()
        print(f"✅ Đã tạo {numberofpartitions} phân mảnh Round Robin thành công!")
        
    except Exception as e:
        openconnection.rollback()
        print(f"❌ Lỗi khi tạo Round Robin Partition: {e}")
        raise
    finally:
        cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm 4: Chèn record mới vào bảng gốc và phân mảnh Range tương ứng
    
    Args:
        ratingstablename (str): Tên bảng gốc (ratings)
        userid (int): User ID
        itemid (int): Movie ID  
        rating (float): Rating value
        openconnection: Kết nối PostgreSQL
    """
    cur = openconnection.cursor()
    
    try:
        # 1. Insert vào bảng gốc
        insert_main_query = f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s);
        """
        cur.execute(insert_main_query, (userid, itemid, rating))
        
        # 2. Tìm số lượng partitions hiện có
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name LIKE 'range_part%';
        """)
        numberofpartitions = cur.fetchone()[0]
        
        if numberofpartitions > 0:
            # 3. Tính toán partition đích dựa trên rating
            range_size = 5.0 / numberofpartitions
            partition_index = int(rating / range_size)
            
            # Xử lý edge case: rating = 5.0
            if partition_index >= numberofpartitions:
                partition_index = numberofpartitions - 1
            
            # 4. Insert vào partition tương ứng
            table_name = f"range_part{partition_index}"
            insert_partition_query = f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
            """
            cur.execute(insert_partition_query, (userid, itemid, rating))
            
            print(f"✅ Đã chèn vào {ratingstablename} và {table_name}")
        else:
            print(f"⚠️ Không tìm thấy Range partitions, chỉ chèn vào {ratingstablename}")
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"❌ Lỗi khi chèn Range Insert: {e}")
        raise
    finally:
        cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm 5: Chèn record mới vào bảng gốc và phân mảnh Round Robin tiếp theo
    
    Args:
        ratingstablename (str): Tên bảng gốc (ratings)
        userid (int): User ID
        itemid (int): Movie ID
        rating (float): Rating value  
        openconnection: Kết nối PostgreSQL
    """
    cur = openconnection.cursor()
    
    try:
        # 1. Insert vào bảng gốc
        insert_main_query = f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s);
        """
        cur.execute(insert_main_query, (userid, itemid, rating))
        
        # 2. Tìm số lượng partitions hiện có
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name LIKE 'rrobin_part%';
        """)
        numberofpartitions = cur.fetchone()[0]
        
        if numberofpartitions > 0:
            # 3. Tính tổng số records hiện tại trong bảng gốc
            cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
            total_records = cur.fetchone()[0]
            
            # 4. Tính partition đích theo Round Robin
            # Record mới sẽ là record thứ total_records (0-indexed: total_records-1)
            partition_index = (total_records - 1) % numberofpartitions
            
            # 5. Insert vào partition tương ứng
            table_name = f"rrobin_part{partition_index}"
            insert_partition_query = f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
            """
            cur.execute(insert_partition_query, (userid, itemid, rating))
            
            print(f"✅ Đã chèn vào {ratingstablename} và {table_name}")
        else:
            print(f"⚠️ Không tìm thấy Round Robin partitions, chỉ chèn vào {ratingstablename}")
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"❌ Lỗi khi chèn Round Robin Insert: {e}")
        raise
    finally:
        cur.close()


# ===============================
# TESTING & DEMO FUNCTIONS
# ===============================

def test_basic_functions():
    """
    Test các hàm cơ bản với dữ liệu mẫu
    """
    print("🧪 TESTING CÁC HÀM CƠ BẢN")
    print("=" * 50)
    
    # Kết nối database
    conn = getopenconnection()
    
    try:
        # Test 1: loadratings
        print("\n📝 Test 1: loadratings")
        loadratings("test_data.dat", conn)
        
        # Test 2: rangepartition
        print("\n📝 Test 2: rangepartition (N=3)")
        rangepartition("ratings", 3, conn)
        
        # Test 3: roundrobinpartition  
        print("\n📝 Test 3: roundrobinpartition (N=3)")
        roundrobinpartition("ratings", 3, conn)
        
        # Test 4: rangeinsert
        print("\n📝 Test 4: rangeinsert")
        rangeinsert("ratings", 999, 888, 4.5, conn)
        
        # Test 5: roundrobininsert
        print("\n📝 Test 5: roundrobininsert")  
        roundrobininsert("ratings", 999, 777, 3.0, conn)
        
        print("\n✅ TẤT CẢ TEST ĐỀU THÀNH CÔNG!")
        
    except Exception as e:
        print(f"\n❌ LỖI TRONG QUÁ TRÌNH TEST: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    print("🚀 BTL Cơ sở dữ liệu phân tán - Phân mảnh dữ liệu")
    print("=" * 60)
    
    # Chạy test
    test_basic_functions() 