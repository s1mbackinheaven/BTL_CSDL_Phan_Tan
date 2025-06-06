#!/usr/bin/env python3
"""
BTL Cơ sở dữ liệu phân tán - Phân mảnh dữ liệu
Phiên bản tối ưu cho dataset lớn
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# Xóa BATCH_SIZE vì không cần thiết
# Thêm các hằng số từ assignment_tester.py
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

def getopenconnection(user='postgres', password='1234', dbname='postgres', host='localhost'):
    """
    Tạo kết nối đến PostgreSQL database với các tham số mặc định
    """
    try:
        connection = psycopg2.connect(
            host=host,
            database=dbname,
            user=user,
            password=password
        )
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        return connection
    except Exception as e:
        print(f"Lỗi kết nối database: {e}")
        raise


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Hàm 1: Tải dữ liệu từ file ratings.dat vào bảng Ratings
    Sử dụng COPY command để tối ưu tốc độ với dataset lớn
    """
    cur = openconnection.cursor()
    
    try:
        # Drop table nếu đã tồn tại
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
        
        # Tạo bảng tạm với cấu trúc phù hợp với file input
        cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid INTEGER,
            extra1 CHAR,
            movieid INTEGER,
            extra2 CHAR,
            rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        )
        """)
        
        # Copy data từ file vào bảng
        with open(ratingsfilepath, 'r') as f:
            cur.copy_from(f, ratingstablename, sep=':')
        
        # Drop các cột không cần thiết
        cur.execute(f"""
        ALTER TABLE {ratingstablename}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp
        """)
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi tải dữ liệu: {e}")
        raise
    finally:
        cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm 2: Phân mảnh bảng theo khoảng Rating đồng đều
    """
    cur = openconnection.cursor()
    
    try:
        # Drop tất cả partition cũ trong một transaction
        cur.execute(f"""
        DO $$ 
        DECLARE 
            i INTEGER;
        BEGIN 
            FOR i IN 0..{numberofpartitions-1} LOOP
                EXECUTE 'DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}' || i || ' CASCADE';
            END LOOP;
        END $$;
        """)
        
        range_size = 5.0 / numberofpartitions
        
        # Tạo tất cả partition tables
        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            create_table_query = f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
            """
            cur.execute(create_table_query)

        # Insert data vào các partition
        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            min_rating = i * range_size
            max_rating = (i + 1) * range_size
            
            if i == 0:
                cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating >= {min_rating} AND rating <= {max_rating}
                """)
            else:
                cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating > {min_rating} AND rating <= {max_rating}
                """)
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi tạo Range Partition: {e}")
        raise
    finally:
        cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm 3: Phân mảnh bảng theo Round Robin
    """
    cur = openconnection.cursor()
    
    try:
        # Drop tất cả partition cũ
        cur.execute(f"""
        DO $$ 
        DECLARE 
            i INTEGER;
        BEGIN 
            FOR i IN 0..{numberofpartitions-1} LOOP
                EXECUTE 'DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}' || i || ' CASCADE';
            END LOOP;
        END $$;
        """)
        
        # Tạo tất cả partition tables
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            create_table_query = f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
            """
            cur.execute(create_table_query)
        
        # Sử dụng window function để phân phối dữ liệu
        for i in range(numberofpartitions):
            cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{i} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT *, ROW_NUMBER() OVER() as rnum
                FROM {ratingstablename}
            ) t
            WHERE MOD(rnum - 1, {numberofpartitions}) = {i}
            """)
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi tạo Round Robin Partition: {e}")
        raise
    finally:
        cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm 4: Chèn record mới vào bảng gốc và phân mảnh Range tương ứng
    """
    cur = openconnection.cursor()
    
    try:
        # Insert vào bảng gốc
        cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Xác định số lượng partition
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name LIKE %s
        """, (RANGE_TABLE_PREFIX + '%',))
        numberofpartitions = cur.fetchone()[0]
        
        if numberofpartitions > 0:
            range_size = 5.0 / numberofpartitions
            
            # Logic mới để tính partition_index
            if rating == 0:
                partition_index = 0
            else:
                partition_index = min(int((rating - 0.000001) / range_size), numberofpartitions - 1)
            
            table_name = f"{RANGE_TABLE_PREFIX}{partition_index}"
            cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            """, (userid, itemid, rating))
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi chèn Range Insert: {e}")
        raise
    finally:
        cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm 5: Chèn record mới vào bảng gốc và phân mảnh Round Robin tiếp theo
    """
    cur = openconnection.cursor()
    
    try:
        # Insert vào bảng gốc
        cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Xác định số lượng partition
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name LIKE %s
        """, (RROBIN_TABLE_PREFIX + '%',))
        numberofpartitions = cur.fetchone()[0]
        
        if numberofpartitions > 0:
            # Xác định tổng số records để tính partition index
            cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
            total_records = cur.fetchone()[0]
            
            partition_index = (total_records - 1) % numberofpartitions
            table_name = f"{RROBIN_TABLE_PREFIX}{partition_index}"
            
            cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            """, (userid, itemid, rating))
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi chèn Round Robin Insert: {e}")
        raise
    finally:
        cur.close()


def create_db(dbname):
    """
    Tạo database mới nếu chưa tồn tại
    Tối ưu bằng cách sử dụng prepared statements và connection pooling
    """
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='postgres',
            password='1234'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Sử dụng prepared statement
        cur.execute("PREPARE check_db (text) AS SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1")
        cur.execute("EXECUTE check_db (%s)", (dbname,))
        exists = cur.fetchone()
        
        if not exists:
            # Tạo DB với các tham số tối ưu
            cur.execute(f"""
            CREATE DATABASE {dbname}
            WITH 
                ENCODING = 'UTF8'
                LC_COLLATE = 'en_US.UTF-8'
                LC_CTYPE = 'en_US.UTF-8'
                TEMPLATE = template0
                CONNECTION LIMIT = -1;
            """)
            print(f"Database {dbname} đã được tạo")
        else:
            print(f"Database {dbname} đã tồn tại")
            
    except Exception as e:
        print(f"Lỗi khi tạo database: {e}")
        raise
    finally:
        cur.execute("DEALLOCATE check_db")
        cur.close()
        conn.close()


def count_partitions(prefix, openconnection):
    """
    Đếm số lượng bảng có prefix cho trước
    Tối ưu bằng cách cache kết quả
    """
    cur = openconnection.cursor()
    try:
        cur.execute("""
        SELECT COUNT(*) 
        FROM pg_stat_user_tables 
        WHERE relname LIKE %s
        """, (prefix + '%',))
        count = cur.fetchone()[0]
        return count
    finally:
        cur.close() 