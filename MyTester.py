#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

import psycopg2
import traceback
import testHelper
import solution as MyAssignment

def print_menu():
    print("\n=== MENU TESTING ===")
    print("1. Test Range Partition")
    print("2. Test Round Robin Partition")
    print("3. Test cả hai Partition")
    print("0. Thoát")
    return input("Chọn chức năng (0-3): ")

def test_range_partition(conn):
    print("\n=== TESTING RANGE PARTITION ===")
    # Test rangepartition
    [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
    if result:
        print("rangepartition function pass!")
    else:
        print("rangepartition function fail!")

    # Test rangeinsert
    [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
    if result:
        print("rangeinsert function pass!")
    else:
        print("rangeinsert function fail!")

def test_round_robin_partition(conn):
    print("\n=== TESTING ROUND ROBIN PARTITION ===")
    # Test roundrobinpartition
    [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
    if result:
        print("roundrobinpartition function pass!")
    else:
        print("roundrobinpartition function fail")

    # Test roundrobininsert
    [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
    if result:
        print("roundrobininsert function pass!")
    else:
        print("roundrobininsert function fail!")

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            while True:
                choice = print_menu()
                
                if choice == '0':
                    break
                    
                # Xóa tất cả bảng và load dữ liệu mới
                testHelper.deleteAllPublicTables(conn)
                [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print("loadratings function pass!")
                else:
                    print("loadratings function fail!")
                    continue

                if choice == '1':
                    test_range_partition(conn)
                elif choice == '2':
                    test_round_robin_partition(conn)
                elif choice == '3':
                    test_range_partition(conn)
                    test_round_robin_partition(conn)
                else:
                    print("Lựa chọn không hợp lệ!")

            # Hỏi người dùng có muốn xóa tất cả bảng không
            choice = input('\nPress enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)

    except Exception as detail:
        traceback.print_exc() 