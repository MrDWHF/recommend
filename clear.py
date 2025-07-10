from pymysql import Error
from get_books_to_dbpy import create_database_connection


def clear_database():
    conn = create_database_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM books")
                cursor.execute("DELETE FROM sub_categories")
                cursor.execute("DELETE FROM main_categories")
                cursor.execute("ALTER TABLE books AUTO_INCREMENT = 1")
                cursor.execute("ALTER TABLE sub_categories AUTO_INCREMENT = 1")
                cursor.execute("ALTER TABLE main_categories AUTO_INCREMENT = 1")
                conn.commit()
                print("数据库数据已清空")
        except Error as e:
            print(f"清空数据库失败: {e}")
        finally:
            conn.close()

# 在主程序前调用
clear_database()