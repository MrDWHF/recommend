import pymysql
from config import DB_CONFIG

def clear_mock_data():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 清空所有用户的收藏记录
    cursor.execute("DELETE FROM collections")

    # 清空所有用户的评论点赞记录
    cursor.execute("DELETE FROM comment_likes")

    # 清空所有模拟的用户数据
    cursor.execute("DELETE FROM users")

    conn.commit()  # 提交事务
    cursor.close()
    conn.close()

    print("模拟数据已清空")

if __name__ == "__main__":
    clear_mock_data()  # 执行清空模拟数据
