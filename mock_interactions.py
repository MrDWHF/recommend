import pymysql
import random
from config import DB_CONFIG


def mock_interactions(n_users=3, likes_per_user=5, collects_per_user=5):
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 插入模拟用户数据
    print("开始插入模拟用户数据")
    user_ids = []
    for user_id in range(1, n_users + 1):
        cursor.execute("""
                       INSERT INTO users (name)
                       VALUES (%s)
                       """, (f"User{user_id}",))  # 假设用户名称为 User1, User2, ...
        user_ids.append(user_id)

    # 确保 users 表中有用户数据
    cursor.execute("SELECT id FROM users")
    fetched_user_ids = [row[0] for row in cursor.fetchall()]
    if not fetched_user_ids:
        print("未能插入模拟用户数据，程序退出")
        return

    print(f"插入的用户 IDs: {fetched_user_ids}")

    # 获取所有书籍ID
    cursor.execute("SELECT id FROM books")
    book_ids = [row[0] for row in cursor.fetchall()]
    if len(book_ids) == 0:
        print("书籍数据为空，无法模拟交互")
        return

    print(f"开始为 {n_users} 个用户生成交互数据")

    # 生成用户的收藏记录和点赞评论数据
    for user_id in fetched_user_ids:
        # 生成用户的收藏记录
        collected = random.sample(book_ids, collects_per_user)
        for book_id in collected:
            # 检查是否已经收藏
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM collections
                           WHERE user_id = %s
                             AND book_id = %s
                           """, (user_id, book_id))
            result = cursor.fetchone()
            if result[0] > 0:
                print(f"跳过已收藏的书籍：用户 {user_id} 已经收藏了书籍 {book_id}")
                continue  # 跳过已经收藏的书籍

            cursor.execute("""
                           INSERT INTO collections (user_id, book_id)
                           VALUES (%s, %s)
                           """, (user_id, book_id))

        # 生成用户的点赞评论和评分记录
        liked = random.sample(book_ids, likes_per_user)
        for book_id in liked:
            rating = random.randint(3, 5)  # 随机生成评分，假设是1到5星
            comment_user = random.choice(fetched_user_ids)  # 假设评论者也是用户之一

            # 检查是否已有相同的 comment_user_id, user_id 和 book_id
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM comment_likes
                           WHERE user_id = %s
                             AND book_id = %s
                             AND comment_user_id = %s
                           """, (user_id, book_id, comment_user))
            result = cursor.fetchone()
            if result[0] > 0:
                print(f"跳过重复评论：用户 {user_id} 已经评论了书籍 {book_id}")
                continue  # 跳过已经评论过的记录

            cursor.execute("""
                           INSERT INTO comment_likes (user_id, book_id, comment_user_id, star_rating)
                           VALUES (%s, %s, %s, %s)
                           """, (user_id, book_id, comment_user, rating))

    conn.commit()  # 提交事务
    cursor.close()
    conn.close()

    print("模拟交互数据已生成并写入数据库")


if __name__ == "__main__":
    mock_interactions()  # 执行模拟交互数据生成


