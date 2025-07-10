# from db import get_connection
#
# def get_user_interactions(user_id):
#     conn = get_connection()
#     cursor = conn.cursor()
#
#     interactions = []
#
#     cursor.execute("SELECT book_id FROM collections WHERE user_id = %s", (user_id,))
#     for row in cursor.fetchall():
#         interactions.append({
#             'book_id': row[0],
#             'implicit_rating': 4
#         })
#
#     cursor.execute("SELECT book_id, star_rating FROM comment_likes WHERE user_id = %s", (user_id,))
#     for row in cursor.fetchall():
#         interactions.append({
#             'book_id': row[0],
#             'is_liked': True,
#             'explicit_rating': row[1]
#         })
#
#     conn.close()
#     return interactions




#kaotu数据库

from db import get_connection

def get_user_interactions(user_id):
    conn = get_connection()
    cursor = conn.cursor()

    interactions = []

    # 获取用户收藏的书籍（favorite 表）
    cursor.execute("SELECT book_id FROM favorite WHERE user_id = %s", (user_id,))
    for row in cursor.fetchall():
        print(row,'win1')
        interactions.append({
            'book_id': row[0],
            'favorite': 4  # 假设收藏行为的隐式评分为 4
        })

    # 获取用户点赞的评论数据（comment_like 表）,计算平均分
    # cursor.execute("""
    #                SELECT c.book_id, c.star
    #                FROM comment_like cl
    #                JOIN comment c ON cl.comment_id = c.id
    #                WHERE cl.user_id = %s
    #                """, (user_id,))
    # for row in cursor.fetchall():
    #     print(row,'win2')
    #     interactions.append({
    #         'book_id': row[0],       # 从 comment 表获取 book_id
    #         'is_liked': True,        # 标记用户已点赞
    #         'explicit_rating': row[1] # 显式评分来自 comment 表中的 star 字段
    #     })
    cursor.execute("""
                   SELECT c.book_id, AVG(c.star) AS avg_star
                   FROM comment_like cl
                   JOIN comment c ON cl.comment_id = c.id
                   WHERE cl.user_id = %s
                   GROUP BY c.book_id
                   """, (user_id,))
    for row in cursor.fetchall():
        print(row, 'win2')
        interactions.append({
            'book_id': row[0],  # 从 comment 表获取 book_id
            'is_liked': True,  # 标记用户已点赞
            'implicit_rating': row[1]  # 使用多个评论点赞的平均评分
        })


    # 获取用户的评论数据（comment 表）
    cursor.execute("SELECT book_id, star FROM comment WHERE user_id = %s", (user_id,))
    for row in cursor.fetchall():
        print(row,'win3')
        interactions.append({
            'book_id': row[0],         # 获取用户评论的书籍 ID
            'explicit_rating': row[1]  # 显式评分来自评论的 star 字段
        })

    conn.close()
    print(interactions)
    return interactions

