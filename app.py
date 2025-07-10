from flask import Flask, jsonify, request
import json
from book_recommender import BookRecommender
from interactions import get_user_interactions
from config import DB_CONFIG

# 初始化 Flask 应用实例
app = Flask(__name__)

# 初始化推荐器
recommender = BookRecommender(DB_CONFIG)

@app.route('/recommend', methods=['GET'])
def recommend_for_user():
    user_id = request.args.get('user_id', type=str)
    if user_id is None:
        return jsonify({'error': 'Missing user_id'}), 400

    # 获取用户的历史交互数据（真实数据）
    user_interactions = get_user_interactions(user_id)
    #print(user_interactions)

    # 计算用户的兴趣度（基于用户的交互）
    user_affinity = recommender.compute_user_affinity(user_interactions)
    if not user_affinity:
        print("没有找到用户交互数据")

    print(user_affinity)
    for i in user_affinity.keys():
        print(i,end=" ")
    # 推荐 12 本书
    recommended_ids = recommender.recommend(user_affinity, n=12)

    # 获取推荐书籍的详细信息
    books = recommender.get_book_details(recommended_ids, reason_map=user_affinity)

    return app.response_class(
        response=json.dumps({
            "user_id": user_id,
            "recommendations": books
        }, ensure_ascii=False),
        mimetype='application/json'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)  # 让 Flask 启动时监听所有 IP 地址


