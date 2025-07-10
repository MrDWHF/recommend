import random
import re

import numpy as np
import pymysql


import requests
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler
from sklearn.neighbors import NearestNeighbors
from opencc import OpenCC
import jieba
from collections import defaultdict
import concurrent.futures
import time


cc = OpenCC('t2s')
jieba.initialize()


class BookRecommender:
    def __init__(self, db_config, use_sparse_sim=True):
        self.db_config = db_config
        self.conn = pymysql.connect(**db_config)
        self.books = self._load_books()
        self.book_ids = [b['id'] for b in self.books]
        self.use_sparse_sim = use_sparse_sim
        self.sim_matrix = self._compute_book_similarity()

    # def _load_books(self):
    #     query = '''
    #             SELECT b.id, \
    #                    b.title, \
    #                    b.publisher, \
    #                    b.comment_count,
    #                    sc.name AS sub_category_name, \
    #                    mc.name AS main_category_name,
    #                    b.book_url, \
    #                    b.img_url
    #             FROM books b
    #                      JOIN sub_categories sc ON b.sub_category_id = sc.id
    #                      JOIN main_categories mc ON sc.main_category_id = mc.id \
    #             '''
    #     with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
    #         cursor.execute(query)
    #         books = cursor.fetchall()
    #
    #     for book in books:
    #         book['categories'] = [book['main_category_name'], book['sub_category_name']]
    #
    #     return books

    #用于考途
    def _load_books(self):
        query = '''
                SELECT b.id,
                       b.title,
                       b.publisher,
                       b.comments, 
                       bc.category_name AS sub_category_name, # 修改为 book_category 中的 category_name 字段
                       pc.name AS main_category_name, # 修改为 parent_category 中的 name 字段
                       b.book_url, b.img_url
                FROM book b
                         JOIN book_category bc ON b.sub_category_id = bc.id # 使用 book_category 表的 id 进行联接
                         JOIN parent_category pc \
                ON bc.parent_category = pc.id # 使用 parent_category 表的 id 进行联接 \
                '''

        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query)
            books = cursor.fetchall()

        for book in books:
            book['categories'] = [book['main_category_name'], book['sub_category_name']]

        return books

    def _normalize_text(self, text):
        if not isinstance(text, str):
            return ''
        text = cc.convert(text)
        text = re.sub(r'[^\w一-鿿\s]', '', text)  # 正则表达式清理文本
        return text.lower().strip()



    def _tokenize_chinese(self, text):
        return ' '.join(jieba.cut(text))



    def _compute_book_similarity(self):
        titles = [self._normalize_text(b['title']) for b in self.books]
        publishers = [self._normalize_text(b['publisher']) for b in self.books]
        categories = [' '.join([self._normalize_text(c) for c in b['categories']]) for b in self.books]

        n = len(titles)
        title_vectorizer = TfidfVectorizer(tokenizer=self._tokenize_chinese, min_df=2)
        title_tfidf = title_vectorizer.fit_transform(titles)

        if self.use_sparse_sim and n > 500:
            nn = NearestNeighbors(n_neighbors=min(100, n - 1), metric='cosine')
            nn.fit(title_tfidf)
            distances, indices = nn.kneighbors(title_tfidf)
            sim_matrix = np.zeros((n, n))
            for i in range(n):
                for d, j in zip(distances[i], indices[i]):
                    if i != j:
                        sim_matrix[i][j] = 1 - d
        else:
            sim_matrix = cosine_similarity(title_tfidf)

        pub_vec = CountVectorizer(binary=True)
        pub_bin = pub_vec.fit_transform(publishers)
        pub_sim = cosine_similarity(pub_bin)

        cat_vec = TfidfVectorizer()
        cat_tfidf = cat_vec.fit_transform(categories)
        cat_sim = cosine_similarity(cat_tfidf)

        combined = 0.6 * sim_matrix + 0.2 * pub_sim + 0.2 * cat_sim
        return MinMaxScaler().fit_transform(combined)



    def compute_user_affinity(self, interactions):
        # 定义各交互行为的权重
        WEIGHTS = {'dwell_time': 0.1, 'favorite': 0.8, 'explicit_rating': 0.4, 'implicit_rating': 0.2}

        affinity = defaultdict(float)  # 用于存储书籍的兴趣度

        # 遍历用户的每一条交互记录
        for inter in interactions:
            book_id = inter['book_id']
            score = 0
            print(inter,'win4')

            # 显式评分：用户对书籍进行了评论，并且给出了评分
            if inter.get('explicit_rating'):
                # if inter['emplicit_rating']<=2.5:
                #     score = max(0,score-WEIGHTS['explicit_rating'] * (inter['explicit_rating']))  # 显式评分，范围 1-5
                # else:
                #     score+=WEIGHTS['emplicit_rating'] * (inter['explicit_rating'])

                #score=max(0,score+WEIGHTS['explicit_rating'] * float((inter['explicit_rating'])-2)))
                score = max(0, score + WEIGHTS['explicit_rating'] * (float(inter['explicit_rating']) - 2.5))

                # 隐式评分：用户没有评论，但点赞了某些评论，取点赞评论的平均评分
            if inter.get('is_liked', False):
                # 如果用户点赞了评论，就计算点赞的所有评论的评分平均值
                # if inter['implicit_rating']<=2.5:
                #     score = max(0,score-WEIGHTS['implicit_rating'] * (inter['implicit_rating']))  # 假设所有点赞的评论评分平均
                # else:
                #     score+=WEIGHTS['implicit_rating'] * (inter['implicit_rating'])
                #score = max(0, score + WEIGHTS['implicit_rating'] * (inter['implicit_rating']-2))
                score = max(0, score + WEIGHTS['implicit_rating'] * (float(inter['implicit_rating']) - 2.5))

            if inter.get('favorite'):
                score += WEIGHTS['favorite'] * inter['favorite']

            # 假设有停留时间，表示用户对该书的关注度
            if inter.get('dwell_time', 0) > 0:
                dwell = np.log1p(inter['dwell_time']) / np.log1p(600)  # 使用对数缩放来减小大值的影响
                score += WEIGHTS['dwell_time'] * dwell


            affinity[book_id] += score  # 累加兴趣度

        return affinity



    def recommend(self, affinity, n=12, diversity_factor=0.3):
        random.seed()  # 每次调用时使用不同的种子，增加随机性
        seen = set(affinity.keys())
        if not seen:
            return self._cold_start_recommend(n)
        scores = defaultdict(float)
        for i, bid in enumerate(self.book_ids):
            if bid in seen:
                continue
            for seen_id in seen:
                j = self.book_ids.index(seen_id)
                sim = self.sim_matrix[i][j]
                scores[bid] += sim * affinity[seen_id]
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return self._diversify(sorted_scores, n, diversity_factor)


    def _cold_start_recommend(self, n):
        """冷启动策略：当没有交互数据时，随机推荐书籍"""
        random_books = random.sample(self.books, len(self.books))  # 随机打乱书籍
        top_books = random_books[:n]  # 选择前 12 本
        return [book['id'] for book in top_books]



    # def _diversify(self, sorted_scores, n, factor):
    #     selected = []
    #     remain = [item[0] for item in sorted_scores]
    #     info = {b['id']: b for b in self.books}
    #
    #     while len(selected) < n and remain:
    #         # 随机选择一个书籍
    #         c = random.choice(remain)
    #         remain.remove(c)
    #
    #         # 多样性控制：避免重复推荐相似的书籍
    #         if selected:
    #             last = info[selected[-1]]
    #             cur = info[c]
    #             if last['publisher'] == cur['publisher'] and any(
    #                     cat in last['categories'] for cat in cur['categories']):
    #                 if random.random() < factor:  # 控制多样性因子
    #                     remain.append(c)
    #                     continue
    #
    #         selected.append(c)
    #
    #     return selected

    # def _diversify(self, sorted_scores, n, factor):
    #     selected = []
    #     remain = [item[0] for item in sorted_scores]  # 先按分数排序
    #     info = {b['id']: b for b in self.books}
    #
    #     # 首先选择高评分的书籍
    #     while len(selected) < n and remain:
    #         c = remain.pop(0)  # 从分数高的开始选
    #         selected.append(c)
    #
    #     # 多样性控制：避免重复推荐相似的书籍
    #     diversified_selected = []
    #     for book_id in selected:
    #         cur = info[book_id]
    #         # 如果 selected 中有书籍，则检查多样性
    #         if diversified_selected:
    #             last = info[diversified_selected[-1]]
    #             if last['publisher'] == cur['publisher'] and any(
    #                     cat in last['categories'] for cat in cur['categories']):
    #                 # 如果相似度过高，随机选择下一个
    #                 if random.random() < factor:
    #                     continue
    #         diversified_selected.append(book_id)
    #
    #     return diversified_selected

    def _diversify(self, sorted_scores, n, factor):
        selected = []
        remain_high = [item[0] for item in sorted_scores[:8]]  # 前20本书（真正相关的高分书籍）
        remain_low = [item[0] for item in sorted_scores[1000:]]  # 剩下的30本书

        # 打乱这两组书籍
        random.shuffle(remain_high)
        random.shuffle(remain_low)

        info = {b['id']: b for b in self.books}

        # 首先从高相关度的前20本书中选择
        while len(selected) < n and remain_high:
            c = remain_high.pop(0)  # 从高相关度书籍中选择
            selected.append(c)

        # 如果还没有选择足够的书籍，则从剩余的30本书中选择
        while len(selected) < n and remain_low:
            c = remain_low.pop(0)  # 从剩余的书籍中选择
            selected.append(c)

        # 如果数量还不足n，继续从low的部分补充
        while len(selected) < n and remain_low:
            c = remain_low.pop(0)
            selected.append(c)

        return selected

    def generate_reason_with_deepseek(self, user_tags, book_title):
        """
        优化版 DeepSeek API 调用 - 使用连接池和异步请求
        """
        # 使用连接池保持HTTP连接复用
        if not hasattr(self, 'session'):
            self.session = requests.Session()

        tag_text = "、".join(user_tags)

        # 精简提示词，减少token数量
        prompt = f"你是一个推销，用户对{tag_text}类内容感兴趣。而你为他推荐了《{book_title}》：请你给出推荐理由，要有理有据"

        payload = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "system",
                    "content": "生成推荐理由，不重复书名，不用套话，25个字以内"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 1.3,
            "max_tokens": 60,  # 限制生成长度
            "stream": False
        }

        headers = {
            "Authorization": "Bearer 你的api key",
            "Content-Type": "application/json"
        }

        try:
            start_time = time.time()
            response = self.session.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=5  # 设置超时时间
            )

            # 检查响应状态
            if response.status_code != 200:
                return f"本书与{tag_text}相关，值得一读"

            result = response.json()
            reason = result['choices'][0]['message']['content'].strip()

            # 清理API返回的额外内容
            if "：" in reason:
                reason = reason.split("：", 1)[-1]
            reason = reason.replace("《" + book_title + "》", "").strip()

            # 记录耗时
            print(f"生成推荐理由耗时: {time.time() - start_time:.2f}s | {book_title}")
            return reason[:40]  # 确保不超过40字

        except Exception as e:
            print(f"DeepSeek API调用失败: {str(e)[:50]}")
            return f"本书在{tag_text}领域广受好评"


    def get_book_details(self, book_ids, reason_map=None):
        books = []
        details_to_fetch = []

        # 第一阶段：收集需要获取的书籍信息
        for book in self.books:
            if book['id'] in book_ids:
                details = {
                    "id": book["id"],
                    "title": book["title"],
                    "publisher": book.get("publisher"),
                    "comment_count": book.get("comment_count", 0),
                    "categories": book.get("categories", []),
                    "book_url": book.get("book_url"),
                    "img_url": book.get("img_url"),
                    #"intro": book.get('description', '暂无简介')[:300],
                    "tags": self.extract_interest_tags(book.get('categories', []))
                }
                details_to_fetch.append(details)
                books.append(details)

        # 第二阶段：使用线程池并行生成推荐理由
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:  # 控制并发数
            future_to_book = {
                executor.submit(
                    self.generate_reason_with_deepseek,
                    book['tags'],
                    book['title'],
                    #book['intro']
                ): book for book in details_to_fetch
            }

            for future in concurrent.futures.as_completed(future_to_book):
                book = future_to_book[future]
                try:
                    reason = future.result()
                    book["recommend_reason"] = reason
                except Exception as e:
                    print(f"生成推荐理由出错: {e}")
                    book["recommend_reason"] = f"本书与{book['tags'][0]}相关，值得阅读"

        return books

    def extract_interest_tags(self, categories):
        kaoyan_tags = ['考研', '考研数学', '考研英语', '考研政治', '考研心得', '考研大纲']
        kaogong_tags = [
            '公务员考试', '国家公务员考试', '重庆市公务员考试', '四川省公务员考试', '贵州省公务员考试',
            '广东省公务员考试', '河南省公务员考试', '上海市公务员考试', '安徽省公务员考试',
            '内蒙古公务员考试', '河北省公务员考试', '北京市公务员考试', '江苏省公务员考试',
            '青海省公务员考试', '吉林省公务员考试', '山东省公务员考试', '广西省公务员考试',
            '福建省公务员考试', '湖南省公务员考试', '湖北省公务员考试', '辽宁省公务员考试',
            '陕西省公务员考试', '山西省公务员考试', '云南省公务员考试', '甘肃省公务员考试',
            '新疆公务员考试'
        ]

        matched_tags = []

        for c in categories:
            if c in kaoyan_tags or any(kt in c for kt in kaoyan_tags):
                matched_tags.append(c)
            elif c in kaogong_tags or any(kg in c for kg in kaogong_tags):
                matched_tags.append(c)

        return list(set(matched_tags)) if matched_tags else ["通用考试类"]

    def close(self):
        if self.conn:
            self.conn.close()

