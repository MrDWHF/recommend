import requests
from bs4 import BeautifulSoup
import re
import time
import random
import jieba
import pymysql
from pymysql import Error

# 定义需要爬取的分类字典
dic = {
    '考研': {
        '考研数学': '.93.04.00.00.html',
        '考研英文': '.93.02.00.00.html',
        '考研政治': ".93.03.00.00.html",
        '考研心得': '.93.14.00.00.html',
        '考研大纲': '.93.01.00.00.html'
    },
    '公务员考试': {
        '国家公务员考试': '.11.01.00.00.html',
        '重庆市公务员考试': '.11.32.00.00.html',
        '四川省公务员考试': '.11.28.00.00.html',
        '贵州省公务员考试': '.11.10.00.00.html',
        '广东省公务员考试': '.11.07.00.00.html',
        '河南省公务员考试': '.11.13.00.00.html',
        '上海市公务员考试': '.11.26.00.00.html',
        '安徽省公务员考试': '.11.05.00.00.html',
        '内蒙古公务员考试': '.11.20.00.00.html',
        '河北省公务员考试': '.11.12.00.00.html',
        '北京市公务员考试': '.11.02.00.00.html',
        '江苏省公务员考试': '.11.04.00.00.html',
        '青海省公务员考试': '.11.22.00.00.html',
        '吉林省公务员考试': '.11.17.00.00.html',
        '山东省公务员考试': '.11.23.00.00.html',
        '广西省公务员考试': '.11.08.00.00.html',
        '福建省公务员考试': '.11.06.00.00.html',
        '湖南省公务员考试': '.11.16.00.00.html',
        '湖北省公务员考试': '.11.15.00.00.html',
        '辽宁省公务员考试': '.11.19.00.00.html',
        '陕西省公务员考试': '.11.25.00.00.html',
        '山西省公务员考试': '.11.24.00.00.html',
        '云南省公务员考试': '11.31.00.00.html',
        '甘肃省公务员考试': '.11.09.00.00.html',
        '新疆公务员考试': '.11.30.00.00.html',
    }
}

# 基础URL
base_url = "https://category.dangdang.com/cp01.47"

# 设置请求头，模拟浏览器访问
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive'
}

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'finalbooks',
    'charset': 'utf8mb4'
}


def create_database_connection():
    """创建数据库连接"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"数据库连接失败: {e}")
        return None


def create_tables(connection):
    """创建必要的数据库表"""
    try:
        with connection.cursor() as cursor:
            # 创建主分类表
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS main_categories
                           (
                               id
                               INT
                               AUTO_INCREMENT
                               PRIMARY
                               KEY,
                               name
                               VARCHAR
                           (
                               50
                           ) NOT NULL UNIQUE
                               )
                           """)
            # 创建子分类表
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS sub_categories
                           (
                               id
                               INT
                               AUTO_INCREMENT
                               PRIMARY
                               KEY,
                               main_category_id
                               INT
                               NOT
                               NULL,
                               name
                               VARCHAR
                           (
                               50
                           ) NOT NULL,
                               FOREIGN KEY
                           (
                               main_category_id
                           ) REFERENCES main_categories
                           (
                               id
                           ),
                               UNIQUE
                           (
                               main_category_id,
                               name
                           )
                               )
                           """)
            # 创建书籍表
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS books
                           (
                               id
                               INT
                               AUTO_INCREMENT
                               PRIMARY
                               KEY,
                               title
                               VARCHAR
                           (
                               255
                           ) NOT NULL,
                               publisher VARCHAR
                           (
                               100
                           ),
                               comment_count INT DEFAULT 0,
                               book_url VARCHAR
                           (
                               255
                           ),
                               img_url VARCHAR
                           (
                               255
                           ),
                               sub_category_id INT NOT NULL,
                               FOREIGN KEY
                           (
                               sub_category_id
                           ) REFERENCES sub_categories
                           (
                               id
                           )
                               )
                           """)
            connection.commit()
            print("数据库表创建成功")
    except Error as e:
        print(f"创建表失败: {e}")
        connection.rollback()


def save_category_to_db(connection, main_category, sub_categories):
    """将分类信息保存到数据库"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("INSERT IGNORE INTO main_categories (name) VALUES (%s)", (main_category,))
            cursor.execute("SELECT id FROM main_categories WHERE name = %s", (main_category,))
            main_category_id = cursor.fetchone()[0]
            for sub_category in sub_categories:
                cursor.execute("INSERT IGNORE INTO sub_categories (main_category_id, name) VALUES (%s, %s)",
                               (main_category_id, sub_category))
            connection.commit()
            print(f"分类 '{main_category}' 保存成功")
    except Error as e:
        print(f"保存分类失败: {e}")
        connection.rollback()


def save_books_to_db(connection, books, main_category, sub_category):
    """将书籍信息保存到数据库"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM sub_categories WHERE name = %s", (sub_category,))
            sub_category_id = cursor.fetchone()[0]
            for book in books:
                cursor.execute("""
                               INSERT INTO books (title, publisher, comment_count, book_url, img_url, sub_category_id)
                               VALUES (%s, %s, %s, %s, %s, %s)
                               """, (book['title'], book['publisher'], int(book['comment_count']), book['book_url'],
                                     book['img_url'], sub_category_id))
            connection.commit()
            print(f"已保存 {len(books)} 本书到 '{sub_category}' 子分类")
    except Error as e:
        print(f"保存书籍失败: {e}")
        connection.rollback()


def normalize_url(url):
    """规范化URL，处理协议相对URL"""
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return "https://product.dangdang.com" + url
    return url


def is_relevant(title, category):
    """关键词匹配判断书籍与分类的相关性"""
    category_keywords = jieba.lcut(category)
    title_keywords = jieba.lcut(title)
    for keyword in category_keywords:
        if keyword in title_keywords:
            return True
    return False


def get_page_books(url, category, max_books=40):
    """从指定URL获取书籍信息（最多max_books本）"""
    books = []
    page = 1
    total_collected = 0
    print(f"开始爬取: {url}")

    while total_collected < max_books:
        page_url = url if page == 1 else url.replace('.html', f'-pg{page}.html')
        try:
            response = requests.get(page_url, headers=headers)
            if response.status_code != 200:
                print(f"请求失败，状态码: {response.status_code}")
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            book_list = soup.select('ul.bigimg > li')
            if not book_list:
                print(f"第 {page} 页没有找到书籍")
                break
            print(f"第 {page} 页找到 {len(book_list)} 本书")
            for item in book_list:
                if total_collected >= max_books:
                    break
                title_elem = item.select_one('p.name a')
                title = title_elem[
                    'title'] if title_elem and 'title' in title_elem.attrs else title_elem.get_text().strip() if title_elem else "无书名信息"

                # 只有相关书籍才保存
                if not is_relevant(title, category):
                    print(f"跳过不相关书籍: {title}")
                    continue

                book_url = normalize_url(title_elem['href'] if title_elem else "")
                pub_elem = item.select_one('p.search_book_author span:nth-of-type(3) a') or item.select_one(
                    'p.search_book_author span:nth-of-type(2) a')
                publisher = pub_elem.get_text().strip() if pub_elem else "无出版社信息"
                comment_elem = item.select_one('p.search_star_line a')
                comment_text = comment_elem.get_text() if comment_elem else ""
                match = re.search(r'(\d+)', comment_text)
                comment_count = match.group(1) if match else "0"
                img_elem = item.select_one('a.pic img')
                img_url = normalize_url(img_elem['data-original'] if 'data-original' in img_elem.attrs else img_elem[
                    'src'] if img_elem else "")
                books.append(
                    {"title": title, "publisher": publisher, "comment_count": comment_count, "book_url": book_url,
                     "img_url": img_url})
                total_collected += 1
            print(f"已收集 {total_collected}/{max_books} 本书")
            if total_collected >= max_books:
                break
            time.sleep(random.uniform(1.5, 3.0))
            page += 1
        except Exception as e:
            print(f"爬取过程中出错: {e}")
            break
    return books


def scrape_all_categories(connection):
    """爬取所有分类的书籍信息并保存到数据库"""
    for main_category, sub_categories in dic.items():
        save_category_to_db(connection, main_category, sub_categories.keys())
        print(f"\n{'=' * 30}")
        print(f"开始爬取主分类: {main_category}")
        print(f"{'=' * 30}")
        for sub_category, url_suffix in sub_categories.items():
            full_url = base_url + url_suffix
            books = get_page_books(full_url, sub_category, max_books=40)  # 传递sub_category作为分类
            if books:
                save_books_to_db(connection, books, main_category, sub_category)
            time.sleep(random.uniform(2, 4))


# 主程序
if __name__ == "__main__":
    print("当当网图书信息爬取程序")
    print("=" * 50)
    conn = create_database_connection()
    if conn is None:
        print("无法连接数据库，程序退出")
        exit(1)
    create_tables(conn)
    start_time = time.time()
    scrape_all_categories(conn)
    elapsed_time = time.time() - start_time
    conn.close()
    print("\n爬取完成！")
    print(f"耗时: {elapsed_time:.2f} 秒")





























