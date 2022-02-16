import pymysql
import requests
import json
import csv
import time


#local database 연결(mysql)

def get_connection():
    conn = pymysql.connect(host='127.0.0.1', port = 3306, user='root', password='1234', db = 'project3', charset='utf8')
    cur = conn.cursor()

    return conn, cur

#make table

def make_table(conn, cur):
    cur.execute("DROP TABLE IF EXISTS new_books;")
    cur.execute("DROP TABLE IF EXISTS best_sellers;")
    cur.execute("DROP TABLE IF EXISTS category;")

    cur.execute("""CREATE TABLE IF NOT EXISTS new_books 
                    (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    title CHAR(120),
                    categoryId INT,
                    categoryName CHAR(120),
                    author CHAR(120),
                    pubDate CHAR(30),
                    price INT
                    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS best_sellers 
                    (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    title CHAR(120),
                    categoryId INT,
                    categoryName CHAR(120),
                    author CHAR(120),
                    pubDate CHAR(30),
                    price INT
                    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS category
                    (
                    id INT PRIMARY KEY,
                    categoryName CHAR(120),
                    mall CHAR(120),
                    Depth_1 CHAR(120),
                    Depth_2 CHAR(120),
                    Depth_3 CHAR(120),
                    Depth_4 CHAR(120),
                    Depth_5 CHAR(120)
                    );""")

conn, cur = get_connection()
make_table(conn, cur)
#api data parsing

def get_api_data(item, start_num):
    key = 'ttbgooook1503001'
    querytpye = item
    start = start_num
    URI = f"http://www.aladin.co.kr/ttb/api/ItemList.aspx?ttbkey={key}&QueryType={querytpye}&MaxResults=100&start={start}&SearchTarget=Book&output=js&Version=20131101"
    reps = requests.get(URI)
    parsed_data = json.loads(reps.text)

    return parsed_data


def insert_category(file, conn, cur):
    with open(file, 'r') as csv_file:
        list = csv.reader(csv_file)
        next(list)
        
        
        for i in list:
            cur.execute(f"""INSERT INTO category 
                        (
                        id,
                        categoryName,
                        mall,
                        Depth_1,
                        Depth_2,
                        Depth_3,
                        Depth_4,
                        Depth_5
                        )
                        VALUES
                        (
                        {int(i[0])},
                        "{i[1]}",
                        "{i[2]}",
                        "{i[3]}",
                        "{i[4]}",
                        "{i[5]}",
                        "{i[6]}",
                        "{i[7]}"
                        );
                        """)
    
    conn.commit()

insert_category('Category_ID.csv', conn, cur)

def insert_newbooks(conn, cur):

    for i in range(0, 21):
        raw_data = get_api_data('ItemNewSpecial', i)
        time.sleep(1)
        for x in range(0, 50):
            data = raw_data['item'][x]
            cur.execute(f"""INSERT IGNORE INTO new_books
                        (
                        id,
                        title,
                        categoryId,
                        categoryName,
                        author,
                        pubDate,
                        price
                        )
                        VALUES
                        (
                        NULL,
                        "{data['title']}",
                        "{data['categoryId']}",
                        "{data['categoryName']}",
                        "{data['author']}",
                        "{data['pubDate']}",
                        "{data['priceStandard']}"
                        );
                        """)
    conn.commit()

insert_newbooks(conn, cur)

def insert_bestseller(conn, cur):

    for i in range(0, 21):
        raw_data = get_api_data('Bestseller', i)
        time.sleep(1)
        for x in range(0, 50):
            data = raw_data['item'][x]
            cur.execute(f"""INSERT IGNORE INTO best_sellers
                        (
                        id,
                        title,
                        categoryId,
                        categoryName,
                        author,
                        pubDate,
                        price
                        )
                        VALUES
                        (
                        NULL,
                        "{data['title']}",
                        "{data['categoryId']}",
                        "{data['categoryName']}",
                        "{data['author']}",
                        "{data['pubDate']}",
                        "{data['priceStandard']}"
                        );
                        """)
    conn.commit()

insert_bestseller(conn, cur)