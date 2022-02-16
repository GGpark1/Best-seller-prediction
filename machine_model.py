from database import get_connection
import pandas as pd
import numpy as np
from category_encoders import OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline
import pickle

"""
데이터 가져오기
"""

conn, cur = get_connection()

def get_data_from_db():
    cur.execute("""SELECT nb.id, nb.title, nb.categoryId, nb.author, nb.price, bs.title as target, c.categoryName, c.Depth_1, c.Depth_2 
    FROM new_books nb
    LEFT JOIN best_sellers bs
    ON nb.title = bs.title
    JOIN category c
    ON nb.categoryId = c.id;""")
    
    data = cur.fetchall()
    df = pd.DataFrame(data, columns = ['id', 'title', 'categoryId', 'author', 'price', 'target', 'categoryName', 'depth_1', 'depth_2'])

    return df

df = get_data_from_db()


"""
Feature 정리
"""

def cleaning_df(df):
    #저자 분리

    df['author_1'] = df.author.str.split(',').str[0]
    df['author_2'] = df.author.str.split(',').str[1]
    df['author_3'] = df.author.str.split(',').str[2]

    # (지은이), (옮긴이) 태그 삭제

    df['author_1'] = df.author_1.str.split('(').str[0]
    df['author_2'] = df.author_2.str.split('(').str[0]
    df['author_3'] = df.author_2.str.split('(').str[0]

    # 특수문자 삭제

    df['author_1'].replace('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]', '', regex=True, inplace=True)
    df['author_2'].replace('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]', '', regex=True, inplace=True)
    df['author_2'].replace('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]', '', regex=True, inplace=True)

    # 칼럼 내 공백 삭제

    df['author_1'] = df['author_1'].str.strip()
    df['author_2'] = df['author_2'].str.strip()
    df['author_3'] = df['author_3'].str.strip()

    # 제2 분류 간소화

    df['depth_2'] = df.depth_2.str.split('/').str[0]

    # 예측을 위한 feature 선택

    df = df[['depth_1', 'depth_2', 'author_1', 'author_2', 'price', 'target']]

    df['target'] = df['target'].fillna('False')
    df.loc[(df['target'] != 'False'), 'target'] = 'True'

    df.loc[df['target'] == 'True', 'target'] = 1
    df.loc[df['target'] == 'False', 'target'] = 0

    clean_df = df

    return clean_df

clean_df = cleaning_df(df)

"""
RandomForest modeling
"""

def make_pickle(df):

    target = 'target'
    features = df.drop(columns = target).columns

    X_train  = df[features]
    y_train = df[target]

    y_train = list(df['target'].values)
    y_train = np.asarray(df['target'], dtype = "|S6")

    pipe = make_pipeline(OrdinalEncoder(),
                        SimpleImputer(),
                        RandomForestClassifier(random_state = 2,class_weight = 'balanced'))

    pipe.fit(X_train, y_train)
    
    with open('pipe_model.pkl', 'wb') as pickle_file:
        pickle.dump(pipe, pickle_file)


make_pickle(clean_df)