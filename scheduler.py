from apscheduler.schedulers.blocking import BlockingScheduler
from database import make_table, insert_bestseller, insert_newbooks
from database import get_connection
from machine_model import get_data_from_db, cleaning_df, make_pickle

conn, cur = get_connection()
scheduler = BlockingScheduler({'apscheduler.timezone':'Asia/seoul'})

def insert_new_data():
    make_table(conn, cur)
    insert_bestseller(conn, cur)
    insert_newbooks(conn, cur)
    print("got it")

def make_new_model():
    df = get_data_from_db()
    clean_df = cleaning_df()
    make_pickle(clean_df)


scheduler.add_job(func=insert_new_data, trigger='interval', hours=24)
scheduler.add_job(func=make_new_model, trigger='interval', hours=25)

scheduler.start()