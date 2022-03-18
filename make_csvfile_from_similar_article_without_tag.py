import os
from pandas.core.dtypes.missing import notnull
from pandas.io.pytables import dropna_doc
import psycopg2
import pandas as pd
import re
from glob import glob
import MeCab 
from gensim.models.doc2vec import TaggedDocument
from gensim.models.doc2vec import Doc2Vec
import numpy as np
from sqlalchemy import create_engine

db_mt_entry_url = os.environ.get("DATABASE_URL_NEW_0304")
db_similar_article_url = os.environ.get("DATABASE_URL_NEW_0304")
db_similar_article_url_create_engine = os.environ.get("DATABASE_URL_CREATE_ENGINE_NEW_0304")

query_1 = 'SELECT * FROM similar_article'

print(db_similar_article_url)
print(db_similar_article_url_create_engine)

def get_connection(string):
    return psycopg2.connect(string)

conn = get_connection(db_mt_entry_url)
cur = conn.cursor()

cur.close
conn.close

df = pd.read_sql(sql = query_1,con = conn)
df.to_csv('result_of_analyze_with_tag.csv')