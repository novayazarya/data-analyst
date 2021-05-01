import psycopg2
import pandas as pd
import numpy as np
import string
import nltk
import configparser
import os
import sys
from gensim.models.word2vec import Word2Vec
from prepare_machine import PrepareMachine

BASE_DIR = os.path.split(os.path.abspath(__file__))[0]
config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR, 'config.cfg'))
DB_AUTH = {key: config['DB'][key] for key in config['DB']}
KEYWORDS = config['KEY'].get('keywords', '').split()

def connect_to_db(auth):
    try:
        conn = psycopg2.connect(**auth)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Database error {e}")
    return False


def from_db_to_df(auth):
    conn = connect_to_db(auth)
    query = """SELECT vacancy, description, array_to_string(skills, ' ') FROM vacancies;"""
    if conn:
        with conn:
            vacancies = pd.read_sql(query, conn)
    else:
        print("Database is not connected")
        sys.exit()
    return vacancies


def prepare_pipeline(data):
    try:
        with open(os.path.join(BASE_DIR, 'manual_stop_words.txt')) as fh:
            manual_stop_words = fh.read().rstrip().split()
    except FileNotFoundError:
        manual_stop_words = []
    
    prep = PrepareMachine(data)
    
    prep.tokenize(reg=r'\w+')
    prep.remove_numbers()
    prep.remove_stop_words(nltk.corpus.stopwords.words('russian'))
    prep.remove_stop_words(nltk.corpus.stopwords.words('english'))
    prep.normalization()
    prep.lemmatizing()
    prepared_data = prep.remove_stop_words(manual_stop_words)
    # prepared_data = prep.remove_hapaxes()
    return prepared_data


def main(auth):
    df = from_db_to_df(auth)
    df = df.drop_duplicates(subset='description', keep='first')
    data = df.apply(' '.join, axis=1)
    prepared_data = prepare_pipeline(data) 
    counts = nltk.FreqDist(sum(prepared_data, []))
    w2v = Word2Vec(sentences=prepared_data, vector_size=200, epochs=30)
    
    dfs, keys = [], []
    
    for key in KEYWORDS:
        lexemes = []
        for lexem, dist in w2v.wv.most_similar(key, topn=13):
            lexemes.append([lexem, dist])
        dfs.append(pd.DataFrame([[key, '', '']] + [[key] + x for x in lexemes]))
        keys += [x for x, _ in lexemes]

    groups = pd.concat(dfs)
    groups.columns = ['cluster', 'lexem', 'rate']
    groups['id'] = 'w2v.' + groups['cluster'] +'.'+ groups['lexem']
    groups['id'] =  groups['id'].str.rstrip('.')
    groups['value'] = groups['lexem'].apply(lambda x: counts.get(x), '')
    groups.fillna('', inplace=True)

    Q1 = groups[groups['value'].apply(lambda x: x != '')]['value'].quantile(0.25)
    Q3 = groups[groups['value'].apply(lambda x: x != '')]['value'].quantile(0.75)
    IQR = Q3 - Q1

    groups['value'] = groups['value'].apply(lambda x: x if x == '' or (Q1 - 1.5 * IQR) < x < (Q3 + 1.5 * IQR) 
                                                        else x * 4.0 if x < (Q3 + 1.5 * IQR) else x / 4.0)
    values = groups[groups['value'] != ''].apply(lambda df: df['value'] * df['rate'], axis=1)
    groups.loc[groups['value'] != '', 'value'] = values

    groups[['id', 'value']].to_csv(os.path.join(BASE_DIR, 'data.csv'), index=False)

if __name__ == '__main__':
    main(DB_AUTH)
