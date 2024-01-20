import pandas as pd
import pysolr
import gdown
import os


def load(df, connection_name):
    solr = pysolr.Solr(f'http://solr:8983/solr/{connection_name}/')
    documents = df.to_dict('records')
    solr.add(documents)
    solr.commit()


def transform(df, load_id):
    df['load_id'] = str(load_id)
    rename_dict = {col:col+"_s" for col in df.columns}
    df = df.rename(columns=rename_dict)
    return df


def extract(connection_name, file_id, load_log_key, load_id):
    print(connection_name, file_id, load_log_key, load_id)
    local_downloaded_path = f'{connection_name}_{load_id}.csv'
    gdown.download(f'https://drive.google.com/uc?id={file_id}', local_downloaded_path, quiet=False)
    df = pd.read_csv(local_downloaded_path, dtype=str)
    lower_load_log_key = (pd.to_datetime(load_log_key, format="%Y%m%d") - pd.to_timedelta(7, unit='d')).strftime("%Y%m%d")
    df = df[(df['load_log_key'] <= load_log_key) & (df['load_log_key'] >= lower_load_log_key)]
    os.remove(local_downloaded_path)
    return df


def ingest_data(**kwargs):
    df = extract(**kwargs['templates_dict'])
    df = transform(df, kwargs['templates_dict']['load_id'])
    load(df, kwargs['templates_dict']['connection_name'])

