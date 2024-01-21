import pandas as pd
import pysolr
import gdown
import os


def load(df, entity_name):
    """
        Load data into a Solr collection.

        Parameters:
        - df: DataFrame, the data to be loaded into Solr.
        - entity_name: str, the name of the entity.

    """
    solr = pysolr.Solr(f'http://solr:8983/solr/{entity_name}/')
    documents = df.to_dict('records')
    solr.add(documents)
    solr.commit()


def transform(df, load_id):
    """
        Transform the DataFrame by adding a 'load_id' column and renaming columns for indexing on solr.

        Parameters:
        - df: DataFrame, the data to be transformed.
        - load_id: int, the load identifier.

        Returns:
        DataFrame
    """
    df['load_id'] = str(load_id)
    rename_dict = {col: col+"_s" for col in df.columns}
    df = df.rename(columns=rename_dict)
    return df


def extract(entity_name, file_id, load_log_key, load_id):
    """
        Extract data from a source using Google Drive, filter based on load_log_key, and return DataFrame.
        Data is extracted between (load_log_key - 7 days, load_log_key)

        Parameters:
        - entity_name: str, the name of the entity.
        - file_id: str, the Google Drive file ID.
        - load_log_key: str, the load log key.
        - load_id: int, the load identifier.

        Returns:
        DataFrame
    """
    print(entity_name, file_id, load_log_key, load_id)
    local_downloaded_path = f'{entity_name}_{load_id}.csv'
    gdown.download(f'https://drive.google.com/uc?id={file_id}', local_downloaded_path, quiet=False)
    df = pd.read_csv(local_downloaded_path, dtype=str)
    lower_load_log_key = (pd.to_datetime(load_log_key, format="%Y%m%d") -
                          pd.to_timedelta(7, unit='d')).strftime("%Y%m%d")
    df = df[(df['load_log_key'] <= load_log_key) & (df['load_log_key'] >= lower_load_log_key)]
    os.remove(local_downloaded_path)
    return df


def ingest_data(**kwargs):
    """
        Ingest data into Solr using the extract, transform, and load process.

        Parameters:
        - **kwargs: dictionary, containing parameters for extract, transform, and load.

    """
    df = extract(**kwargs['templates_dict'])
    df = transform(df, kwargs['templates_dict']['load_id'])
    load(df, kwargs['templates_dict']['entity_name'])
