import sqlite3
import pandas as pd


def lite_to_pandas(sql: str, dbname: str='trades.db') -> pd.DataFrame:
    """Read data from a local SQLite database
    
    Parameters
    ----------
    sql: str
        A regular sql expression to run
    dbname: str
        Default absolute path to the local database

    Returns
    -------
    df: DataFrame
    """
    with sqlite3.connect(dbname) as connection:
        df = pd.read_sql(sql, connection)
        print(f'SQL fetched a df{df.shape}')
        return df
    

def pandas_to_lite(df: str, table: str, dbname: str='trades.db', mode: str='append'):
    """Read data from a local SQLite database
    
    Parameters
    ----------
    sql: str
        A regular sql expression to run
    table: str
        Target table name, that may contain also a schema
    mode: str
        Insertion mode: append, replace, fail
    dbname: str
        Default absolute path to the local database

    """
    with sqlite3.connect(dbname) as connection:
        df.to_sql(table, connection, if_exists=mode, index=False)
    print(f"SQL pushed a df{df.shape} to {dbname.split('.')[0]}.{table}")