def kline(symb, tf, start_dt):
    url='https://api.bybit.com'
    path='/v5/market/kline'
    URL = url + path

    start = int(start_dt.timestamp() * 1000)
    end = int(pd.to_datetime(date.today() - timedelta(1)).timestamp() * 1000)

    batch_size = 200
    dtf = int(tf) * 60 * 1000
    N = round((end - start) / dtf + 0.5)
    batch_cnt =  N // batch_size + int(N % batch_size != 0)

    dfs = pd.DataFrame()
    for i in range(batch_cnt):
        batch_start = start + i*batch_size*dtf
        batch_end = min(end,batch_start + batch_size*dtf)

        params={'category': 'linear', 'symbol': symb, 'interval': tf, 'start': batch_start, 'end': batch_end}
        r = requests.get(URL,params=params)

        df = pd.DataFrame(r.json()['result']['list'])
        m = pd.DataFrame()
        m['report_dttm'] = pd.to_datetime(df.iloc[:, 0], unit='ms')
        m['open_price'] = df.iloc[:, 1].astype(float)
        m['high_price'] = df.iloc[:, 2].astype(float)
        m['low_price'] = df.iloc[:, 3].astype(float)
        m['close_price'] = df.iloc[:, 4].astype(float)
        m['volume'] = df.iloc[:, 5].astype(float)

        dfs = pd.concat([dfs,m])

    return dfs

user = USER
password = PASSWORD
host = HOST
port = PORT
database_name = DATABASE_NAME

library_import = False
creating_api = False
data_frame_is_ok = False
is_connected = False
is_inserted = False

try:
    import requests
    import pandas as pd
    from datetime import timedelta, date, datetime, timezone
    import sqlalchemy

    library_import = True
    current_time = datetime.now()

    print(f'Succesful imported libraries. Time: {current_time}')
    
except Exception as e:
    current_time = datetime.now()
    print(f'Fatal error. Libraries is not imported. Time: {current_time}\n\n {str(e)}')

if library_import:
    try:
        # Параметры запуска
        # Какую валюту парсим
        symb = 'BTCUSDT'
        # Интервал в минутах
        tf = '5'
        # Сколько берем от текущей
        engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
        con=engine.connect()
        query = sqlalchemy.text(f'''SELECT MAX(report_dttm) max_report_dttm
                                    FROM btc_quotes bq;''')
        max_report_dttm = con.execute(query).fetchone()[0]
        
        # Запрос к бирже
        df = kline('BTCUSDT', '5', max_report_dttm)
        
        creating_api = True
        current_time = datetime.now()
        con.close()

    except Exception as e:
        current_time = datetime.now()
        print(f'Fatal error on created Api connection. Time: {current_time}\n\n{str(e)}')
else:
    pass

if creating_api:
    print(f'Succesful created Api connection. Time: {current_time}\n')
    try:
        min_report_dttm = min(df['report_dttm'])
        max_report_dttm = max(df['report_dttm'])
        df_rows = df.shape[0]
        df_columns = df.shape[1]

        data_frame_is_ok = True
        current_time = datetime.now()

        print(f'Dataframe fill is succesful. Time: {current_time}\n\nResult DF head:\n{df.head()}\n\nResult DF tail:\n{df.tail()}\n\nmin_report_dttm = {min_report_dttm}\nmax_report_dttm = {max_report_dttm}\ndf_rows={df_rows}\ndf_columns={df_columns}')

    except:
        current_time = datetime.now()
        print(f'Fatal error. Dataframe fill is not succesful. Time: {current_time}')
else:
    pass

if data_frame_is_ok:
    try:
        engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
        con=engine.connect()

        is_connected = True
        current_time = datetime.now()

        print(f'1. Successful connection. Time: {current_time}')
        
    except:
        current_time = datetime.now()
        print(f'Fatal error. Can`t establish connection to database. Time: {current_time}')
else:
    pass

if is_connected:
    try:
        current_time = datetime.now()

        print(f'2. Deleting existing rows. Time: {current_time}')

        query = sqlalchemy.text(f'''SELECT COALESCE(SUM(1), 0)
                                    FROM btc_quotes bq 
                                    WHERE report_dttm BETWEEN '{min_report_dttm}'::timestamp AND '{max_report_dttm}'::timestamp;''')
        cnt_existing_values = con.execute(query).fetchone()[0]
        
        current_time = datetime.now()
        
        print(f'3. Rows to delete: {cnt_existing_values}. Time: {current_time}')

        query = sqlalchemy.text(f'''DELETE FROM btc_quotes bq 
                                    WHERE report_dttm BETWEEN '{min_report_dttm}'::timestamp AND '{max_report_dttm}'::timestamp;''')
        con.execute(query)
        
        current_time = datetime.now()

        print(f'4. Existing rows deleted. Inserting new rows. Rows to insert: {df_rows}. Time: {current_time}')

        cnt_inserted_values = df.to_sql(name = 'btc_quotes', con = con, if_exists = 'append', index = False)

        is_inserted = True
        current_time = datetime.now()

        con.commit()
        con.close()

        print(f'5. Successful inserted. Inserted value: {cnt_inserted_values}. Time: {current_time}')

    except:
        current_time = datetime.now()
        print(f'Fatal error. Can not insert or delete values check the sql query. Time: {current_time}')
else:
    pass

print(f'library_import: {library_import}\ncreating_api: {creating_api}\ndata_frame_is_ok: {data_frame_is_ok}\nis_connected: {is_connected}\nis_inserted: {is_inserted}')
