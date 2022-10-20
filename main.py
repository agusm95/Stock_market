import yfinance as yf
import pandas as pd
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

#Elimina los precios de las acciones de la API de finanzas de Yahoo

def extract():
    stocks = ['AAPL', 'MSFT', 'MCD', 'GOOG', 'AMZN', 'NFLX', 'TWTR', 'JNJ']

    df_list = list()

    for ticker in stocks:
        data = yf.download(ticker, period='max', start = '2000-01-01')
        data['ticker'] = ticker 
        df_list.append(data)

    
    df = pd.concat(df_list)
    #Remuevo espacio en el nombre de columna
    df.reset_index(inplace=True)
    df['date'] = pd.to_datetime(df['Date']).dt.date
    df.drop(columns=['Date'], inplace=True)
    df.rename(columns = {'Adj Close':'AdjClose'}, inplace = True)
    
    return df

def extract_ticker():
    # extract the ticker data
    
    msft = yf.Ticker('MSFT')
    aapl = yf.Ticker('AAPl')
    google = yf.Ticker('GOOG')
    amazon = yf.Ticker('AMZN')
    netflix = yf.Ticker('NFLX')
    ibm = yf.Ticker('IBM')
    twitter = yf.Ticker('TWTR')
    jnj = yf.Ticker('JNJ')
    mcd = yf.Ticker('MCD')

    #creacion del diccionario
    dicc = {'Company_name':[msft.info['longName'], aapl.info['longName'], ibm.info['longName'], google.info['longName'], amazon.info['longName'], netflix.info['longName'], twitter.info['longName'], jnj.info['longName'], mcd.info['longName']],
                'Company_ticker':[msft.info['symbol'], aapl.info['symbol'], ibm.info['symbol'], google.info['symbol'], amazon.info['symbol'], netflix.info['symbol'], twitter.info['symbol'], jnj.info['symbol'], mcd.info['symbol']],
                'Closed_price':[msft.info['previousClose'], aapl.info['previousClose'], ibm.info['previousClose'], google.info['previousClose'], amazon.info['previousClose'], netflix.info['previousClose'], twitter.info['previousClose'], jnj.info['previousClose'], mcd.info['previousClose']],
                'Company_info':[msft.info['longBusinessSummary'], aapl.info['longBusinessSummary'], ibm.info['longBusinessSummary'], google.info['longBusinessSummary'], amazon.info['longBusinessSummary'], netflix.info['longBusinessSummary'], twitter.info['longBusinessSummary'], jnj.info['longBusinessSummary'], mcd.info['longBusinessSummary']],
                'Company_PE':[msft.info['trailingPE'], aapl.info['trailingPE'], ibm.info['trailingPE'], google.info['trailingPE'], amazon.info['trailingPE'], netflix.info['trailingPE'], twitter.info['trailingPE'], jnj.info['trailingPE'], mcd.info['trailingPE']],
                'Company_cash_flow':[msft.info['operatingCashflow'], aapl.info['operatingCashflow'], ibm.info['operatingCashflow'], google.info['operatingCashflow'], amazon.info['operatingCashflow'], netflix.info['operatingCashflow'], twitter.info['operatingCashflow'], jnj.info['operatingCashflow'], mcd.info['operatingCashflow']],
                'Company_dividend':[msft.info['dividendRate'], aapl.info['dividendRate'], ibm.info['dividendRate'], google.info['dividendRate'], amazon.info['dividendRate'], netflix.info['dividendRate'], twitter.info['dividendRate'], jnj.info['dividendRate'], mcd.info['dividendRate']]}
    
    df = pd.DataFrame(dicc)
    # return the dataframe
    return df


#Remuevo espacio en el nombre de columna

def transform_data():
    # create a dataframe to store the data
    df_transformed = extract_ticker()
    # round the values of the dataset to 2 decimal places
    df_transformed = df_transformed.round(2)
    return df_transformed


    
    return df_principal

def carga_sql():
    usuario = "root"
    password = "administrador"
    host= "localhost"
    port="3306"
    database="finance"
    conexion=create_engine(f'mysql://{usuario}:{password}@{host}:{port}/{database}')

    df_toload = transform_data()
    
    df_principal = extract()

    #Enviando las tablas
    df_principal.to_sql(con=conexion, name='principal',if_exists='append', index=False)
    df_toload.to_sql(con=conexion, name='tabla',if_exists='append', index=False)

    return True


    


def main():
    ## llamar a la función de extracción
     # call the transform function
    transform_data()
    # llamar funcion cargar
    carga_sql()
    return True

### Ejecutar capa ###

if __name__ == '__main__':
    main()
    print('Los datos han sido extraídos, transformados y guardados en una base de datos mysql llamada finance')