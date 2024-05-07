import requests
from datetime import datetime, timedelta
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import joblib


class IMOEXDataDownloader:
    def __init__(self, start_date, end_date=datetime.now().strftime('%Y-%m-%d')):
        self.main_df = pd.DataFrame()
        self.data = []
        self.today_df = []
        self.start_date = start_date
        self.end_date = end_date
        self.born_date = datetime.strptime("2004-01-01", '%Y-%m-%d')


    def get_history_data(self):
            """
            Парсит и сохраняет архивные данные в указанном промежутке.
            """
            self.download_data()
            self.get_open_price()
            self._clean_data()


    def download_data(self):
        start_date_dt = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(self.end_date, '%Y-%m-%d')

        if start_date_dt < self.born_date:
            print("Дата начала должна быть не ранее 2004-01-01.")
            return
        
        days_difference = (end_date_dt - start_date_dt).days
        iterations = days_difference // 100 + (1 if days_difference % 100 != 0 else 0)

        for _ in range(iterations):
            temporary_end = min(start_date_dt + timedelta(days=100), end_date_dt)
            start_date_str = start_date_dt.strftime('%Y-%m-%d')
            temporary_end_str = temporary_end.strftime('%Y-%m-%d')

            url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/securities/IMOEX.csv?from={start_date_str}&till={temporary_end_str}"

            response = requests.get(url)
            if response.status_code == 200:
                content_string = response.content.decode('windows-1251')
                content_lines = content_string.splitlines()
                clean_content = '\n'.join(content_lines[1:-6])  
                self.data = pd.read_csv(StringIO(clean_content), sep=';')
                self.main_df = pd.concat([self.main_df, self.data], ignore_index=True)
            else:
                print(f"Ошибка при скачивании файла: {response.status_code}")

            start_date_dt = temporary_end + timedelta(days=1)
        

    def _clean_data(self):
        self.main_df = self.main_df[['TRADEDATE', 'OPEN']].rename(columns={"TRADEDATE": "DATE"})
        self.main_df = pd.concat([self.main_df, self.today_df], ignore_index=True)
        self.main_df['DATE'] = pd.to_datetime(self.main_df['DATE'], format='%Y-%m-%d')

        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        self.main_df = self.main_df[(self.main_df['DATE'] >= start_date_dt) & (self.main_df['DATE'] <= end_date_dt)]

        date_range = pd.date_range(start=start_date_dt, end=end_date_dt)
        date_df = pd.DataFrame(date_range, columns=['DATE'])

        self.main_df = pd.merge(date_df, self.main_df, on='DATE', how='left')
        self.main_df['OPEN'] = self.main_df['OPEN'].fillna(method='ffill')

        self.main_df = self.main_df.dropna().drop_duplicates(subset='DATE', keep='last')


    def get_open_price(self):
        """
        Забирает цену открытия за сегодня с мосБиржи.
        """
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-ssl-errors=yes')
        options.add_argument('--ignore-certificate-errors')
        driver = webdriver.Remote(
            command_executor='http://airflow-chrome-1:4444/wd/hub',
            options=options
        )
        try:
            driver.get('https://ru.tradingview.com/symbols/MOEX-IMOEX/')
            time.sleep(1)
            price_elements = driver.find_elements(By.CSS_SELECTOR, '.apply-overflow-tooltip.value-GgmpMpKr')
            open = price_elements[2].text  
            open = str(open).replace("RUB", "")
            open = [float(open)]
            
        finally:
            driver.quit()
        
        today = [datetime.now().strftime('%Y-%m-%d')]
        self.today_df = pd.DataFrame({
            "DATE":today,
            "OPEN":open
        })


class SPBEDataDownloader:
    def __init__(self, start_date, end_date=datetime.now().strftime('%Y-%m-%d'), download_folder = os.path.join(os.getcwd(), "data"), prefix = "SPBIRUS"):
        self.start_date = start_date
        self.end_date = end_date
        self.download_folder = download_folder
        self.prefix = prefix
        self.main_df = pd.DataFrame()
        self.today_df = pd.DataFrame()
        self.url = "https://spbexchange.ru/stocks/indices/SPBIRUS/"
        self.response = requests.get(self.url)



    def get_history_data(self):
        """
        Парсит и сохраняет архивные данные с 2015-11-25 по вчерашний день.
        """
        self._download_data()
        self.get_open_price()
        file_path = self._find_download_data()

        if file_path:
            print(f"Найден файл: {file_path}")
            self._clean_data(file_path)

        else:
            print("Файл не найден.")


    def _find_download_data(self):
        """
        Ищет скаченный файл с архивными данными индекса.
        """
        files_with_dates = [(file, os.path.getmtime(os.path.join(self.download_folder, file))) 
                    for file in os.listdir(self.download_folder) if file.startswith('SPBIRUS')]
        
        files_with_dates.sort(key=lambda x: x[1], reverse=True)
        
        if files_with_dates:
            latest_file_path = os.path.join(self.download_folder, files_with_dates[0][0])
            return latest_file_path
        
        else:
            print("Файлы с заданным префиксом не найдены.")
            return False


    def _download_data(self):
        """
        Загружает данные, кликая по кнопке на странице.
        
        :param url: URL страницы для загрузки.
        """
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-ssl-errors=yes')
        options.add_argument('--ignore-certificate-errors')
        driver = webdriver.Remote(
            command_executor='http://airflow-chrome-1:4444/wd/hub',
            options=options
        )
        driver.get(self.url)
        try:
            button_selector = '.Button_root__ZhrsR.Button_spb__tqqde.Button_contained__7HNa2.Button_cancel__5lME4.Button_sm__ndTMz'
            driver.find_element(By.CSS_SELECTOR, button_selector).click()
            time.sleep(2)  # Ожидание загрузки файла
        finally:
            driver.quit()


    def _clean_data(self, file_path):
        """
        Обрабатывает скачанный файл: читает и сохраняет его с новой структурой.
        
        :param file_path: Полный путь к файлу для обработки.
        """
        self.main_df = pd.read_csv(file_path, sep=';', decimal=',',names = ["OPEN", "DATE"],header=None)
        self.main_df = pd.concat([self.main_df, self.today_df], ignore_index=True)
        self.main_df['DATE'] = pd.to_datetime(self.main_df['DATE'], format='%Y-%m-%d')

        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        self.main_df = self.main_df[(self.main_df['DATE'] >= start_date_dt) & (self.main_df['DATE'] <= end_date_dt)]

        date_range = pd.date_range(start=start_date_dt, end=end_date_dt)
        date_df = pd.DataFrame(date_range, columns=['DATE'])

        self.main_df = pd.merge(date_df, self.main_df, on='DATE', how='left')
        self.main_df['OPEN'] = self.main_df['OPEN'].fillna(method='ffill')

        self.main_df = self.main_df.dropna().drop_duplicates(subset='DATE', keep='last')



    def get_open_price(self):
        """
        Забирает цену открытия с Питерской Биржи и обновляет данные в архивных значениях.
        """
        html_content = self.response.text
        
        soup = BeautifulSoup(html_content, 'html.parser')
        price_elements = soup.select('.IndexesCard_value__cjTBc')

        open = float(price_elements[0].text.replace(',', '.').replace(' ', ''))
        today = datetime.now().strftime('%Y-%m-%d')

        self.today_df = pd.DataFrame({
            "DATE":[today],
            "OPEN":[open]
        })

        
'''if __name__ == "__main__":
    imoex_parser = SPBEDataDownloader(start_date="2009-01-01")
    imoex_parser.get_history_data()
    imoex_parser.main_df.to_csv("test2.csv", index= False)'''













from datetime import timedelta
from datetime import datetime
import pandas as pd
from os import remove
from catboost import CatBoostRegressor as cbr
import joblib

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
#from news_parser import Scrapper
#from Parsing_index import IMOEXDataDownloader, SPBEDataDownloader
from airflow.utils.dates import days_ago
from botocore.exceptions import (
    ConnectTimeoutError,
    EndpointConnectionError,
    ConnectionError,
)

_LOG = logging.getLogger()
_LOG.addHandler(logging.StreamHandler())

cid = "s3_connection"
s3_hook = S3Hook(cid)


DEFAULT_ARGS = {
    "owner": "Team 22",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "scrapping_index_dag",
    tags=["mlops"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)

def init():
    """
    Step0: Pipeline initialisation.
    """
    info = {}
    info["start_timestamp"] = datetime.now().strftime("%Y%m%d %H:%M")
    # Импользуем данные с сегодня на 2 года назад.
    info["date_start"] = datetime.now().strftime("%Y-%m-%d")
    info["date_end"] = (datetime.now()+timedelta(1)).strftime("%Y-%m-%d")
    return info


def scrape_index(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="init")
    info["index_download_start"] = datetime.now().strftime("%Y%m%d %H:%M")
    start_date = info["date_start"]
    end_date = info["date_end"]

    imoex_pars = IMOEXDataDownloader(start_date= start_date, end_date= end_date)
    spbe_pars = SPBEDataDownloader(start_date= start_date, end_date= end_date)

    imoex_pars.get_history_data()
    imoex_df = imoex_pars.main_df[:-1]

    spbe_pars.get_history_data()
    spbe_df = spbe_pars.main_df[:-1]
    
    try:
        remove('data/IMOEX_filled.csv')
    except:
        pass
    s3_hook.download_file(
        key="IMOEX_filled.csv",
        bucket_name="studcamp-ml",
        local_path="data",
        preserve_file_name=True,
        use_autogenerated_subdir=False
    )
    s3_imoex = pd.read_csv('data/IMOEX_filled.csv')
    update_imoex = pd.concat([s3_imoex,imoex_df], ignore_index=True)
    update_imoex.to_csv('data/update_imoex.csv', index=False)
    
    try:
        remove('data/SPBIRUS2_filled.csv')
    except:
        pass
    s3_hook.download_file(
        key="SPBIRUS2_filled.csv",
        bucket_name="studcamp-ml",
        local_path="data",
        preserve_file_name=True,
        use_autogenerated_subdir=False
    )
    s3_spbe = pd.read_csv('data/SPBIRUS2_filled.csv')
    update_spbe = pd.concat([s3_spbe,spbe_df], ignore_index=True)
    update_spbe.to_csv('data/update_spbe.csv', index=False)
    
    s3_hook.load_file('data/update_imoex.csv', "IMOEX_filled.csv", 
                          bucket_name="studcamp-ml", replace=True)
    remove('data/IMOEX_filled.csv')
    remove('data/update_imoex.csv')
    
    _LOG.info("IMOEX index loading finished.")
    
    s3_hook.load_file('data/update_spbe.csv', "SPBIRUS2_filled.csv", 
                          bucket_name="studcamp-ml", replace=True)
    remove('data/SPBIRUS2_filled.csv')
    remove('data/update_spbe.csv')
    
    _LOG.info("SPBE index loading finished.")
    
    info["index_download_end"] = datetime.now().strftime("%Y%m%d %H:%M")

    return info


t1 = PythonOperator(
    task_id="init",
    provide_context=True,
    python_callable=init,
    dag=dag
)

t2 = PythonOperator(
    task_id="scrape_index",
    provide_context=True,
    python_callable=scrape_index,
    dag=dag
)




t1 >> t2  #for imoex
#[t2,t3] >> t4 >> t6 >> t7 >> t8
#[t2,t3] >> t5 >> t9 >> t10 >> t11
    




