from selenium import webdriver
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.common.by import By
import joblib


class Scrapper:
    def __init__(self, url, start_date, end_date):
        self.main_df = pd.DataFrame(columns=['day', 'news'])
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-ssl-errors=yes')
        options.add_argument('--ignore-certificate-errors')
        self.driver = webdriver.Remote(
            command_executor='http://airflow-chrome-1:4444/wd/hub',
            options=options
        )
        self.data = []
        self.url = url
        self.cur_day = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_day = datetime.strptime(end_date, '%Y-%m-%d')
        self.k = 0

    def get_next_day(self):
        self.cur_day = self.cur_day + timedelta(days=1)

    def cache(self):
        df = pd.DataFrame(self.data, columns=['day', 'news'])
        self.data = []
        df.reset_index(drop=True, inplace=True)
        self.main_df.reset_index(drop=True, inplace=True)
        self.main_df = pd.concat([self.main_df, df], axis=0)
        # print(df_cache.tail())
        print(self.cur_day, self.main_df.shape[0])

    def parse(self):
        self.k = 0

        while self.cur_day <= self.end_day:
            url_to_parse = self.url + self.cur_day.strftime('%Y-%m-%d') + '/'

            self.driver.get(url_to_parse)
            while True:
                button = self.driver.find_element(By.XPATH,
                                                  '//*[@id="finfin-local-plugin-block-item-publication-list-filter-date-showMore-container"]/span')
                try:
                    button.click()
                except:
                    break

            bs = BeautifulSoup(self.driver.page_source)
            news = bs.find_all('a', {'class': 'display-b pt2x pb05x publication-list-item'})
            for piece in news:
                to_insert = []
                to_insert.append(self.cur_day)
                to_insert.append(piece.find('div', {'class': 'font-l bold'}).text.strip())
                self.data.append(to_insert)
                self.k += 1

            if self.k >= 100:
                self.k = 0
                self.cache()

            self.get_next_day()
            
        self.cache()
        self.driver.quit()

    def estimate(self):
        pipeline = joblib.load('data/model.joblib')
        self.main_df['score'] = pipeline.predict(self.main_df.news)
        return self.main_df
        # self.main_df['score'] = self.main_df.news.apply(lambda x: pipeline.predict(x))















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
    "scrapping_news_dag",
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
    info["date_end"] = datetime.now().strftime("%Y-%m-%d")
    return info


def scrape_news(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="init")
    info["data_download_start"] = datetime.now().strftime("%Y%m%d %H:%M")
    start_date = info["date_start"]
    end_date = info["date_end"]

    scr = Scrapper("https://www.finam.ru/publications/section/market/date/", start_date, end_date)
    scr.parse()
    try:
        remove('data/model.joblib')
    except:
        pass
    s3_hook.download_file(
        key = "model.joblib",
        bucket_name = "studcamp-ml",
        local_path = 'data',
        preserve_file_name = True,
        use_autogenerated_subdir = False
    )
    
    news_df = scr.estimate()
    
    try:
        remove('data/finam_news_scored.csv')
    except:
        pass
    s3_hook.download_file(
        key = "finam_news_scored.csv",
        bucket_name = "studcamp-ml",
        local_path = 'data',
        preserve_file_name = True,
        use_autogenerated_subdir = False
    )
    s3_news_df = pd.read_csv("data/finam_news_scored.csv")
    news_df = pd.concat([s3_news_df,news_df], ignore_index=True)
    news_df.to_csv('data/news.csv', index=False)
    s3_hook.load_file('data/news.csv', key = "finam_news_scored.csv", 
                          bucket_name="studcamp-ml", replace=True)
    remove('data/finam_news_scored.csv')
    remove('data/news.csv')
    _LOG.info("News loading finished.")

    info["data_download_end"] = datetime.now().strftime("%Y%m%d %H:%M")

    return info

'''def scrape_index(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="init")
    info["index_download_start"] = datetime.now().strftime("%Y%m%d %H:%M")
    start_date = info["date_start"]
    end_date = info["date_end"]

    imoex_pars = IMOEXDataDownloader(start_date= start_date, end_date= end_date)
    spbe_pars = SPBEDataDownloader(start_date= start_date, end_date= end_date)

    imoex_pars.get_history_data()
    imoex_df = imoex_pars.main_df

    spbe_pars.get_history_data()
    spbe_df = spbe_pars.main_df

    s3_hook.download_file(
        key="IMOEX_filled.csv",
        bucket_name="studcamp-ml",
        local_path="data",
        preserve_file_name=True,
        use_autogenerated_subdir=False
    )

    s3_hook.download_file(
        key="SPBIRUS2_filled.csv",
        bucket_name="studcamp-ml",
        local_path="data",
        preserve_file_name=True,
        use_autogenerated_subdir=False
    )

    s3_hook.load_file(imoex_df, "IMOEX_filled.csv", 
                          bucket_name="studcamp-ml", replace=True)
    remove('IMOEX_filled.csv')
    
    
    _LOG.info("IMOEX index loading finished.")
    
    s3_hook.load_file(spbe_df, "SPBIRUS2_filled.csv", 
                          bucket_name="studcamp-ml", replace=True)
    remove('SPBIRUS2_filled.csv')
    
    _LOG.info("SPBE index loading finished.")
    

    info["index_download_end"] = datetime.now().strftime("%Y%m%d %H:%M")

    return info'''


t1 = PythonOperator(
    task_id="init",
    provide_context=True,
    python_callable=init,
    dag=dag
)

t2 = PythonOperator(
    task_id="scrape_news",
    provide_context=True,
    python_callable=scrape_news,
    dag=dag
)




t1 >> t2  #for imoex
#[t2,t3] >> t4 >> t6 >> t7 >> t8
#[t2,t3] >> t5 >> t9 >> t10 >> t11
    



