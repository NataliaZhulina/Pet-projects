# pip install telegram
# pip install python-telegram-bot
import telegram
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from plotly.subplots import make_subplots
import seaborn as sns
import io
from io import StringIO
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import sys
import os

connection = {
    'host': '....',
    'password': '...',
    'user': '...',
    'database': '...'
}

# Функция для получения датафрейма из базы данных Clickhouse
def ch_get_df(query='SELECT 1', connection=connection):
    result = ph.read_clickhouse(query, connection=connection)
    return result

my_token = '....'
chat_id =  ....

def check_anomaly(df, metric, a=4,n=5):
    # функция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a*df['iqr']
        df['low'] =  df['q25'] - a*df['iqr']
    
        df['up'] = df['up'].rolling(n, center=True, min_periods =1).mean()
        df['low'] = df['low'].rolling(n, center=True,min_periods =1).mean()
    
        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0
    
        return is_alert, df
    
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n-zhulina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 4),
}

schedule_interval = timedelta(minutes=15)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kc_alert_nzhulina():
    @task
    def run_alerts_feed(chat=None):
        # непосредственно сама система алертов
        chat_id = chat or 101509140
        bot = telegram.Bot(token = '...')
        query = """
            SELECT 
                    toStartOfFifteenMinutes(time) as ts 
                    , toDate(time) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_feed
                    ,countIf(user_id, action = 'view') as views
                    ,countIf(user_id, action = 'like') as likes
            FROM simulator_20230220.feed_actions
            WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts"""
        data = ch_get_df(query=query)
        print(data)
        metrics_list = ['users_feed','views', 'likes']
        for metric in metrics_list:
            print(metric)
            df = data[['ts','date', 'hm', metric]].copy()
            is_alert,df = check_anomaly(df,metric)
            
            if is_alert ==1:
                msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%}'''.\
                format(metric= metric, current_val= df[metric].iloc[-1],last_val_diff = abs(1-df[metric].iloc[-1]/df[metric].iloc[-2]))                                                                                                             
                sns.set(rc={'figure.figsize':(16,18)})
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label=metric)
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='ap')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
                
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel = 'time')
                ax.set_title(metric)
                ax.set(ylim=(0, None))
                
               
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                
                bot.sendMessage(chat_id=chat_id, text = msg)
                bot.sendPhoto(chat_id=chat_id, photo = plot_object)
                
    @task
    def run_alerts_msg(chat=None):
        # непосредственно сама система алертов
        chat_id = chat or ....
        bot = telegram.Bot(token = '....')
        query = """
            SELECT 
                    toStartOfFifteenMinutes(time) as ts 
                    , toDate(time) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_msg
                    , count(user_id) as messages
            FROM simulator_20230220.message_actions
            WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts"""
        data = ch_get_df(query=query)
        #data = data.reset_index()
        print(data)
        metrics_list = ['users_msg', 'messages']
        for metric in metrics_list:
            print(metric)
            df = data[['ts','date', 'hm', metric]].copy()
            is_alert,df = check_anomaly(df,metric)
            
            if is_alert ==1:
                msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%}'''.\
                format(metric= metric, current_val= df[metric].iloc[-1], last_val_diff = abs(1-df[metric].iloc[-1]/df[metric].iloc[-2]))                                                                                                                                                 
                sns.set(rc={'figure.figsize':(16,18)})
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label=metric)
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='ap')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
                
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel = 'time')
                ax.set_title(metric)
                ax.set(ylim=(0, None))
                
               
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                
                bot.sendMessage(chat_id=chat_id, text = msg)
                bot.sendPhoto(chat_id=chat_id, photo = plot_object)
    
    run_alerts_feed(#chat)
    run_alerts_msg(#chat)
nz_alert_report = kc_alert_nzhulina()
                