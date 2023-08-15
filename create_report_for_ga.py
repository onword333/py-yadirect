from Ads import Ads
from Report import Report
from urllib.parse import urlparse, parse_qs
import pandas as pd
import json
from io import StringIO
import datetime as dt
import re


def get_keyword(row):
   # (not set) по умолчанию, нужно чтобы было сопоставление, 
   # иначе расход будет 0
  term = '(not set)'
  cond_impress = row['Тип условия показа']
  if (cond_impress == 'автотаргетинг'):
    term = row['Условие показа']
    term = re.sub(r"[\'+]", "", term).strip()

  if (cond_impress == 'фраза'):
    term = row['Условие показа'].split(' -')[0]
    term = re.sub(r"[\'+]", "", term).strip()
  return term



def get_position(row):
  # для поиска по умолчанию ставим 1 
  # т.к. невозможно понять какое было место при выгрузки расходов
  pos = {'поиск': '1', 'сети': '0'}
  return pos.get(row['Тип площадки'], '')



def get_source_type(row):
  source_types = {'поиск': 'search', 'сети': 'context'}
  return source_types.get(row['Тип площадки'], '')



def get_utm(row, utm_name):
  utm_value = ''
  dict = {
    '{campaign_name}': row['Кампания'], 
    '{campaign_id}': str(row['№ Кампании']),
    '{gbid}': str(row['№ Группы']),
    '{ad_id}': str(row['№ Объявления']),
    '{banner_id}': str(row['№ Объявления']),
    '{source_type}': get_source_type(row),
    '{phrase_id}': str(row['№ Условия показа']),
    '{position}': get_position(row),
    '{keyword}': get_keyword(row)
  }

  url_str = row['CampaignUrlPath']

  # сообщим в случае когда кампания отсутствует в report.tsv
  if (pd.isnull(url_str)):
    print(row['№ Кампании'], '- не найдена')

  for key, val in dict.items():
    url_str = url_str.replace(key, val)

  parse = urlparse(url_str)
  params = parse_qs(parse.query)

  if utm_name in params:
    utm_value = params[utm_name][0]
  
  return utm_value



def convertDate(val, str_from, str_to):
  date = dt.datetime.strptime(val, str_from).date()
  return date.strftime(str_to)


# читаем файл статистики из директа
file_report = './report/2023-07-26_2023-08-05_gorodinvestorov-new.csv'
col_types = {
  'Дата': 'string',
  'Кампания': 'string',
  '№ Кампании': 'string',
  'Группа': 'string',
  '№ Группы': 'string',
  '№ Объявления': 'string',
  'Условие показа': 'string',
  '№ Условия показа': 'string',
  'Тип условия показа': 'string',
  'Тип площадки': 'string',
  'Показы': 'int32',
  'Клики': 'int32',
  'Расход (руб.)': 'string'
}

direct_report = pd.read_csv(file_report, skiprows = 4, sep = ';', dtype = col_types);
#direct_report['Дата'] = direct_report['Дата'].apply(convertDate, args=('%d.%m.%Y', '%Y%m%d'))                      # конвертируем дату
direct_report['Дата'] = direct_report['Дата'].apply(convertDate, args=('%d.%m.%Y', '%Y-%m-%d'))                      # конвертируем дату

direct_report['№ Объявления'] = direct_report['№ Объявления'].str.replace('M-', '')   # земена "M-" на пустую строку
direct_report['Расход (руб.)'] = direct_report['Расход (руб.)'].str.replace(',', '.') # замена "," на точку "."
direct_report['Расход (руб.)'] = direct_report['Расход (руб.)'].astype(float)         # преобразования в float


#date_start = convertDate(direct_report['Дата'].iloc[0], '%Y%m%d', '%Y-%m-%d')
#date_end = convertDate(direct_report['Дата'].iloc[-1], '%Y%m%d', '%Y-%m-%d')

date_start = direct_report['Дата'].iloc[0]
date_end = direct_report['Дата'].iloc[-1]

client = 'result_' # указать префикс клиента
token = '' # токен я.директ
report_name = client + date_start + '_' + date_end + '_' + dt.datetime.today().strftime('%H_%M')

report = Report(report_name, token)
report.set_type('CUSTOM_REPORT')
report.set_data_range('CUSTOM_DATE')
report.set_period(date_start, date_end)


# колонки
report.add_field('Date')
report.add_field('CampaignName')
report.add_field('CampaignId')
report.add_field('AdGroupId')
report.add_field('AdId')
report.add_field('CriteriaId')
report.add_field('CampaignUrlPath')
report.add_field('AdNetworkType')
report.add_field('Impressions')
report.add_field('Clicks')
report.add_field('Cost')

result = report.get_report()
#with open('statistics__1.tsv', 'w') as file: 
#  file.write(result)

#exit()
report_data = pd.read_csv(StringIO(result), sep='\t', dtype={'CampaignId': 'string', 'AdId': 'string', 'CampaignUrlPath': 'string'})
report_data.to_csv(report_name + '_statistic.csv');
report_data.info()

ads_without_url = report_data[report_data.CampaignUrlPath == '--']['AdId']
uniq_ads_without_url = ads_without_url.drop_duplicates().to_list()


ads = Ads(token)
ads.add_field('Id')
ads.add_field('CampaignId')
ads.set_criteria('Ids', uniq_ads_without_url)
ads.add_param('TextAdFieldNames', ['Href'])
result = ads.get_ads()
res_json = json.loads(result)
ads_urls = pd.json_normalize(res_json['result']['Ads'])
ads_urls = ads_urls.astype({'Id': 'string', 'TextAd.Href': 'string'})
print('Ads gets and save')
ads_urls.to_csv(report_name + '_ads_urls.csv')
ads_urls.info()


# соединяем с таб. url по id кампании
merged_report = report_data.merge(ads_urls[['Id', 'TextAd.Href']], how = 'left', left_on='AdId', right_on='Id')
merged_report['CampaignUrlPath'].where(merged_report['CampaignUrlPath']!='--', merged_report['TextAd.Href'], inplace=True)
merged_report.drop(columns=['TextAd.Href', 'Id'], inplace=True)
merged_report.to_csv(report_name + '_merged_statistic_and_url.csv')
print('merge is done and save')
merged_report.info()


report_group_data = merged_report[['CampaignId', 'CampaignUrlPath']].drop_duplicates()

# ============
# содиняем данные и делаем файл для google analytics
# соединяем с таб. url по id кампании
ga_data = direct_report.merge(report_group_data, how = 'left', left_on='№ Кампании', right_on='CampaignId')
print(ga_data.info(memory_usage='deep'))

ga_data.to_csv('direct_report_marged_' + report_name + '.csv')




ga_data['utm_source'] = ga_data.apply(get_utm, args=['utm_source'], axis = 1)
ga_data['utm_medium'] = ga_data.apply(get_utm, args=['utm_medium'], axis = 1)
ga_data['utm_campaign'] = ga_data.apply(get_utm, args=['utm_campaign'], axis = 1)
ga_data['utm_content'] = ga_data.apply(get_utm, args=['utm_content'], axis = 1)
ga_data['utm_term'] = ga_data.apply(get_utm, args=['utm_term'], axis = 1)
ga_data['ga:adwordsCampaignID'] = client + ga_data['№ Кампании']

# переименуем колокни
new_colomns_name = {
  'Дата': 'ga:date',
  'utm_source': 'ga:source', 
  'utm_medium': 'ga:medium',
  'utm_campaign': 'ga:campaign',
  'utm_content': 'ga:adContent',
  'utm_term': 'ga:keyword',
  'Показы': 'ga:impressions',
  'Клики': 'ga:adClicks',
  'Расход (руб.)': 'ga:adCost'
}
ga_data.rename(columns=new_colomns_name, inplace = True)


#merged_data['utm_term'] = merged_data['utm_term'].str.replace(r"['+]", "", regex = True) # замена '+ на пустую строку ""
#merged_data['utm_term'].replace(r"['+]", '', regex=True, inplace=True)
#merged_data['utm_term'] = merged_data['utm_term'].str.strip() # после замены нужно удалить по краям пробелы

# для отладки
#merged_data = merged_data[['Дата', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'Тип условия показа', 'Условие показа', 'utm_term', 'Показы', 'Клики', 'Расход (руб.)', 'Тип площадки']]

# для ga
ga_dimensions = ['ga:date', 'ga:source', 'ga:medium', 'ga:campaign', 'ga:adContent', 'ga:keyword', 'ga:adwordsCampaignID']

# для ga4 только по source, medium, campaign можно делать
#ga_dimensions = ['ga:date', 'ga:source', 'ga:medium', 'ga:campaign', 'ga:adwordsCampaignID']
ga_metrics = ['ga:impressions', 'ga:adClicks', 'ga:adCost']

merged_data = ga_data[ga_dimensions + ga_metrics]

merged_data = merged_data.groupby(by=ga_dimensions).sum()

merged_data['ga:adCost'] = merged_data['ga:adCost'].round(decimals = 2)


print(merged_data)

pd.DataFrame.to_csv(merged_data, report_name + '_for_ga.csv', sep = ',')

