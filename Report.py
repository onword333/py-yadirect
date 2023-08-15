import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import pandas as pd
from io import StringIO

# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import sys

class Report:
  reports_endpoint = 'https://api.direct.yandex.com/json/v5/reports'
  #reports_endpoint = 'https://webhook.site/f49a3943-0b63-41c3-9a61-be569015cc39'
  body = {}
  selection_criteria = {}
  field_names = []
  params = {}
  headers = {}
  order_by = []


  def __init__(self, report_name, token, include_vat = 'YES', format = 'TSV'):
    self.set_name(report_name)
    self.__token = token
    self.set_vat(include_vat)
    self.set_include_discount('NO')
    self.set_format(format)

    self.headers = {
      # OAuth-токен. Использование слова Bearer обязательно
           "Authorization": "Bearer " + token,
           # Логин клиента рекламного агентства
           #"Client-Login": clientLogin,
           # Язык ответных сообщений
           "Accept-Language": "ru",
           # Режим формирования отчета
           "processingMode": "auto",
           # Формат денежных значений в отчете
           "returnMoneyInMicros": "false",
           # Не выводить в отчете строку с названием отчета и диапазоном дат
          "skipReportHeader": "true",
           # Не выводить в отчете строку с названиями полей
           #"skipColumnHeader": "true",
           # Не выводить в отчете строку с количеством строк статистики
           "skipReportSummary": "true"
    }



  def u(self, x):
    if type(x) == type(b''):
      return x.decode('utf8')
    else:
      return x



  def set_period(self, start, end):
     self.selection_criteria['DateFrom'] = start
     self.selection_criteria['DateTo'] = end



  def add_field(self, field_name):
    self.field_names.append(field_name)



  def add_param(self, param, val):
    self.params[param] = val



  def set_name(self, val):
    self.add_param('ReportName', val) 



  def set_type(self, val):
    self.add_param('ReportType', val) 



  def set_data_range(self, val):
    self.add_param('DateRangeType', val)



  def set_format(self, val):
    self.add_param('Format', val)



  def set_vat(self, val):
    self.add_param('IncludeVAT', val)



  def set_include_discount(self, val):
    self.add_param('IncludeDiscount', val)  
  


  def get_report(self):
    self.add_param('SelectionCriteria', self.selection_criteria)
    self.add_param('FieldNames', self.field_names)
    
    #if (len(self.order_by)):
    #  self.add_param('OrderBy', self.order_by)

    self.body['params'] = self.params
    return self.send_data_post()

  
  def set_order_by(self, field, sort = 'ASCENDING'):
    self.order_by.append({'Field': field, 'SortOrder': sort})
  
  
  def send_data_post(self):
    result = '';
    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
      try:
        req = requests.post(self.reports_endpoint, json.dumps(self.body, indent=4), headers = self.headers)
        req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
        if req.status_code == 400:
          print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
          print("RequestId: {}".format(req.headers.get("RequestId", False)))
          print("JSON-код запроса: {}".format(self.u(self.body)))
          print("JSON-код ответа сервера: \n{}".format(self.u(req.json())))
          break
        elif req.status_code == 200:
          print("Отчет создан успешно")
          print("RequestId: {}".format(req.headers.get("RequestId", False)))
          #print("Содержание отчета: \n{}".format(self.u(req.text)))
          result = format(self.u(req.text))
          break
        elif req.status_code == 201:
          print("Отчет успешно поставлен в очередь в режиме офлайн")
          retryIn = int(req.headers.get("retryIn", 60))
          print("Повторная отправка запроса через {} секунд".format(retryIn))
          print("RequestId: {}".format(req.headers.get("RequestId", False)))
          sleep(retryIn)
        elif req.status_code == 202:
          print("Отчет формируется в режиме офлайн")
          retryIn = int(req.headers.get("retryIn", 60))
          print("Повторная отправка запроса через {} секунд".format(retryIn))
          print("RequestId:  {}".format(req.headers.get("RequestId", False)))
          sleep(retryIn)
        elif req.status_code == 500:
          print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
          print("RequestId: {}".format(req.headers.get("RequestId", False)))
          print("JSON-код ответа сервера: \n{}".format(self.u(req.json())))
          break
        elif req.status_code == 502:
          print("Время формирования отчета превысило серверное ограничение.")
          print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
          print("JSON-код запроса: {}".format(self.body))
          print("RequestId: {}".format(req.headers.get("RequestId", False)))
          print("JSON-код ответа сервера: \n{}".format(self.u(req.json())))
          break
        else:
          print("Произошла непредвиденная ошибка")
          print("RequestId:  {}".format(req.headers.get("RequestId", False)))
          print("JSON-код запроса: {}".format(self.body))
          print("JSON-код ответа сервера: \n{}".format(self.u(req.json())))
          break

      # Обработка ошибки, если не удалось соединиться с сервером API Директа
      except ConnectionError:
        # В данном случае мы рекомендуем повторить запрос позднее
        print("Произошла ошибка соединения с сервером API")
        # Принудительный выход из цикла
        break

      # Если возникла какая-либо другая ошибка
      except:
        # В данном случае мы рекомендуем проанализировать действия приложения
        print("Произошла непредвиденная ошибка")
        # Принудительный выход из цикла
        break    
    return result

"""
report_name = 'my report'


rep = Report('my report', 'y0_AgAAAABk9T6lAAjeywAAAADdzJ61hWEiGTH4QByPpVBd7L8neEf48jQ')

rep.set_type('CUSTOM_REPORT')
rep.set_data_range('CUSTOM_DATE')
rep.set_period('2023-06-25', '2023-06-26')
rep.set_order_by('Date')

# колонки
rep.add_field('Date')
rep.add_field('CampaignName')
rep.add_field('CampaignId')
rep.add_field('AdGroupId')
rep.add_field('AdId')
rep.add_field('CriteriaId')
rep.add_field('CampaignUrlPath')
rep.add_field('AdNetworkType')
rep.add_field('Impressions')
rep.add_field('Clicks')
rep.add_field('Cost')


rep.add_param('ReportName', 'report python 5')
result = rep.get_report()
data = pd.read_csv(StringIO(result), sep='\t', header=None)

f = 0
data.to_csv(report_name + '.csv', index=None, header=None, mode = 'w')      
"""