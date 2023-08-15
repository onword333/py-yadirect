import pandas as pd
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json

class Ads:
  reports_endpoint = 'https://api.direct.yandex.com/json/v5/ads'

  body = {}
  selection_criteria = {}
  field_names = []
  params = {}
  headers = {}


  def __init__(self, token):
    self.__token = token
    self.headers = {
      "Authorization": "Bearer " + self.__token,  # OAuth-токен. Использование слова Bearer обязательно
      #"Client-Login": clientLogin,  # Логин клиента рекламного агентства
      "Accept-Language": "ru",  # Язык ответных сообщений.
    }



  def set_criteria(self, key, val):
    self.selection_criteria[key] = val


  def add_field(self, field_name):
    self.field_names.append(field_name)


  def add_param(self, param, val):
    self.params[param] = val


  def u(self, x):
    if type(x) == type(b''):
      return x.decode('utf8')
    else:
      return x


  def get_ads(self):
    self.add_param('SelectionCriteria', self.selection_criteria)
    self.add_param('FieldNames', self.field_names)
    self.body['method'] = 'get'
    self.body['params'] = self.params
    return self.send_data_post()


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
          print("JSON-код запроса: {}".format(u(self.body)))
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


#data = pd.read_csv('my report.csv')
#adids = data[data['CampaignUrlPath'] == '--']['AdId']


"""
ads = Ads('y0_AgAAAABk9T6lAAjeywAAAADdzJ61hWEiGTH4QByPpVBd7L8neEf48jQ')

ads.add_field('Id')
ads.add_field('CampaignId')
ads.set_criteria('Ids', [13297592009, 13297592010])
ads.add_param('TextAdFieldNames', ['Href'])

result = ads.get_ads()

res_json = json.loads(result)

df = pd.json_normalize(res_json['result']['Ads'])

df.info()

"""




