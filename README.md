# Скрипты для работы с я.директ

## Для чего нужен
Необходимо было получить расходы в разрезе utm меток из рекл. ссылки (распарсить url ссылку), чтобы полученную информацию можно было загрузить в GA4 для оценки рентабельности инвестиций в рекламу.

## Как работает
Предварительно из кабинета я.директ необходимо выгрузить отчеты в формате csv с детализацей "Каждый день", с колонками:
* Дата
* Кампания
* № Кампании
* Группа
* № Группы
* № Объявления
* Условие показа
* № Условия показа
* Тип условия показа
* Тип площадки
* Показы
* Клики
* Расход (руб.)

Положить полученный файл в каталог report и в скрипте указать путь к нему (перем.file_report).

Указать токен и префикc клиента

Запустить create_report_for_ga.py. Скрипт прочитает файл статистики я.директ и в результате сформирует файл csv в формате

префикcклиента_датаначала_датаоконцания_текущеевремя_for_ga.csv

пример: result_2023-07-15_2023-07-25_20_16_for_ga.csv

### Файл статистики из я.директа назначение
Т.к. api директа для "Мастер кампаний" выгружает только до уровня кампаний, то он нужен, чтобы получить ниже (до ключ. слова).