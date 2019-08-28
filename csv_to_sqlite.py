import sqlite3
import pandas as pd
import glob
import os


path = 'C:\\Users\\test' #каталог исходных таблиц csv
new_path = path+'_new\\' #новый каталаог для сохранения новых таблиц csv

#созание нового каталога
try:
    os.makedirs(new_path) #создание папки
except OSError:
    pass

#подключение БД
conn = sqlite3.connect('DB.db')  
c = conn.cursor()


for csvFile in glob.glob(os.path.join(path,'*.csv')): #сканирование каталога
    tablename = os.path.splitext(os.path.basename(csvFile))[0] #имя файла - имя таблицы
    df = pd.read_csv (csvFile) #чтение csv файла и создание таблицы
    df['Дата сдачи'] = pd.to_datetime(df['Дата сдачи']) #преобразование столбца в тип data
    df.to_sql(tablename, conn, if_exists='append', index = False) #добавление данных в БД
    sql = """SELECT * FROM %s ORDER BY `Дата сдачи`"""%(tablename) #запрос сортировки по дате сдачи
    c.execute(sql) #выполнение запроса
    res = c.fetchall() #сохранение отсортированной таблицы
    new_df = pd.DataFrame(res,columns=df.columns) #добавление в DataFrame
    new_df.to_csv(new_path+tablename+'_export.csv', index = None, header=True) #создание csv файла из отсортированной таблицы



conn.commit()
c.close()
conn.close()
