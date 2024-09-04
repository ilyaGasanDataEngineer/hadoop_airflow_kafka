# Проект: Импорт данных из PostgreSQL в HDFS с помощью Sqoop и Airflow

## Описание

Этот проект был создан для импорта данных из таблицы базы данных PostgreSQL в HDFS с помощью Apache Sqoop. Чтобы сделать процесс регулярным и автоматизированным, использовался Apache Airflow. Airflow выполняет импорт по расписанию, что позволяет автоматически перемещать данные.

## Основные задачи проекта


1. Автоматизация импорта данных через Apache Airflow. 
2. Проверка результата импорта в HDFS. 
3. Установка и настройка Hadoop и Sqoop. 
4. Настройка подключения к базе данных PostgreSQL и импорт данных с помощью Sqoop.

## Что было установлено

### Hadoop

1. **Скачивание Hadoop**:
   ```bash
   wget https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
   tar -xzvf hadoop-3.3.1.tar.gz
Настройка переменных окружения для Hadoop: Добавил это в файл ~/.bashrc:
bash

export HADOOP_HOME=/path/to/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
Запуск HDFS:
bash

start-dfs.sh
Sqoop
Скачивание и установка Sqoop:

bash

wget https://downloads.apache.org/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
tar -xzvf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
sudo mv sqoop-1.4.7.bin__hadoop-2.6.0 /usr/local/sqoop
Настройка переменных окружения для Sqoop: Добавил это в файл ~/.bashrc:

bash

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
Подключение драйвера PostgreSQL JDBC: Для работы с PostgreSQL добавил драйвер:

bash

wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar
sudo mv postgresql-42.2.23.jar /usr/local/sqoop/lib
PostgreSQL
Установка PostgreSQL:
bash

sudo apt-get install postgresql postgresql-contrib
Создание базы данных и таблицы: Создал таблицу для данных о погоде:
sql

CREATE TABLE api_data_weather (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    city VARCHAR(255),
    timestamp TIMESTAMP
);
Импорт данных с помощью Sqoop
Для импорта данных из PostgreSQL в HDFS, использовал следующую команду:

bash

sqoop import \
--connect jdbc:postgresql://localhost:5432/postgres \
--username postgres \
--password <password> \
--table api_data_weather \
--target-dir /user/hadoop/api_data_weather \
--m 1
Эта команда подключается к PostgreSQL и копирует таблицу api_data_weather в папку HDFS /user/hadoop/api_data_weather.

Проверка результата
После успешного импорта можно посмотреть файлы в HDFS:

bash

hadoop fs -ls /user/hadoop/api_data_weather
Для просмотра данных:

bash
hadoop fs -cat /user/hadoop/api_data_weather/part-m-00000

**Автоматизация через Apache Airflow**
перенесите файл [kafka_airflow.py](dag_wheather/kafka_airflow.py) в вашу папку с dags
после запустите 

далее моежет перезапустить sqoop и проверить ваши данные 