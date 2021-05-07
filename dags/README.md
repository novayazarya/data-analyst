# Установка Apache Airflow

Apache Airflow свои настройки хранит в файле airflow.cfg, который по умолчанию будет создан в домашней директории юзера по пути ~/airflow/airflow.cfg

Домашний каталог airflow по-умолчанию `~/airflow`
Путь можно изменить, присвоив переменной окружения новое значение:

`export AIRFLOW_HOME=~/airflow`

Пример скрипта установки

```sh
AIRFLOW_VERSION=2.0.1
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```
## Инициализация базы данных

В качестве базы данных рекомендуется использовать PostgreSQL или MySQL.
Создаю базу данных и пользователя к ней для Airflow:

```postgresql
postgres=# create database airflow_metadata;
CREATE DATABASE
postgres=# CREATE USER airflow WITH password 'airflow';
CREATE ROLE
postgres=# grant all privileges on database airflow_metadata to airflow;
GRANT
```

А теперь открываем airflow.cfg и правим значение параметра `sql_alchemy_conn` на `postgresql+psycopg2://airflow:airflow@localhost/airflow_metadata` и `load_examples = False`.

Инициализируем новую базу данных:

```bash
airflow db init
```
## Веб-приложение

Запускаем веб-приложение на 8080 порту:

```bash
airflow webserver -p 8080
```
Создаем супер-юзера для управления задачами:

```bash
airflow users create \
    --username admin \
    --firstname Mikhail \
    --lastname Popov \
    --role Admin \
    --email me@popov.mn
```
## Планировщик

Запустить планировщик можно командой:

```bash
airflow scheduler
```
Ключ -D позволяет запустить  планировщик в режиме демона.

## Задачи

В файле настроек airflow.cfg есть параметр `dags_folder`, он указывает на путь, где лежат файлы с DAGами. Это путь `$AIRFLOW_HOME/dags`. Именно туда мы положим наш код с задачами.
