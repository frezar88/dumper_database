import os
import subprocess
import datetime
from mysql.connector import connect

# Параметры подключения к MySQL
host = 'localhost'
user = 'user'
password = 'password'
port = 3306

# Подключение к MySQL
conn = connect(
    host=host,
    user=user,
    password=password,
    port=port
)

# Создание курсора
cursor = conn.cursor()

# Получение списка всех баз данных
cursor.execute("SHOW DATABASES")
databases = cursor.fetchall()

# Текущая дата и время
now = datetime.datetime.now()

# Цикл по базам данных
for database in databases:
    database_name = database[0]
    # Пропускаем системные базы данных, такие как 'information_schema', 'performance_schema', 'sys'
    if database_name in ['information_schema', 'performance_schema', 'sys']:
        continue

    # Создание директории для дампа базы данных
    dump_dir = f'dump/{database_name}'
    os.makedirs(dump_dir, exist_ok=True)

    # Создание команды для создания дампа базы данных
    dump_file = f'{dump_dir}/{database_name}_{now.strftime("%Y%m%d%H%M%S")}.sql'
    dump_command = f'mysqldump -h {host} -P {port} -u {user} -p{password} {database_name} > {dump_file}'

    # Создание дампа базы данных
    subprocess.run(dump_command, shell=True, check=True)

    print(f'Дамп базы данных {database_name} создан в директории {dump_dir}')

    # Удаление дампов старше недели
    for file in os.listdir(dump_dir):
        file_path = os.path.join(dump_dir, file)
        if os.path.isfile(file_path):
            # Получение времени создания файла
            file_created_time = os.path.getctime(file_path)
            # Вычисление разницы между текущим временем и временем создания файла
            time_difference = now.timestamp() - file_created_time
            # Если файл старше недели, удаляем его
            if time_difference > 7 * 24 * 60 * 60:
                os.remove(file_path)
                print(f'Дамп {file} удален, так как он старше недели')

# Закрытие курсора и соединения
cursor.close()
conn.close()
