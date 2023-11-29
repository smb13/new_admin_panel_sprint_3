FROM python:3.10

WORKDIR /opt/app

# Скопируйте в контейнер файлы, которые редко меняются
COPY requirements.txt requirements.txt

# Установите зависимости
RUN  pip install --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt

# Скопируйте всё оставшееся. Для ускорения сборки образа эту команду стоит разместить ближе к концу файла.
COPY . .

# Укажите, как запускать ваш сервис
ENTRYPOINT ["python", "main.py"]