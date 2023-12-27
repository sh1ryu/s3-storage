"""
Этот модуль содержит код для создания и сохранения JSON-файла для S3(Yandex Cloud Storage).
"""
import time
import json
import datetime
import uuid
import sys
import requests
import boto3
from config import API_KEY, SECRET_KEY


def create_file_for_package(package_data):
    """
    Создает и сохраняет JSON-файл для пакета с api.
    
    Аргументы:
        package_data (dict): Данные пакета вашего API.
    """
    try:
        city_name = package_data["name"] # Имя вашей коллекции
        current_time = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
        file_id = str(uuid.uuid4())[:8]
        filename = f"{city_name}_{current_time}_{file_id}.json"

        s3 = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=API_KEY,
            aws_secret_access_key=SECRET_KEY
        )

        bucket_name = 'Ваш бакет'  # Имя вашего бакета
        key = filename

        response = s3.put_object(
            Body=json.dumps(package_data),
            Bucket=bucket_name,
            Key=key
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("JSON-файл успешно сохранен в S3 бакете:", bucket_name)

    except requests.exceptions.RequestException as e:
        print("Произошла ошибка при выполнении запроса к API:", e)

    except KeyboardInterrupt:
        print("Программа завершена.")
        sys.exit()

    except ValueError as e:
        print("Произошла ошибка при сохранении файла в S3:", e)

while True:
    try:
        url = ("Ваш сервер API") # URL вашего сервера API
        api_response = requests.get(url, timeout=5)

        if api_response.status_code == 200:
            package = api_response.json()
            create_file_for_package(package)

    except requests.exceptions.RequestException as exception:
        print("Произошла ошибка при выполнении запроса к API:", exception)

    except KeyboardInterrupt:
        print("Программа завершена.")
        sys.exit()

    time.sleep(1)
