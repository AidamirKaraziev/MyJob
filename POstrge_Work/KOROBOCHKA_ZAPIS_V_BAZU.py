import os
import psycopg2
from config import host, user, database, password, port
from flask import Flask, jsonify, request
from flask_cors import CORS
# import json
# import requests


# ПАРСЕР RAW_DATA
def parser_data_in_korobochka(ready_data):
    imei = ready_data[0]
    date_time = ready_data[1]
    coord = ready_data[2]
    speed = ready_data[3]
    name_video = ready_data[4]
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT id_car 
                FROM "all_cars"
                WHERE imei = '{imei}'AND actual = 'true';""")

        id_car = cursor.fetchone()
        id_car = id_car[0]
        return id_car, imei, date_time, coord, speed, name_video


try:
    app = Flask(__name__)
    CORS(app)
    # Подключение к базк данных
    connection = psycopg2.connect(
        host=host,
        user=user,
        database=database,
        password=password,
        port=port
    )
    connection.autocommit = True


    @app.route('/connection_test', methods=['GET'])
    def test():
        return "1"


    @app.route('/test_1', methods=['POST'])
    def fun():
        request_data = request.get_json()
        print(request_data)
        return "Спасибо"

    # СОХРАНЯЕТ В БАЗУ ДАННЫХ ВСЕ ДАННЫЕ И ПАРСИТ ИХ
    @app.route('/raw_data', methods=['POST'])
    def raw_data_1():
        print("СТАРТ ПАРСЕРА RAW DATA")
        # ДАННЫЕ ПРИХОДЯТ В БАЙТОВОМ ЗНАЧЕНИИ, ПЕРЕВОД
        raw_data = request.data
        # print(raw_data)
        raw_data = bytes.decode(raw_data, encoding="utf-8")
        # print(raw_data)

        # РАЗДЕЛЕНИЕ ПО ТОЧКЕ С ЗАПЯТОЙ
        raw_data = raw_data.split(";")
        print("RAW DATA", raw_data)
        raw_data_s = ";".join(raw_data)

        ready_data = []
        for element in raw_data:
            element = element.strip()
            element.replace('\n', '')
            if element == "null":
                element = None
                print("ПУСТОЙ ЭЛЕМЕНТ", element)
                ready_data.append(element)

            ready_data.append(element)
        id_car, imei, date_time, coord, speed, name_video = parser_data_in_korobochka(ready_data)

        # ЕСЛИ ID_CAR НЕТ В БАЗЕ
        if not id_car:
            print(F"{id_car} МАШИНЫ НЕ ОБНАРУЖЕНО")

        # ЕСЛИ ТАКАЯ ТАБЛИЦА ЕСТЬ ЗАПОЛНИТЬ ЕЕ
        else:
            with connection.cursor() as insert:
                insert.execute(
                    f"""INSERT INTO "{id_car}" (imei, date_time, coord, speed, path_video, raw_data)
                     VALUES(%s, %s, %s, %s, %s, %s);""", (imei, date_time, coord, speed, name_video, raw_data_s))
            print("[INFO] Данные были успешно добавлены")
            return jsonify("Данные были успешно добавлены")

    # СОХРАНЯЕТ ВИДЕО В ПАПКУ ПО IMEI
    @app.route("/video_file_<name>", methods=['GET', 'POST'])
    def file_video(name):
        try:
            data = request.files['video_file'].read()
            # 2022-02-21-07-33-16_234567898765432.mkv
            print(name)
            imei = name[-19:-4]
            date = name[:10]

            # СОЗДАЮТСЯ ПАПКИ НА ОСЕНОВАНИИ IMEI
            if not os.path.isdir(f"/share/{imei}"):
                os.mkdir(f"/share/{imei}")
                print("ПАПКА С ИМЕЕМ СОЗДАНА")

            # СОЗДАЮТСЯ ПАПКИ НА ОСНОВАНИИ ДАТЫ
            if not os.path.isdir(f"/share/{imei}/{date}"):
                os.mkdir(f"/share/{imei}/{date}")
                os.mkdir(f"/share/{imei}/{date}/video")
                os.mkdir(f"/share/{imei}/{date}/photo")
                print("Папки созданы")

            # ЗАПИСЫВАЕМ ВИДЕО В УЖЕ ГОТОВЫЕ ПАПКИ
            with open(f"C:/share/{imei}/{date}/video/{name}", mode="wb") as new:
                new.write(data)

            return jsonify(1)
        except:
            return jsonify(0)

    # СОХРАНЯЕТ ФОТО В ПАПКУ ПО IMEI
    @app.route("/photo_file_<name>", methods=['GET', 'POST'])
    def file_photo(name):
        try:
            data = request.files['photo_file'].read()
            print(name)
            imei = name[20:-4]
            date = name[:10]

            with open(f"C:/share/{imei}/{date}/photo/{name}", mode="wb") as new:
                new.write(data)

            return jsonify(1)
        except:
            return jsonify(0)

except Exception as _ex:
    print("[INFO] Ошибки связанные с PostgreSQL", _ex)
    # print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if __name__ == "__main__":
        app.run(debug=True, host='192.168.15.9', port=10010)
    if connection:
        connection.close()
        print("[INFO]Подключение завершено")
