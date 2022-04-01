# import time

from flask import Flask, g, jsonify, request
from geopy.geocoders import Nominatim
import datetime
import psycopg2

# import db

app = Flask(__name__)


def connect_db():
    return psycopg2.connect(
        host="192.168.6.75",
        user="postgres",
        password="2432546Pg",
        database="First data",
        port="5433"  # 5433 было
        # cursorclass=psycopg2.cursors.DictCursor
    )


def get_db():
    """Opens a new database connection per request."""
    if not hasattr(g, 'db'):
        g.db = connect_db()
    return g.db


@app.teardown_appcontext
def close_db(error):
    """Closes the database connection at the end of request."""
    if hasattr(g, 'db'):
        g.db.close()


class DB:

    # Метод по выводу всех ID и последних COORD
    # Все машины и их последние координаты
    @staticmethod
    def all_id_car_about():
        time_1 = datetime.datetime.now()
        data = []
        try:
            # Функция по выводу всех Айди
            with get_db().cursor() as all_cars:
                all_cars.execute(
                    f"""SELECT id_car, car_number, brand_model, type_of_machine
                        FROM "all_cars"
                        WHERE actual = 'true' 
                        """)

                list_car = []
                all_cars = all_cars.fetchall()
                for i in range(len(all_cars)):
                    list_car.append(all_cars[i][0])
                for s in range(len(list_car)):
                    list_car[s] = '"' + str(list_car[s]) + '"'
                    data1 = []
                    # Функция по выводу последних координат
                    with get_db().cursor() as select_all:
                        select_all.execute(
                            f"""SELECT coord, speed, raw_fuel, date_time
                            FROM {list_car[s]} 
                            WHERE coord != '_____'
                            ORDER BY date_time DESC;
                            """)

                        select = select_all.fetchone()
                        # print("SELECT", select)
                        if not select:
                            select = ["Пусто"]
                            data1.extend(all_cars[s])
                            data1.extend(select)
                            data.append(data1)

                            # print("удалось")
                        else:
                            select1 = select[-1]
                            # print(str(select1))

                            # print("SELECT!!!!!", select)
                            data1.extend(all_cars[s])

                            data1.extend(select[0:-1])
                            # дата и время
                            data1.append(str(select[-1]))
                            data.append(data1)
                print(data)

                time_2 = datetime.datetime.now()
                delta_time = time_2 - time_1
                print("ВРЕМЯ: ", delta_time)
                return jsonify(data)
        except Exception as _ex:
            print("Ошибка в all_id_car ", _ex)

    # Метод вывода всех данных по одной машине за определенный промежуток времени
    @staticmethod
    def all_about_one_car(id_car, date_start, date_end):
        try:
            with get_db().cursor() as data_for_this_time:
                data_for_this_time.execute(
                    f"""SELECT coord, date_time
                       FROM "{id_car}"
                       WHERE date_time > '{date_start}' AND date_time < '{date_end}'
                       LIMIT 20
                       """)
                data_for_this_time = data_for_this_time.fetchall()
                # print("ddddddddddddddd", str(data_for_this_time[0][1]))
                # print("ddddddddddddddd", str(data_for_this_time[1]))

                return jsonify(data_for_this_time)
        except Exception as _ex:
            print("ОШИБКА_1", _ex)

    # Метод вывода списка названия видео файлов за промежуток времени
    @staticmethod
    def list_video(id_car, date_start, date_end):
        list_data = []
        try:
            with get_db().cursor() as all_video:
                all_video.execute(
                    f"""SELECT coord, path_video, address, date_time 
                        FROM "{id_car}"
                        WHERE date_time > '{date_start}' AND date_time < '{date_end}' AND path_video IS NOT NULL 
                        AND path_video != '1'
                        ORDER BY date_time ASC;""")
                all_video = all_video.fetchall()
                for data in all_video:
                    date_time = str(data[3])
                    data_s = list(data[:3])
                    data_s.append(date_time)
                    # print(data_s)
                    list_data.append(data_s)
            return jsonify(id_car, list_data)
        except Exception as _ex:
            print("[INFO]", _ex)

    # ВЫВОД МАРШРУТА
    @staticmethod
    def select_coord(id_car, date_start, date_end):
        try:
            with get_db().cursor() as select_way:
                select_way.execute(
                    f"""SELECT coord
                        FROM "{id_car}"
                        WHERE date_time > '{date_start}' AND date_time < '{date_end}'
                        ORDER BY date_time ASC;""")
                select_way = select_way.fetchall()
                return jsonify(select_way)
        except Exception as _ex:
            print("[МАРШРУТ]", _ex)

    # ВЫВОД СТАТУСА СТОП
    @staticmethod
    def select_status_stop(id_car, date_start, date_end):
        try:
            with get_db().cursor() as select_status_stop:
                select_status_stop.execute(
                    f"""SELECT status_stop
                        FROM "{id_car}"
                        WHERE status_stop IS NOT NULL AND date_time > '{date_start}' AND date_time < '{date_end}'
                        ORDER BY date_time ASC;""")
                select_status_stop = select_status_stop.fetchall()
                return jsonify(select_status_stop)
        except Exception as _ex:
            print('[СТАТУС СТОП]', _ex)

    # Вывод объектов
    @staticmethod
    def select_objects():
        try:
            with get_db().cursor() as select_objects:
                select_objects.execute(
                    f"""SELECT *
                        FROM "objects" """)
                select_objects = select_objects.fetchall()
                return jsonify(select_objects)
        except Exception as _ex:
            print("[SELECT OBJECTS]", _ex)
