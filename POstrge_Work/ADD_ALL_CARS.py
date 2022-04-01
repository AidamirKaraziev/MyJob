# ДОБАВЛЕНИЕ НОВЫХ МАШИН В БАЗУ ДАННЫХ

import psycopg2
from config import host, user, database, password, port
from flask import Flask, g, jsonify, request
from flask_cors import CORS
import json
import requests

try:

    # Подключение к базк данных
    connection = psycopg2.connect(
        host=host,
        user=user,
        database=database,
        password=password,
        port=port
    )
    connection.autocommit = True
    # imei = '862531049109565'
    imei = input("Imei: ")
    # ВАЖНО !!!
    # ТОЛЬКО АНГЛИЙСКИЕ СИМВОЛЫ
    # car_number = 'K073BO193'
    car_number = input("Car_number: ")

    # Добавление данных в таблицу "ALL_CARS"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""INSERT INTO "all_cars" (imei, car_number)
                VALUES('{imei}','{car_number}')
            ;"""
        )

    # Показывает ID машины
    with connection.cursor() as select:
        select.execute(
            f"""SELECT id_car 
                FROM "all_cars"
                WHERE imei = '{imei}' AND car_number = '{car_number}'
            ;"""
        )
        id_car = select.fetchall()
        # id_car = select.fetchone()
        print(id_car[0][0])
        # id_car = id_car[0]
        # print(id_car)
        # print(type(id_car))
        if id_car == []:
            print("Такого ID_CAR нет!")
        print(id_car[0][0])
        id_car = id_car[0][0]
        # СОЗДАНИЕ ТАБЛИЦЫ ТАРИРОВКИ "1013_tarirovka"
        with connection.cursor() as create:
            create.execute(
                f"""CREATE TABLE "{id_car}_tarirovka"
                (voltage smallint,
                litre smallint
                )""")

        # Создание таблицы по ID_CAR
        with connection.cursor() as create:
            create.execute(
                f"""CREATE TABLE "{id_car}"
            (
    imei varchar (50),
    date_time timestamp,
    date_now timestamp default current_timestamp,
    coord varchar (100),
    address text,
    speed smallint,
    course smallint,
    alt smallint,
    variation real,
    sats smallint,
    sats_glonass smallint,
    pwr_ext real,
    pwr_akb real,
    raw_fuel smallint,
    ready_fuel smallint,
    status_stop varchar (50),
    raw_data text,
    status varchar(50),
    sats_gps smallint,
    temp_s smallint,
    boot_count smallint,
    path_video text
            )
            ;

            ALTER TABLE IF EXISTS "{id_car}"
                OWNER to postgres;"""
            )
        print("Таблица успешно создана")

    print(F"[INFO] Данные были успешно добавлены")

except Exception as _ex:
    # print("[INFO] Ошибки связанные с PostgreSQL", _ex)
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO]Подключение завершено")


# """ car_number varchar(50)"""
