import psycopg2
from config import host, user, database, password, port


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
    # ПРОГРАММА ПО СОЗДАНИЮ СПИСКА СООТВЕТСВИЙ МЕЖДУ НАПРЯЖЕНИЕМ В ДУТ И ЛИТРАМИ В БАКЕ

    s = {1: 0, 2: 10, 33: 20, 63: 30, 93: 40, 122: 50, 151: 60, 178: 70, 207: 80, 236: 90, 264: 100, 293: 110, 320: 120,
         349: 130, 377: 140, 406: 150,
         434: 160, 461: 170, 492: 180, 517: 190, 545: 200, 572: 210, 599: 220, 627: 230, 655: 240, 684: 250, 710: 260,
         738: 270, 767: 280, 795: 290,
         824: 300, 852: 310, 881: 320, 910: 330, 939: 340, 971: 350, 982: 353}

    list_key = []
    list_value = []
    temp_key = 0
    temp_v = 0
    konechka = 0
    # print(s[1])
    # Функция создания таблицы соответсвия ДУТ и ЛИТРОВ топлива
    for key, value in s.items():
        # print("#" * 30)
        # print("KEY", key, "VALUE", value)
        # print("VALUE", value)
        # Итерации по ключам от нижней границы до верхней
        for i in range(temp_key + 1, key + 1):
            list_key.append(i)
            # print(value - temp_v)
            # print(key - temp_key)
            key_difference = key - temp_key
            value_difference = value - temp_v
            # print(i)
            matem = value_difference / key_difference

            # print("matem", matem)
            # Взятие нижней границы в VALUE в этой итерации lower_limit
            lower_limit = value - value_difference

            # print("F", lower_limit)

            # прибавление к нижней границе величины шага
        for g in range(key_difference):
            lower_limit += matem
            # print(lower_limit)
            list_value.append(round(lower_limit, 1))

        temp_v = value
        temp_key = key

    print("СПИСОК ЛИТРОВ: ", list_value)
    print("СПИСОК НАПРЯЖЕНИЙ: ", list_key)
    # print(len(list_value))
    # print(len(list_key))

    print(list_value[981])
    for f in range(len(list_value)):
        voltage = list_key[f]
        litre = list_value[f]
        # print(voltage, ":", litre)

    # Добавление данных в таблицу тарировки
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO "TARIROVKA_1" (voltage, litre)
                VALUES ({voltage}, {litre})"""
            )


except Exception as _ex:
    # print("[INFO] Ошибки связанные с PostgreSQL", _ex)
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO]Подключение завершено")


# ЭТО СЕЛЕКТ
# SELECT "1013".coord, "TARIROVKA".litre
# FROM "1013" INNER JOIN "TARIROVKA"
# ON "1013".raw_fuel = "TARIROVKA".voltage

# ЭТО АПДЕЙТ
# UPDATE "1013"
# SET "ready_fuel" = "TARIROVKA_1".litre
# FROM "TARIROVKA_1"
# WHERE "1013".raw_fuel = "TARIROVKA_1".voltage;

# КАК БУДЕТ ВЫГЛЯДЕТЬ КОНЕЧНЫЙ ВАРИАНТ
#     id_car = 1013
#     f"""
#     UPDATE "{id_car}"
#     SET "ready_fuel" = "{id_car}_tarirovka".litre
#     FROM "{id_car}_tarirovka"
#     WHERE "{id_car}".raw_fuel = "{id_car}_tarirovka".voltage;
#     """
