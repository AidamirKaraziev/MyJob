import psycopg2
from config import host, user, database, password, port
from geopy.geocoders import Nominatim


# ВЫВОДИТ СПИСОК ВСЕХ АКТУАЛЬНЫХ МАШИН
def select_all_cars():
    with connection.cursor() as select:
        select.execute(
            f"""SELECT id_car
                FROM "all_cars"
                WHERE actual = 'true';""")
        list_all_cars = select.fetchall()
        print(list_all_cars)
    return list_all_cars
    # return select.fetchall()


# ЗДЕСЬ ДОБАВЛЯЕТСЯ В ЯЧЕЙКУ АДРЕСА: ГОРОД, УЛИЦА, ДОМ
def update_address(table_name, add, path):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""UPDATE public."{table_name}"
                    SET address = '{add}'
                    WHERE path_video = '{path}' AND address IS NULL;""")
        print("АПДЕЙТ ПРОИЗОШЕЛ")
        print("ADDRESS", add)
        # print("OTHER", address_1)
    except Exception as select_error:
        print("Ошибка во втором SELECT:", select_error)


# ПРОГРАММА КОТОРАЯ ПРОХОДИТСЯ ПО ВСЕЙ БАЗЕ ДАННЫХ
# ИЩЕТ ТАБЛИЦЫ В КОТОРЫХ ЕСТЬ ВИДЕО, НО НЕТ АДРЕСА И ЗАПИСЫВАТ В НИХ АДРЕС
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
    list_id_car = []

    # ВЫВОДИТ НАЗВАНИЯ ТАБЛИЦ В КОТОРЫХ ЕСТЬ ВИДЕО
    try:

        list_all_cars = select_all_cars()
        print(list_all_cars)
        for id_car in list_all_cars:
            id_car = str(id_car)[1:-2]
            # print(id_car)
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""SELECT path_video
                        FROM "{id_car}"
                        WHERE path_video IS NOT NULL;""")
                path_video = cursor.fetchall()
                print(id_car, path_video)
                if not path_video:
                    pass
                else:
                    list_id_car.append(id_car)
        # СПИСОК ТАЗВАНИЯ ТАБЛИЦ ГДЕ ЕСТЬ ВИДЕОЗАПИСИ
        print(list_id_car)
    except Exception as _list:
        print("1111111111", _list)
    try:
        # ПРОХОДИТСЯ ПО СПИСКУ ТАБЛИЦ ГДЕ ЕСТЬ ВИДЕО И НЕТ АДРЕСА

        for table_name in list_id_car:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""SELECT coord, path_video
                        FROM "{table_name}"
                        WHERE path_video IS NOT NULL AND address IS NULL AND path_video !='1';""")
                video_no_address = cursor.fetchall()
                print(video_no_address)

            for coord_path in video_no_address:
                coord = coord_path[0]
                path = coord_path[1]
                # coord = str(coord)[2:-3]
                print("COORD", coord)
                print("PATH", path)

                # БЛОК В КОТОРОМ ПРОИСХОДИТ ГЕОКОДИРОВАНИЕ
                try:
                    geolocator = Nominatim(user_agent="ajdamir.karaziev@gmail.com123")
                    # location = geolocator.reverse("45.060723484731476, 38.94698711675511")
                    # coord = '45.459924, 39.432997'
                    loc = str(coord)

                    location = geolocator.reverse(loc)
                    raw_location = location.raw
                    print(location.raw)
                    list_good_words = ['state', 'city', 'town', 'suburb', 'quarter', 'road', 'house_number']
                    add = []
                    for word in list_good_words:
                        if word in raw_location["address"]:
                            add.append(raw_location['address'][word])
                            # print("ADD!!!!!!!!", add)
                        else:
                            print('ЛИШНИЙ')
                            continue
                    add = ','.join(add)
                    print(add)
                    update_address(table_name, add, path)

                    # ДОПИСАТЬ ОПТИМИЗИРОВАННЫЙ КОДА ОЧЕНЬ КЛАССНЫЙ И ЛАКОНИЧНЫЙ
                    # add = ["".join(raw_location['address'][word]) if word in raw_location['address'] else
                    #        print("") for word in list_good_words]

                    # СТАРЫЙ, НЕ ОТПИМИЗИРОВАННЫЙ, НО РАБОТАЮЩИЙ КОД.
                    # Я ОСТАВИЛ ЕГО ВДРУГ КОМУ ПРИГОДИТСЯ, НО ЛУЧШЕ УДАЛИТЬ
                    # if 'town' in raw_location['address']:
                    #     address_1 = raw_location['address']['town'], raw_location['address']['road']\
                    #         , raw_location['address']['house_number']
                    #     add = ",".join(address_1)
                    #     update_address(table_name, add, path)
                    # elif 'city' in raw_location['address']:
                    #     address_1 = raw_location['address']['city'], raw_location['address']['road']\
                    #         , raw_location['address']['house_number']
                    #     add = ",".join(address_1)
                    #     update_address(table_name, add, path)
                    # elif 'suburb' in raw_location['address']:
                    #     address_1 = raw_location['address']['suburb'], raw_location['address']['road']\
                    #         , raw_location['address']['house_number']
                    #     add = ",".join(address_1)
                    #     update_address(table_name, add, path)
                    # else:
                    #     address_1 = raw_location['address']['state']
                    #     print("ADDRESS_1", address_1)
                    #     # add = ",".join(address_1)
                    #     add = address_1
                    #     print("ADDRESS", add)
                    #     update_address(table_name, add, path)
                    # print("OTHER", address_1)
                except Exception as geocoder_error:
                    print("[INFO] Ошибка в геокодере: ", geocoder_error)

                # ЗДЕСЬ ДОБАВЛЯЕТСЯ В ЯЧЕЙКУ АДРЕСА: ГОРОД, УЛИЦА, ДОМ
                # try:
                #     with connection.cursor() as cursor:
                #         cursor.execute(
                #             f"""UPDATE public."{table_name}"
                #                 SET address = '{add}'
                #                 WHERE path_video = '{path}' AND address IS NULL;""")
                #     print("АПДЕЙТ ПРОИЗОШЕЛ")
                # except Exception as select_error:
                #     print("Ошибка во втором SELECT:", select_error)
    except Exception as select_error_1:
        print("[INFO] Ошибка в SELECT:", select_error_1)

except Exception as _ex:
    print("[INFO] Ошибки связанные с PostgreSQL", _ex)
finally:
    # if __name__ == "__main__":
    #     app.run(debug=True, host='192.168.6.75')
    if connection:
        connection.close()
        print("[INFO]Подключение завершено")

# 'address': {'building': 'ТП-34', 'road': 'Озёрная', 'city_district': 'Адлерский внутригородской район',
# 'county': 'городской округ Сочи', 'state': 'Краснодарский край', 'region': 'Южный федеральный округ',
# 'country': 'Россия', 'country_code': 'ru'}
