import psycopg2


class DataBase:

    def db_select_id_car(self, imei):  # Выбираем номер машины по imei
        try:
            connection = psycopg2.connect(
                database="First data",
                user="postgres",
                password="2432546Pg",
                host="192.168.6.75",
                port="5433")

            cursor = connection.cursor()
            cursor.execute(f"""SELECT id_car
                            FROM "all_cars"
                            WHERE imei = '{imei}'AND actual = 'true';""")

            id_car = cursor.fetchall()[0][0]
            if not id_car:
                print(F"{imei} МАШИНЫ НЕ ОБНАРУЖЕНО")
                connection.commit()
                connection.close()

            else:
                connection.commit()
                connection.close()
                return id_car

        except Exception as _ex:
            print("[INFO] Ошибка при подключении к БД", _ex)

    def db_insert(self, id_car, imei, datetime, gps, speed, course, alt, hdop, sats, sats_glonass,
                  pwr_ext, pwr_akb, fuel, raw_data, status, sats_gps, temp, bootcount):
        try:
            connection = psycopg2.connect(
                database="First data",
                user="postgres",
                password="2432546Pg",
                host="192.168.6.75",
                port="5433")

            cursor = connection.cursor()
            cursor.execute(f"""INSERT INTO "{id_car}"
                        (imei, date_time, coord, speed, course, alt, variation, sats, sats_glonass, pwr_ext, 
                        pwr_akb, raw_fuel, raw_data, status, sats_gps, temp_s, boot_count)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (imei, datetime, gps, speed, course, alt, hdop, sats, sats_glonass, pwr_ext, pwr_akb,
                                fuel, raw_data, status, sats_gps, temp, bootcount))


            connection.commit()
            connection.close()
            return True

        except Exception as _ex:
            print("[INFO] Ошибка при записи в БД", _ex)

