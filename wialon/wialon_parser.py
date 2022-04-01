import database


class WialonParser:

    def get_l_pack(self, data):
        print('1) Пакет логина: {}'.format(data).rstrip())
        imei = data.split("#")[2].split(";")[1]
        return imei

    def get_b_pack(self, data, imei):
        print('3) Пакет черного ящика: {}'.format(data))
        data = data.strip("b'#B#")
        data = data.split("|")
        count_packet = len(data)-1
        del data[-1]

        write_to_db = False
        for i in data:
            mass = i.split(";")
            raw_data = ", ".join(mass)
            date, time, gps, speed, course, alt, sats, hdop, params = [], [], [], [], [], [], [], [], []
            status, sats_gps, sats_glonass, pwr_ext, pwr_akb, fuel, temp, bootcount = None, None, None, None, None, None, None, None

            date.append(mass[0].strip("b'#B#"))
            if date[0] == "NA":
                date = None
            else:
                date = date[0]
                date = list(date)
                date_day = "".join(date[:2])
                date_month = "".join(date[2:4])
                date_year = "".join(date[4::])
                date_year = int(date_year) + 2000
                date.clear()
                date.append(str(date_year))
                date.append(date_month)
                date.append(date_day)
                date = "-".join(date)

            time.append(mass[1])
            if time[0] == "NA":
                time = None
            else:
                time = time[0]
                time = list(time)
                hours = "".join(time[:2])
                minutes = "".join(time[2:4])
                seconds = "".join(time[4::])
                time.clear()
                time.append(hours)
                time.append(minutes)
                time.append(seconds)
                time = ":".join(time)
            datetime = f"{date} {time}"

            gps.append(mass[2])
            gps.append(mass[4])
            if gps[0] == "NA" or gps[1] == "NA":
                gps = None
            else:
                gps.clear()
                gps.append(str((round(float(mass[2][0:2]), 6) + (round(float(mass[2][2::]) / 60, 6)))))
                gps.append(str((round(float(mass[4][1:3]), 6)) + (round(float(mass[4][3::]) / 60, 6))))
                gps = ", ".join(gps)

            speed.append(mass[6])
            if speed[0] == "NA":
                speed = None
            else:
                speed = speed[0]

            course.append(mass[7])
            if course[0] == "NA":
                course = None
            else:
                course = course[0]

            alt.append(mass[8])
            if alt[0] == "NA":
                alt = None
            else:
                alt = alt[0]

            sats.append(mass[9])
            if sats[0] == "NA":
                sats = None
            else:
                sats = sats[0]

            hdop.append(mass[10])
            if hdop[0] == "NA":
                hdop = None
            else:
                hdop = hdop[0]

            params.append(mass[15].split(","))
            params = params[0]
            # print(params)

            for j in range(0, len(params)):
                if params[j].find("status") != -1:
                    status = params[j][9:]
                    # print("status")
                if params[j].find("sats_gps") != -1:
                    sats_gps = params[j][11:]
                    # print("sats_gps")
                if params[j].find("sats_glonass") != -1:
                    sats_glonass = params[j][15:]
                    # print("sats_glonass")
                if params[j].find("pwr_ext") != -1:
                    pwr_ext = params[j][10:]
                    # print("pwr_ext")
                if params[j].find("pwr_akb") != -1:
                    pwr_akb = params[j][10:]
                    # print("pwr_akb")
                if params[j].find("fuel") != -1:
                    fuel = params[j][8:]
                    # print("fuel")
                if params[j].find("temp") != -1:
                    temp = params[j][8:]
                    # print("temp")
                if params[j].find("bootcount") != -1:
                    bootcount = params[j][12:]

            id_car = database.DataBase().db_select_id_car(imei)
            write_to_db = database.DataBase().db_insert(id_car, imei, datetime, gps, speed, course, alt, hdop,
                                                        sats, sats_glonass, pwr_ext, pwr_akb, fuel, raw_data, status,
                                                        sats_gps, temp, bootcount)
            # if write_to_db:
            #     break

        if write_to_db:
            print("Данные с черного ящика успешно добавлены в БД")
            return count_packet
        else:
            return False

    def get_d_pack(self, data, imei):
        print('Расширенный пакет: {}'.format(data))
        raw_data = data
        data = data.split("#")
        mass = data[2].split(";")
        date, time, gps, speed, course, alt, sats, hdop, params = [], [], [], [], [], [], [], [], []
        status, sats_gps, sats_glonass, pwr_ext, pwr_akb, fuel, temp, bootcount = None, None, None, None, None, None, None, None

        date.append(mass[0].strip("b'#D#"))
        if date[0] == "NA":
            date = None
        else:
            date = date[0]
            date = list(date)
            date_day = "".join(date[:2])
            date_month = "".join(date[2:4])
            date_year = "".join(date[4::])
            date_year = int(date_year) + 2000
            date.clear()
            date.append(date_year)
            date.append(date_month)
            date.append(date_day)
            date = "-".join(date)

        time.append(mass[1])
        if time[0] == "NA":
            time = None
        else:
            time = time[0]
            time = list(time)
            hours = "".join(time[:2])
            minutes = "".join(time[2:4])
            seconds = "".join(time[4::])
            time.clear()
            time.append(hours)
            time.append(minutes)
            time.append(seconds)
            time = ":".join(time)

        datetime = ""
        datetime += date
        datetime += time

        gps.append(str((round(float(mass[2][0:2]), 6) + (round(float(mass[2][2::]) / 60, 6)))))
        gps.append(str((round(float(mass[4][1:3]), 6)) + (round(float(mass[4][3::]) / 60, 6))))

        if gps[0] == "NA" or gps[1] == "NA":
            gps = None
        else:
            gps = ", ".join(gps)

        speed.append(mass[6])
        if speed[0] == "NA":
            speed = None
        else:
            speed = speed[0]

        course.append(mass[7])
        if course[0] == "NA":
            course = None
        else:
            course = course[0]

        alt.append(mass[8])
        if alt[0] == "NA":
            alt = None
        else:
            alt = alt[0]

        sats.append(mass[9])
        if sats[0] == "NA":
            sats = None
        else:
            sats = sats[0]

        hdop.append(mass[10])
        if hdop[0] == "NA":
            hdop = None
        else:
            hdop = hdop[0]

        params.append(mass[15].split(","))
        params = params[0]

        for j in range(0, len(params)):
            if params[j].find("status") != -1:
                status = params[j][9:]
            if params[j].find("sats_gps") != -1:
                sats_gps = params[j][11:]
            if params[j].find("sats_glonass") != -1:
                sats_glonass = params[j][15:]
            if params[j].find("pwr_ext") != -1:
                pwr_ext = params[j][10:]
            if params[j].find("pwr_akb") != -1:
                pwr_akb = params[j][10:]
            if params[j].find("fuel") != -1:
                fuel = int(params[j][8:])
            if params[j].find("temp") != -1:
                temp = int(params[j][8:])
            if params[j].find("bootcount") != -1:
                bootcount = params[j][12:]

        id_car = database.DataBase().db_select_id_car(imei)
        write_to_db = database.DataBase().db_insert(id_car, imei, datetime, gps, speed, course, alt, hdop,
                                                    sats, sats_glonass, pwr_ext, pwr_akb, fuel, raw_data, status,
                                                    sats_gps, temp, bootcount)
        if write_to_db:
            return True

    def answer_l_pack(self, connection):
        server_message = bytes("#AL#1\r\n", encoding="UTF-8")
        connection.sendall(server_message)
        print('2) Ответ отправлен: {}'.format(server_message))

    # Функция ответа на пакет B
    def answer_b_pack(self, connection, count_package):
        server_message = bytes("#AB#{}\r\n".format(count_package), encoding="UTF-8")
        connection.sendall(server_message)
        print('Ответ отправлен: {}'.format(server_message))

    # Функция ответа на пакет D
    def answer_d_pack(self, connection):
        server_message = bytes("#AD#1\r\n", encoding="UTF-8")
        connection.sendall(server_message)
        print('Ответ отправлен: {}'.format(server_message))