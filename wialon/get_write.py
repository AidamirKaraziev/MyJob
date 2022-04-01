import socket
import psycopg2

import wialon_parser


# создаемTCP/IP сокет
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# задаем адрес
server_address = '192.168.6.75', 20015
print('Старт сервера на {} порт {}'.format(*server_address))

try:
    while True:
        # связываем с клиентским адресом
        sock.bind(server_address)
        sock.listen(1)
        connection, client_address = sock.accept()

        while True:
            pal = connection.recv(1024)  # Принимаем данные и преобразуем их
            if pal:
                data = pal.decode()
                data.strip()

                # Ветвление в зависимости от типа пакета
                if data[0] == "#":
                    if data[1] == "L":  # Пакет логина
                        imei = wialon_parser.WialonParser().get_l_pack(data)
                        wialon_parser.WialonParser().answer_l_pack(connection)

                    if data[1] == "B":  # Пакет черного ящика
                        count_package = wialon_parser.WialonParser().get_b_pack(data, imei)
                        if count_package:
                            wialon_parser.WialonParser().answer_b_pack(connection, count_package)

                    if data[1] == "D":  # Обычный пакет
                        test1 = wialon_parser.WialonParser().get_d_pack(data, imei)
                        if test1:
                            wialon_parser.WialonParser().answer_d_pack(connection)
                else:
                    continue
            else:
                print('Нет данных от:', client_address)
                break


except Exception as _ex:
    print("[INFO] Error while working TCP", _ex)

finally:
    connection.close()
    print("Соединение закрыто")
