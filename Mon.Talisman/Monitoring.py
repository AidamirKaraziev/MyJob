from flask_cors import CORS
import os
import re

from flask import Flask, render_template, Response, request, jsonify

from Class import DB

app = Flask(__name__)
CORS(app)
db = DB()


@app.route('/connection_test', methods=['GET'])
def col():
    # take = request.get_data()
    # print(take)
    # try:
    #     print(db.select())
    # except:
    #     print('Wrong data')
    # print("timer_end")

    return jsonify('1')


# АПИ по выводу ID и последних COORD
# Все машины и их последние координаты
@app.route('/all_id_car', methods=['POST'])
def fun():
    request_data = request.get_json()
    print(request_data)

    # some_key = request_data
    return db.all_id_car_about()


# АПИ по выводу всей информации за определенное время
@app.route('/all_about_one_car', methods=['POST'])
def fun1():
    request_data = request.get_json()
    print(request_data)

    id_car = request_data['id_car']
    date_start = request_data['date_start']
    date_end = request_data['date_end']
    return db.all_about_one_car(id_car, date_start, date_end)


# АПИ по выводу списка названия видео файлов за промежуток видео
#
@app.route('/video_on_date', methods=['POST'])
def check_list_video():
    request_data = request.get_json()
    id_car = request_data['id_car']
    date_start = request_data['date_start']
    date_end = request_data['date_end']
    return db.list_video(id_car, date_start, date_end)


# АПИ по вывода машрута за определенное время
@app.route('/way', methods=['POST'])
def check_select_way():
    request_data = request.get_json()
    id_car = request_data['id_car']
    date_start = request_data['date_start']
    date_end = request_data['date_end']
    return db.select_coord(id_car, date_start, date_end)


# АПИ ВЫВОДА СТАТУСА СТОП ЗА ПРОМЕЖУТОК ВРЕМЕНИ
@app.route('/status_stop', methods=['POST'])
def check_status():
    request_data = request.get_json()
    id_car = request_data['id_car']
    date_start = request_data['date_start']
    date_end = request_data['date_end']
    return db.select_status_stop(id_car, date_start, date_end)


# АПИ по отображение координат из таблицы "objects"
# Вывод объектов
@app.route('/select_objects', methods=['POST'])
def print_coord():
    request_data = request.get_json()
    print(request_data)
    return db.select_objects()


# АПИ КОТОРОЕ ПАРСИТ FILE_NAME И ОТПРАВЛЯЕТ "1" , НЕ ЗНАЮ ЗАЧЕМ
@app.route('/file_name', methods=['POST'])
def pars():
    request_data = request.get_json()
    file_name = request_data['file_name']
    print(file_name)
    imei = file_name[-19:-4]
    date = file_name[:10]
    print(file_name, imei, date, sep='\n')
    return jsonify('1')


# АПИ Сергея
@app.after_request
def after_request(response):
    response.headers.add('Accept-Ranges', 'bytes')
    return response


def get_chunk(video_name, imei, date, format_file, byte1=None, byte2=None):
    print(video_name)

    full_path = f'C:/share/{imei}/{date}/{format_file}/{video_name}'
    file_size = os.stat(full_path).st_size
    start = 0

    if byte1 < file_size:
        start = byte1
    if byte2:
        length = byte2 + 1 - byte1
    else:
        length = file_size - start

    with open(full_path, 'rb') as f:
        f.seek(start)
        chunk = f.read(length)
    return chunk, start, length, file_size


# АПИ показа видео
@app.route('/<name>')
def get_file0_test(name):
    video_name = name
    imei = video_name[-19:-4]
    date = video_name[:10]
    print(video_name, imei, date, sep='\n')
    format_file = 'video'

    range_header = request.headers.get('Range', None)
    byte1, byte2 = 0, None
    if range_header:
        match = re.search(r'(\d+)-(\d*)', range_header)
        groups = match.groups()

        if groups[0]:
            byte1 = int(groups[0])
        if groups[1]:
            byte2 = int(groups[1])

    chunk, start, length, file_size = get_chunk(video_name, imei, date, format_file, byte1, byte2)
    resp = Response(chunk, 206, mimetype='video/x-matroska',
                    content_type='video/webm', direct_passthrough=True)
    resp.headers.add('Content-Range', 'bytes {0}-{1}/{2}'.format(start, start + length - 1, file_size))
    return resp


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=9999)
