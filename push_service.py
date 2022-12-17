import firebase_admin
from firebase_admin import credentials
from firebase_admin import messaging
from datetime import datetime, timedelta
from multiprocessing import Pool
import urllib.request
import time
import pymysql
import firebase_admin
from firebase_admin import credentials
from points import (
    korea_coor_data,
    nearest_city
)
from twitter_api import (
    PORT,
    DB,
    HOST,
    db_connection
)

weather_tag = {
    1 : '비',
    2 : '눈',
    3 : '안개',
    4 : '미세먼지',
    5 : '소나기'
}

twitter_time_url = "https://twitter.com"

month = {
    'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6,
    'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12
}

FCM_CRED_PATH = '.\\res\\knu-capston-firebase-adminsdk-e1d14-3013d9752e.json'

USERADMIN           = 'userAdmin'
USERADMINPASS       = 'userAdminPass'
WEATHERADMIN        = 'getWeatherInfo'
WEATHERADMINPASS    = 'getWeatherInfoPass'
UPDATEPERIOD        = 5
TRIGGER             = 3
TIMEOUT             = 60 * 5
GUARD               = 20

def get_server_time(url):
    date = urllib.request.urlopen(url).headers['Date'][5:-4]
    d, m, y, hour, min, sec = int(date[:2]), month[date[3:6]], int(date[7:11]), int(date[12:14]), int(date[15:17]), int(date[18:])
    timestamp = datetime(y, m, d, hour + 9, min, sec)
    return timestamp

def send_push_data(message_title, message_body, data_message, registration_ids):
    message = messaging.MulticastMessage(
        #data=data_message,
        notification=messaging.Notification(
            title=message_title,
            body=message_body
        ),
        tokens=registration_ids
    )
    response = messaging.send_multicast(message)
    if response.failure_count > 0:
        responses = response.responses
        failed_tokens = []
        for idx, resp in enumerate(responses):
            if not resp.success:
                # The order of responses corresponds to the order of the registration tokens.
                failed_tokens.append(registration_ids[idx])
        print('List of tokens that caused failures: {0}'.format(failed_tokens))

def construct_push_data(weather_info):
    title = '날씨 알림'
    body = f'{weather_info["city_name"]} 지역에 {weather_tag[weather_info["weather_tag"]]} 보고'
    data = dict()

    for i in range(len(weather_info['data'])):
        created_at  = str(weather_info["data"][i][0])
        position    = f'{weather_info["data"][i][1][0]} {weather_info["data"][i][1][0]}'
        text        = weather_info["data"][i][2]

        data.update({f'data_{i}':{'created_at'  :created_at,
                                  'position'    :position,
                                  'text'        :text}})

    return title, body, data

def update_weather_info(weather_list, db_cursor, city_list, kdtree, begin, end):
    print(f'begin : {begin - timedelta(seconds=GUARD)}, end : {end}')
    sql = f"SELECT weatherInfoId, createTime, ST_X(location) as longitude, ST_Y(location) as latitude, container, weatherId FROM weatherInfo WHERE createTime >= '{begin - timedelta(seconds=GUARD)}'"
    db_cursor.execute(sql)
    rows = db_cursor.fetchall()
    updated_city = []

    for row in rows:
        flag = False
        info_id = row['weatherInfoId']
        timestamp = row['createTime']
        position = (float(row['longitude']), float(row['latitude']))
        text = row['container']
        weather_id = row['weatherId']
        if begin - timedelta(seconds=GUARD) >= timestamp:
            continue
        city_name = nearest_city(city_list, kdtree, position[0], position[1])
        if city_name in weather_list:
            if weather_id in weather_list[city_name]:
                for item in weather_list[city_name][weather_id]:
                    if info_id in item:
                        flag = True
                        break
                if flag is False:
                    try:
                        weather_list[city_name][weather_id].append([timestamp, position, text, weather_id, info_id])
                    except:
                        weather_list[city_name].update({weather_id:[[timestamp, position, text, weather_id, info_id]]})
        else:
            weather_list.update({city_name:{weather_id:[[timestamp, position, text, weather_id, info_id]]}})
        if city_name not in updated_city and flag is False:
            updated_city.append(city_name)
            print(f'updated information : {city_name}, {timestamp}, {position}, {text}, {weather_tag[weather_id]}')

    return updated_city

def get_user_list(db_cursor, city_list, kdtree):
    sql = 'SELECT userToken, created_at, ST_X(position) as longitude, ST_Y(position) as latitude FROM location'
    db_cursor.execute(sql)
    rows = db_cursor.fetchall()
    user_list = dict()

    for row in rows:
        token = row['userToken']
        created_at = row['created_at']
        city_name = nearest_city(city_list, kdtree, float(row['longitude']), float(row['latitude']))
        index = 0

        if city_name in user_list:
            flag = False
            new_ver = False
            for user in user_list[city_name][2]:
                if token in user:
                    if created_at > user[1]:
                        user_list[city_name][2][index][1] = created_at
                        flag = True
                        break
            if flag is False:
                user_list[city_name][2].append([token, created_at])
        else:
            user_list.update({city_name:[False, datetime.now(), [[token, created_at]]]})

    return user_list

def update_user_list(db_cursor, user_list, city_list, kdtree):
    sql = 'SELECT userToken, created_at, ST_X(position) as longitude, ST_Y(position) as latitude FROM location'
    db_cursor.execute(sql)
    rows = db_cursor.fetchall()

    for row in rows:
        token = row['userToken']
        created_at = row['created_at']
        city_name = nearest_city(city_list, kdtree, float(row['longitude']), float(row['latitude']))

        if city_name in user_list:
            flag = False
            new_ver = False
            index = 0
            for user in user_list[city_name][2]:
                if token in user:
                    flag = True
                    if created_at > user[1]:
                        new_ver = True
                index += 1
            if flag is False:
                user_list[city_name][2].append([token, created_at])
            elif flag is True and new_ver is False:
                continue
            else:
                user_list[city_name][2][index][1] = created_at
            print(f'user updated : {city_name}, {token}, {created_at}')
        else:
            user_list.update({city_name:[False, datetime.now(), [[token, created_at]]]})
            print(f'user updated : {city_name}, {token}, {created_at}')

def main():
    cred = credentials.Certificate(FCM_CRED_PATH)
    firebase_admin.initialize_app(cred)

    user_db = db_connection(HOST, PORT, USERADMIN, USERADMINPASS, DB)
    user_cursor = user_db.cursor(pymysql.cursors.DictCursor)

    kdtree, city_list = korea_coor_data()

    user_list = get_user_list(user_cursor, city_list, kdtree)
    user_db.close()
    for city_name, users in user_list.items():
        for user in users[2]:
            print(f'user_list : {city_name}, {user[0]}, {user[1]}')
    weather_list = dict()

    timestamp = get_server_time(twitter_time_url)
    cur_timestamp = get_server_time(twitter_time_url)

    while True:
        user_db = db_connection(HOST, PORT, USERADMIN, USERADMINPASS, DB)
        user_cursor = user_db.cursor(pymysql.cursors.DictCursor)

        weather_db = db_connection(HOST, PORT, WEATHERADMIN, WEATHERADMINPASS, DB)
        weather_cursor = weather_db.cursor(pymysql.cursors.DictCursor)

        time.sleep(UPDATEPERIOD)
        cur_timestamp = get_server_time(twitter_time_url)
        updated = update_weather_info(weather_list, weather_cursor, city_list, kdtree, timestamp, cur_timestamp)
        update_user_list(user_cursor, user_list, city_list, kdtree)

        user_db.close()
        weather_db.close()

        for city in updated:
            city_weather = weather_list[city]
            for i in range(len(weather_tag)):
                try:
                    print(*city_weather[i + 1], sep='\n')
                    for j in range(len(city_weather[i + 1])):
                        if (cur_timestamp - city_weather[i + 1][0][0]).seconds >= TIMEOUT:
                            print(weather_list[city][i + 1].pop(0))
                        else:
                            break
                    if len(city_weather[i + 1]) >= TRIGGER:
                        weather_info = {
                            'city_name'     : city,
                            'weather_tag'   : i + 1,
                            'data'          : []
                        }
                        try:
                            if user_list[city][0] is False or (user_list[city][0] is True and (cur_timestamp - user_list[city][1]).seconds >= TIMEOUT):
                                user_list[city][1] = cur_timestamp
                                push_list = []
                                for user in user_list[city][2]:
                                    push_list.append(user[0])
                                user_list[city][0] = True
                                for weather in city_weather[i + 1]:
                                    weather_info['data'].append([weather[0], weather[1], weather[2], weather[3]])
                        except:
                            print(f'{city} user does not exist')
                        title, body, data = construct_push_data(weather_info)
                        try:
                            send_push_data(title, body, data, push_list)
                        except (AuthenticationError, FCMServerError, InvalidDataError, InternalPackageError) as e:
                            print(e)

                except:
                    print(f'{weather_tag[i + 1]} data does not exist\n')

        timestamp = get_server_time(twitter_time_url)

    return 0

if __name__ == "__main__":
    main()