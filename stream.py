import requests
import json
import pymysql
import urllib.request

from push_service import (
    get_server_time,
    month,
    twitter_time_url
)
from categorize import categorize
from points import calc_loc
from twitter_api import (
    BEARER_TOKEN,
    HOST,
    PORT,
    USER,
    PASSWD,
    DB,
    db_connection
)

ID_MAX = 999999
UPDATEPERIOD = 10

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    rules = [
        {"value": "비 -방탄 - BTS -RT -is:retweet", "tag": "비"},
        {"value": "눈 -뜨 -RT -is:retweet", "tag": "눈"},
        {"value": "안개 -RT -is:retweet", "tag": "안개"},
        {"value": "미세먼지 -RT -is:retweet", "tag": "미세먼지"},
        {"value": "소나기 -RT -is:retweet", "tag": "소나기"}
    ]
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set, db, cursor):
    
    buf = list()
    buf_size = 0
    sql_rows = []
    
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,geo&expansions=geo.place_id&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type", 
        auth=bearer_oauth, 
        stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            weather = categorize(json_response['data']['text'], json_response['matching_rules'][0]['tag'], json_response['data']['geo'])
            weather._id = int(json_response['data']['id']) % ID_MAX
            weather.set_time(' '.join(json_response['data']['created_at'].replace('.000', '').replace('Z', '').split('T')))
            try:
                coor_0 = json_response['includes']['places'][0]['geo']['bbox'][0]
                coor_1 = json_response['includes']['places'][0]['geo']['bbox'][1]
                coor_2 = json_response['includes']['places'][0]['geo']['bbox'][2]
                coor_3 = json_response['includes']['places'][0]['geo']['bbox'][3]
                coordinate = calc_loc(coor_2, coor_0, coor_3, coor_1)
                location = f'{coordinate[0]}' + ' ' + f'{coordinate[1]}'
                weather.set_location(location)
            except:
                weather.set_location(None)
            try:
                if weather is not None:
                    sql_row = '({}, {}, {}, {}, {})'.format(weather._id, 
                                                            f'"{weather._datetime}"', 
                                                            f'ST_GeomFromText("POINT({weather._location})")', 
                                                            f'"{weather._text}"', 
                                                            weather._tag)
                    sql = "INSERT INTO weatherInfo (weatherInfoId, createTime, location, container, weatherId) VALUES " + sql_row
                    cursor.execute(sql)
                    db.commit()
                    print(f'commited data : {weather._datetime}, {weather._location}, {weather._text}, {weather._tag}')
            except:
                continue

def main():
    weather_db = db_connection(HOST, PORT, USER, PASSWD, DB)
    cursor = weather_db.cursor(pymysql.cursors.DictCursor)

    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set, weather_db, cursor)

    return 0

if __name__ == "__main__":
    main()