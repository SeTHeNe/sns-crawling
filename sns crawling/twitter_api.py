CONSUMER_KEY = 'A1hcaDS7Gkf9O4QydXNyJf51A'
CONSUMER_KEY_SECRET = 'jeqHZAYvYKswwuWwmP67zB32dh3d2y0gixQycffQeGovtqVhi1'
ACCESS_TOKEN = '197744622-P89YsX5ahVdOLhs6QzHzusAZHKulpmnoNuJX9J0h'
ACCESS_TOKEN_SECRET = 'W8SSCfymaW1XAdhgNOkZOPoJtdbVYNSauPZB3KVEaNJua'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAAEohQEAAAAAsT4uY7Nk9nlk0feKpNsFCdJElIg%3DP0U9jkNGVYUQLxc37uuVS9N2c6sr7R0C8nvJZ26viTECrxGKlU'

HOST = 'capstondb.c4bvboyjjxwz.ap-northeast-2.rds.amazonaws.com'
PORT = 3306
USER = 'admin'
PASSWD = 'pLMAxsbEAuqQtl1BF6wq'
DB = 'knucapston'

import pymysql

def db_connection(_host, _port, user_id, _passwd, db_name):
    db = pymysql.connect(
        host=_host,
        port=_port,
        user=user_id,
        passwd=_passwd,
        db=db_name,
        charset="utf8"
    )

    return db