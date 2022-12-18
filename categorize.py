import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn import svm
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
import re
from datetime import datetime
from konlpy.tag import Okt

class weather:
    def __init__(self, id, text, tag, geo):
        self._id = id
        self._text = text
        self._tag = tag
        self._geo = geo

    def __str__(self):
        return f'text : {self._text}\n    tag : {self._tag}\n    geo : {self._geo}\n'

    def set_time(self, timestamp):
        created_at = timestamp.split(' ')
        date = list(map(int, created_at[0].split('-')))
        time = list(map(int, created_at[1].split(':')))
        self._datetime = datetime(date[0], date[1], date[2], (time[0] + 9) % 24, time[1], time[2])

    def set_location(self, location):
        self._location = location

def categorize(response, tag, geo):
    analyzer = Okt()
    weather_tag = {'비' : 1, '눈' : 2, '안개' : 3, '미세먼지' : 4, '소나기' : 5}
    token_list = []

    for item in response.split(' '):
        if item:
            token_list.append(item)

    for item in token_list:
        for text in analyzer.morphs(item, norm = False, stem = True):
            if tag == text:
                data = weather(id, response, weather_tag[tag], geo)
                print(tag)
                return data

    return None