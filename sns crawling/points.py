import pandas as pd
from scipy import spatial

def calc_loc(x_1, x_2, y_1, y_2):
    x = (x_1 + x_2) / 2.0
    y = (y_1 + y_2) / 2.0
    return x, y

def read_coor(file_name):
    df = pd.read_excel(file_name, engine='openpyxl', usecols=[0, 3, 4])
    return df

def create_Kdtree(data):
    tree = spatial.cKDTree(data)
    return tree

def nearest_city(city_list, kdtree, x, y):
    index = kdtree.query((x, y))
    city_name = city_list[index[1]]
    return city_name

def korea_coor_data():
    file_name = '.\KNU_KCG\sns crawling\korea_administrative_division_latitude_longitude.xlsx'
    data = read_coor(file_name)
    print(data)

    coor_data = []
    city_data = []

    for row in data.itertuples():
        coor = [row[2], row[3]]
        coor_data.append(coor)
        city_data.append(row[1])

    coor_tree = create_Kdtree(coor_data)

    return coor_tree, city_data

def main():
    coor_data, city_data = korea_coor_data()
    tree = create_Kdtree(coor_data)
    nearest_city(city_data, tree, 128.567863, 35.196876)

if __name__ == "__main__":
    main()