import os
import sys
import logging
import warnings
import configparser
import pandas as pd
import geopandas as gpd
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 50)
warnings.filterwarnings('ignore')


def get_coordinates(index):
    return [item.bounds[index] for item in df['geometry']]


if __name__ == '__main__':
    logging.basicConfig(filename="file.log",
                        level=logging.INFO,
                        format='%(levelname)s   %(asctime)s   %(message)s')
    logging.info("All setting of the logging is done")

    # read config file
    try:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config.read(config_path)
        shp_path = config['config']['shp_path']
        fc_name = config['config']['fc_name']
        logging.info("Config file read successfully")
    except Exception as e:
        logging.error("Error reading config file: " + str(e))
        sys.exit()

    # read shapefile with geopandas
    data_path = os.path.join(shp_path, fc_name)
    try:
        df = gpd.read_file(data_path)
        logging.info("Shapefile read successfully")
    except Exception as e:
        logging.error("Error reading shapefile: " + str(e))
        sys.exit()

    df['x1'] = get_coordinates(0)
    df['y1'] = get_coordinates(1)
    df['x2'] = get_coordinates(2)
    df['y2'] = get_coordinates(3)
    df.reset_index(inplace=True)
    df.drop('name', axis=1, inplace=True)
    df['x1_occ'] = df.apply(lambda row: (df['x1'] == row['x1']).sum() + (df['x2'] == row['x1']).sum(), axis=1)
    df['same_id'] = [[] for _ in range(len(df))]
    df['same_id'] = df.apply(lambda row: df[(df['x1'] == row['x1']) | (df['x2'] == row['x1'])]['index'].tolist(), axis=1)

    drop_list = []
    for index, row in df.iterrows():
        if row['x1_occ'] == 2:
            df.at[row['same_id'][0], 'geometry'] = df.iloc[row['same_id'][0]]['geometry'].union(df.iloc[row['same_id'][1]]['geometry'])
            drop_list.append(row['same_id'][1])


    df.drop(drop_list, inplace=True)
    df.drop(['index', 'x1', 'y1', 'x2', 'y2', 'x1_occ', 'same_id'], axis=1, inplace=True)

    df['x1'] = get_coordinates(0)
    df['y1'] = get_coordinates(1)
    df['x2'] = get_coordinates(2)
    df['y2'] = get_coordinates(3)
    df.reset_index(inplace=True, drop=True)
    df.reset_index(inplace=True)
    df['x1_occ'] = df.apply(lambda row: (df['x1'] == row['x1']).sum() + (df['x2'] == row['x1']).sum(), axis=1)
    df['same_id'] = [[] for _ in range(len(df))]
    df['same_id'] = df.apply(lambda row: df[(df['x1'] == row['x1']) | (df['x2'] == row['x1'])]['index'].tolist(),
                             axis=1)
    drop_list = []

    for index, row in df.iterrows():
        if row['x1_occ'] == 2:
            df.at[row['same_id'][0], 'geometry'] = df.iloc[row['same_id'][0]]['geometry'].union(
                df.iloc[row['same_id'][1]]['geometry'])
            drop_list.append(row['same_id'][1])

    df.drop(drop_list, inplace=True)
    df.drop(['index', 'x1', 'y1', 'x2', 'y2', 'x1_occ', 'same_id'], axis=1, inplace=True)
    df.to_file(os.path.join(shp_path, 'final.shp'))

    df['x1'] = get_coordinates(0)
    df['y1'] = get_coordinates(1)
    df['x2'] = get_coordinates(2)
    df['y2'] = get_coordinates(3)
    df.reset_index(inplace=True, drop=True)
    df.reset_index(inplace=True)
    df['x1_occ'] = df.apply(lambda row: (df['x1'] == row['x1']).sum() + (df['x2'] == row['x1']).sum(), axis=1)
    df['same_id'] = [[] for _ in range(len(df))]
    df['same_id'] = df.apply(lambda row: df[(df['x1'] == row['x1']) | (df['x2'] == row['x1'])]['index'].tolist(),
                             axis=1)
    drop_list = []

    for index, row in df.iterrows():
        if row['x1_occ'] == 2:
            df.at[row['same_id'][0], 'geometry'] = df.iloc[row['same_id'][0]]['geometry'].union(
                df.iloc[row['same_id'][1]]['geometry'])
            drop_list.append(row['same_id'][1])

    df.drop(drop_list, inplace=True)
    df.drop(['index', 'x1', 'y1', 'x2', 'y2', 'x1_occ', 'same_id'], axis=1, inplace=True)
    df.to_file(os.path.join(shp_path, 'final11.shp'))

    df['x1'] = get_coordinates(0)
    df['y1'] = get_coordinates(1)
    df['x2'] = get_coordinates(2)
    df['y2'] = get_coordinates(3)
    df.reset_index(inplace=True, drop=True)
    df.reset_index(inplace=True)
    df['x1_occ'] = df.apply(lambda row: (df['x1'] == row['x1']).sum() + (df['x2'] == row['x1']).sum(), axis=1)
    df['same_id'] = [[] for _ in range(len(df))]
    df['same_id'] = df.apply(lambda row: df[(df['x1'] == row['x1']) | (df['x2'] == row['x1'])]['index'].tolist(),
                             axis=1)
    drop_list = []

    for index, row in df.iterrows():
        if row['x1_occ'] == 2:
            df.at[row['same_id'][0], 'geometry'] = df.iloc[row['same_id'][0]]['geometry'].union(
                df.iloc[row['same_id'][1]]['geometry'])
            drop_list.append(row['same_id'][1])

    df.drop(drop_list, inplace=True)
    df.drop(['index', 'x1', 'y1', 'x2', 'y2', 'x1_occ', 'same_id'], axis=1, inplace=True)
    df.to_file(os.path.join(shp_path, 'final111.shp'))

    df['x1'] = get_coordinates(0)
    df['y1'] = get_coordinates(1)
    df['x2'] = get_coordinates(2)
    df['y2'] = get_coordinates(3)
    df.reset_index(inplace=True, drop=True)
    df.reset_index(inplace=True)
    df['x1_occ'] = df.apply(lambda row: (df['x1'] == row['x1']).sum() + (df['x2'] == row['x1']).sum(), axis=1)
    df['same_id'] = [[] for _ in range(len(df))]
    df['same_id'] = df.apply(lambda row: df[(df['x1'] == row['x1']) | (df['x2'] == row['x1'])]['index'].tolist(),
                             axis=1)
    drop_list = []

    for index, row in df.iterrows():
        if row['x1_occ'] == 2:
            df.at[row['same_id'][0], 'geometry'] = df.iloc[row['same_id'][0]]['geometry'].union(
                df.iloc[row['same_id'][1]]['geometry'])
            drop_list.append(row['same_id'][1])

    df.drop(drop_list, inplace=True)
    df.drop(['index', 'x1', 'y1', 'x2', 'y2', 'x1_occ', 'same_id'], axis=1, inplace=True)


    df.to_file(os.path.join(shp_path, 'final1111.shp'))






