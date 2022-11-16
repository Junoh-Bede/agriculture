import pandas as pd
import geopandas as gpd
from datetime import datetime
from multiprocessing import Pool, cpu_count


def agricultural_hours(db_path):
    occupancy_file = f"{db_path}/inputs/technology/archetypes/use_types/AGRICULTURAL.csv"
    occupancy = pd.read_csv(occupancy_file, header=2)
    result = {}
    for day in ['WEEKDAY', 'SATURDAY', 'SUNDAY']:
        day_df = occupancy[occupancy['DAY'] == day]
        result[day] = list(day_df['OCCUPANCY'])
    total_hours = len(result['WEEKDAY']) * 246 + len(result['SATURDAY']) * 52 + len(result['SUNDAY']) * 67
    return total_hours, result


def fill_agriculture(row, usage, area, path, energy_per_sqm):
    total_hours, result = agricultural_hours(path)
    week_day = result['WEEKDAY']
    saturday = result['SATURDAY']
    sunday = result['SUNDAY']
    day = datetime.fromisoformat(row['DATE']).weekday()
    hour = datetime.fromisoformat(row['DATE']).time().hour
    if usage == 'AGRICULTURAL':
        if day in range(5):
            return energy_per_sqm * area / total_hours * week_day[hour]

        elif day == 5:
            return energy_per_sqm * area / total_hours * saturday[hour]

        elif day == 6:
            return energy_per_sqm * area / total_hours * sunday[hour]
        else:
            return 0
    else:
        return 0


def calculate_agriculture_loads(item):
    path = item['path']
    area = item['AREA']
    usage = item['1ST_USE']
    energy_df = pd.read_csv('agriculture.csv')
    energy_df['result'] = energy_df['energy_per_animal'] * energy_df['animal_num'] / energy_df['floor_area_total']
    energy = sum(energy_df['result'])
    data = pd.read_csv(path)
    data['agricultural'] = data.apply(fill_agriculture, axis=1, args=(usage, area, path, energy))
    data.to_csv(path)


def get_building_info(db_path):
    architecture = gpd.read_file(f'{db_path}/inputs/building-properties/architecture.dbf')
    architecture.drop(columns=['geometry'], inplace=True)
    typology = gpd.read_file(f'{db_path}/inputs/building-properties/typology.dbf')
    typology.drop(columns=['geometry'], inplace=True)
    shape = gpd.read_file(f'{db_path}/inputs/building-geometry/zone.shp')
    building = shape.merge(typology, right_on='Name', left_on='Name')
    building = building.merge(architecture, right_on='Name', left_on='Name')
    building['AREA'] = building.area * building['floors_ag'] * building['Hs_ag']
    building = building[['Name', 'AREA', '1ST_USE']]
    return building


def process_agriculture_loads(db_path, energy_type, multi_processing=True):
    check_energy_type(energy_type)
    building_info = get_building_info(db_path)
    building_info['path'] = db_path
    data = list(building_info.transpose().to_dict())
    if multi_processing:
        multi = cpu_count() - 1
        Pool(multi).map(calculate_agriculture_loads, data)
    else:
        for datum in data:
            calculate_agriculture_loads(datum)


def check_energy_type(energy_type):
    if energy_type in ['E', 'NG']:
        return True
    else:
        raise NameError('It is not supported cooking energy type!')


def main():
    energy_type = input('Please input the energy type (NG or E): ')
    db_path = input("Please input CEA scenario path: ")
    multi = input("Are you going to run multiprocessing? (y/n) : ")
    if multi == 'y':
        process_agriculture_loads(db_path, energy_type)
    elif multi == 'n':
        process_agriculture_loads(db_path, energy_type, multi_processing=False)


if __name__ == '__main__':
    main()
