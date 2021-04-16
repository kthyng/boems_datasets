from search.ErddapReader import ErddapReader
import pandas as pd
import numpy as np
from datetime import datetime
import xarray as xr


def test_station_ioos_1dataset_id_alltime():
    reader = ErddapReader(known_server='ioos')
    station = reader.stations(dataset_ids='noaa_nos_co_ops_8771013')
    assert station.kw == {'min_time': '1900-01-01', 'max_time': '2100-12-31'}
    assert station.dataset_ids == ['noaa_nos_co_ops_8771013']

def test_station_ioos_1dataset_id():
    reader = ErddapReader(known_server='ioos')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2'}
    station = reader.stations(dataset_ids='noaa_nos_co_ops_8771013', kw=kw)
    assert station.kw == {'min_time': '2019-1-1', 'max_time': '2019-1-2'}
    assert isinstance(station.meta, pd.DataFrame)
    assert isinstance(station.data['noaa_nos_co_ops_8771013'], pd.DataFrame)
    assert (list(station.data['noaa_nos_co_ops_8771013'].index[[0,-1]].sort_values().values) == 
                       [np.datetime64('2019-01-01T00:00:00.000000000'),
 np.datetime64('2019-01-02T00:00:00.000000000')])
    
def test_station_ioos_2dataset_ids():
    reader = ErddapReader(known_server='ioos')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2'}
    dataset_ids = ['noaa_nos_co_ops_8771013', 'noaa_nos_co_ops_8774230']
    stations = reader.stations(dataset_ids=dataset_ids, kw=kw)
    assert stations.dataset_ids == dataset_ids
    assert not stations.meta.empty
    assert not stations.data['noaa_nos_co_ops_8771013'].empty
    assert not stations.data['noaa_nos_co_ops_8774230'].empty
    
def test_station_ioos_1station():
    reader = ErddapReader(known_server='ioos')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2'}
    stationname = '8771013'
    stations = reader.stations(stations=stationname, kw=kw)
    assert stations.dataset_ids == ['noaa_nos_co_ops_8771013']
    assert not stations.meta.empty
    assert not stations.data['noaa_nos_co_ops_8771013'].empty

def test_station_ioos_2stations():
    reader = ErddapReader(known_server='ioos')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2'}
    dataset_ids = ['noaa_nos_co_ops_8771013', 'noaa_nos_co_ops_8774230']
    stationnames = ['8771013', '8774230']
    stations = reader.stations(stations=stationnames, kw=kw)
    assert sorted(stations.dataset_ids) == dataset_ids
    assert not stations.meta.empty
    assert not stations.data['noaa_nos_co_ops_8771013'].empty
    assert not stations.data['noaa_nos_co_ops_8774230'].empty

def test_region_coastwatch():
    reader = ErddapReader(known_server='coastwatch')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2',
         'min_lon': -99, 'max_lon': -88, 'min_lat': 20, 'max_lat': 30}
    standard_names = ['surface_eastward_sea_water_velocity',
                      'surface_northward_sea_water_velocity']
    region = reader.region(kw=kw, standard_names=standard_names)
    assert sorted(region.dataset_ids) == ['ucsdHfrE1', 'ucsdHfrE2', 'ucsdHfrE6']
    assert not region.meta.empty
    assert region.data_by_dataset('ucsdHfrE6') is not None

def test_station_coastwatch():
    reader = ErddapReader(known_server='coastwatch')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2'}
    dataset_id = 'ucsdHfrE6'
    station = reader.stations(dataset_ids=dataset_id, kw=kw)
    assert station.kw == kw
    assert isinstance(station.meta, pd.DataFrame)
    assert isinstance(station.data['ucsdHfrE6'], xr.Dataset)

def test_region_ioos():
    reader = ErddapReader(known_server='ioos')
    kw = {'min_time': '2019-1-1', 'max_time': '2019-1-2',
         'min_lon': -95, 'max_lon': -94, 'min_lat': 27, 'max_lat': 29}
    standard_names = ['sea_water_practical_salinity']
    region = reader.region(kw=kw, standard_names=standard_names)
    assert 'tabs_b' in region.dataset_ids
    assert not region.meta.empty
    assert region.data_by_dataset('tabs_b') is not None
    assert isinstance(region.data['tabs_b'], pd.DataFrame)
    
    
    
