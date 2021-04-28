import search
from search.ErddapReader import ErddapReader
from search.axdsReader import axdsReader
import pandas as pd

# # data functions by data_type
# DATASOURCES_GRID = [hfradar, seaice_extent, seaice_con]
# DATASOURCES_SENSOR = [sensors]
# DATASOURCES_PLATFORM = [sensors, argo]  # has gliders

# GO THROUGH ALL KNOWN SOURCES?
SOURCES = [ErddapReader(known_server='ioos'),
           ErddapReader(known_server='coastwatch'),
           axdsReader()]

# MAYBE SHOULD BE ABLE TO INITIALIZE THE CLASS WITH ONLY METADATA OR DATASET NAMES?
# to skip looking for the datasets

class Data(object):
    
    def __init__(self, kw=None, standard_names=None, data_types=None):
        
        # default kw to all U.S. and most recent 4 weeks
        if kw is None:
            now = pd.Timestamp.now().normalize()
            
            # Gulf of Mexico
            kw = {
                "min_lon": -99,
                "max_lon": -88, 
                "min_lat": 20, 
                "max_lat": 30, 
                "min_time": (now - pd.Timedelta('4W')).strftime('%Y-%m-%d'),
                "max_time": (now).strftime('%Y-%m-%d'),
            }
            

#             # full U.S.
#             kw = {
#                 "min_lon": -195,
#                 "max_lon": -60, 
#                 "min_lat": 17, 
#                 "max_lat": 80, 
#                 "min_time": (now - pd.Timedelta('4W')).strftime('%Y-%m-%d'),
#                 "max_time": (now).strftime('%Y-%m-%d'),
#             }

        self.kw = kw
        
#         self.only_meta = only_meta
        
        # default to all reasonable options
        # Note that `sea_ice_concentration` is not a standard name but 
        # we do want to include it from NSIDC.
        if standard_names is None:
            
            standard_names = ['sea_water_temperature', 
                              'sea_water_practical_salinity', 
                              'sea_water_speed', 
                              'sea_water_velocity_to_direction', 
                              'sea_surface_height', 
                              'sea_surface_height_above_sea_level', 
                              'sea_surface_height_amplitude_due_to_geocentric_ocean_tide',
                              'surface_eastward_sea_water_velocity',  # hfradar
                              'surface_northward_sea_water_velocity',  # hfradar
                              'sea_ice_speed',
                              'direction_of_sea_ice_velocity',
                              'eastward_sea_ice_velocity',
                              'northward_sea_ice_velocity',
                              'sea_ice_extent',
                              'sea_ice_area_fraction'  # multiply by 100 to get sea_ice_concentration which is not a standard name
                              ]
            
        self.standard_names = standard_names

        # default to including all data types
        # These should map to the type of plot that will be possible.
        if data_types is None:

            data_types = ['sensor', 'platform', 'grid']
            
        self.data_types = data_types
        
        
    @property
    def sources(self):
        '''Set up data sources.
        '''
            
        if not hasattr(self, '_sources'):

            # loop over data sources to set them up
            sources = []
            for source in SOURCES:
                
                # setup reader
                reader = source.region(kw=self.kw, standard_names=self.standard_names)
                
                sources.append(reader)

            self._sources = sources
        
        return self._sources
    
    
    @property
    def dataset_ids(self):
        
        if not hasattr(self, '_dataset_ids'):

            dataset_ids = []
            for source in self.sources:

                dataset_ids.append(source.dataset_ids)

            self._dataset_ids = dataset_ids
        
        return self._dataset_ids        
            
    
    @property
    def meta(self):
        '''Find and return metadata for datasets.
        
        Do this by querying each data source function for metadata
        then use the metadata for quick returns.
        
        This will not rerun if the metadata has already been found.
        
        SEPARATE DATASOURCE FUNCTIONS INTO A PART THAT RETRIEVES THE 
        DATASET_IDS AND METADATA AND A PART THAT READS IN THE DATA.
        
        DIFFERENT SOURCES HAVE DIFFERENT METADATA
        
        START EVERYTHING BEING REGION BASED BUT LATER MAYBE ADD A STATION
        OPTION.
        
        EXPOSE DATASET_IDS?
        
        '''
        
        if not hasattr(self, '_meta'):

            # loop over data sources to read in metadata
            meta = []
            for source in self.sources:
                
                meta.append(source.meta)

            self._meta = meta
        
        return self._meta
    
    
    @property
    def data(self):
        '''Return the data, given metadata.'''
        
        if not hasattr(self, '_data'):
            
            # loop over data sources to read in data
            data = []
            for source in self.sources:
                
                data.append(source.data)
                
            self._data = data
                
        return self._data
    
    
