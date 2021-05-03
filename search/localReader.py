import logging
import os
import intake
import pandas as pd
import hashlib
from joblib import Parallel, delayed
import multiprocessing

os.makedir("../catalogs", exist_ok=True)  # succeeds even if directory exists.

# Capture warnings in log
logging.captureWarnings(True)

# formatting for logfile
os.makedir("../logs", exist_ok=True)  # succeeds even if directory exists.
formatter = logging.Formatter('%(asctime)s %(message)s','%a %b %d %H:%M:%S %Z %Y')
log_name = 'reader_local'
# logfilename = os.path.join(name + '.log')
logfilename = os.path.join('..','logs', log_name + '.log')
loglevel=logging.WARNING

# set up logger file
handler = logging.FileHandler(logfilename)
handler.setFormatter(formatter)
logger_local = logging.getLogger(log_name)
logger_local.setLevel(loglevel)
logger_local.addHandler(handler)

# this can be queried with 
# search.localReader.reader
reader = 'localReader'



class localReader:
    

    def __init__(self, parallel=True, catalog_name=None, filenames=None, kw=None):

        self.parallel = parallel

        if catalog_name is None:
            name = f'{pd.Timestamp.now().isoformat()}'
            hash_name = hashlib.sha256(name.encode()).hexdigest()[:7]
            self.catalog_name = os.path.join('..','catalogs', f'catalog_{hash_name}.yml')
        else:
            self.catalog_name = catalog_name
            # if catalog_name already exists, read it in to save time
            self.catalog

        if (filenames is not None) and (not isinstance(filenames, list)):
            filenames = [filenames]
        self.filenames = filenames
        
        if kw is None:
            kw = {'min_time': '1900-01-01', 'max_time': '2100-12-31'}
            
        self.kw = kw
        
        if (filenames == None) and (catalog_name == None):
            self._dataset_ids = []
            logger_local.warning('no datasets for localReader with catalog_name {catalog_name} and filenames {filenames}.')
        
        # name
        self.name = 'local'

        self.reader = 'localReader'


    
    def write_catalog(self):
        
        # if the catalog already exists, don't do this
        if os.path.exists(self.catalog_name):
            return
        
        else:
            lines = 'sources:\n'

            for filename in self.filenames:

                if 'csv' in filename:
                    file_intake = intake.open_csv(filename)
                    data = file_intake.read()
                    metadata = {'variables': list(data.columns.values),
                                'geospatial_lon_min': float(data['longitude'].min()),
                                'geospatial_lat_min': float(data['latitude'].min()),
                                'geospatial_lon_max': float(data['longitude'].max()),
                                'geospatial_lat_max': float(data['latitude'].max()),
                                'time_coverage_start': data['time'].min(),
                                'time_coverage_end': data['time'].max()}
                    file_intake.metadata = metadata
#                                             'time variables info': 'test', 'space variables info': 'test'}
                elif 'nc' in filename:
                    file_intake = intake.open_netcdf(filename)
                    data = file_intake.read()
                    metadata = {'coords': list(data.coords.keys()),
                                'variables': list(data.data_vars.keys()),
                                }
                    file_intake.metadata = metadata

                file_intake.name = filename.split('/')[-1]
                lines += file_intake.yaml().strip('sources:')

            f = open(self.catalog_name, "w")
            f.write(lines)
            f.close()    

    
    @property
    def catalog(self):
        
        if not hasattr(self, '_catalog'):
            
            self.write_catalog()
            catalog = intake.open_catalog(self.catalog_name)
            self._catalog = catalog
            
        return self._catalog
    
    
    @property
    def dataset_ids(self):
        '''Find dataset_ids for server.'''
        
        if not hasattr(self, '_dataset_ids'):
            self._dataset_ids = list(self.catalog)
                
        return self._dataset_ids
        
    
    def meta_by_dataset(self, dataset_id):
        '''Should this return intake-style or a row of the metadata dataframe?'''
        
        return self.catalog[dataset_id]
    
      
    @property
    def meta(self):
        '''Rearrange the individual metadata into a dataframe.'''
        
        if not hasattr(self, '_meta'):
            
            data = []
            if self.dataset_ids == []:
                self._meta = None
            else:
                for dataset_id in self.dataset_ids:
                    meta = self.meta_by_dataset(dataset_id)
                    columns = ['download_url'] + list(meta.metadata.keys())  # this only needs to be set once
                    data.append([meta.urlpath] + list(meta.metadata.values()))
                self._meta = pd.DataFrame(index=self.dataset_ids, columns=columns, data=data)
           
        return self._meta       
    
    
    def data_by_dataset(self, dataset_id):
        '''SHOULD I INCLUDE TIME RANGE?'''
        
        data = self.catalog[dataset_id].read()
#         data = data.set_index('time')
#         data = data[self.kw['min_time']:self.kw['max_time']]

        return (dataset_id, data)
#         return (dataset_id, self.catalog[dataset_id].read())


    @property
    def data(self):
        '''Do I need to worry about intake caching?
        
        Data will be dataframes for csvs and 
        Datasets for netcdf files.
        '''
        
        if not hasattr(self, '_data'):
            
            if self.parallel:
                num_cores = multiprocessing.cpu_count()
                downloads = Parallel(n_jobs=num_cores)(
                    delayed(self.data_by_dataset)(dataset_id) for dataset_id in self.dataset_ids
                )
            else:
                downloads = []
                for dataset_id in self.dataset_ids:
                    downloads.append(self.data_by_dataset(dataset_id))

#             if downloads is not None:
            dds = {dataset_id: dd for (dataset_id, dd) in downloads}
#             else:
#                 dds = None

            self._data = dds

        return self._data


class region(localReader):
#     def region(self, kw, variables=None):
#         '''HOW TO INCORPORATE VARIABLE NAMES?'''
        
    def __init__(self, kwargs):
        lo_kwargs = {'catalog_name': kwargs.get('catalog_name', None),
                     'filenames': kwargs.get('filenames', None),
                     'parallel': kwargs.get('parallel', True)}
        localReader.__init__(self, **lo_kwargs)
        
        kw = kwargs['kw']
        variables = kwargs.get('variables', None)

        
        self.approach = 'region'
        
        self._stations = None

        # run checks for KW 
        # check for lon/lat values and time
        self.kw = kw
                          
# #         self.data_type = data_type
        if (variables is not None) and (not isinstance(variables, list)):
            variables = [variables]
            
        # make sure variables are on parameter list
        if variables is not None:
            self.check_variables(variables)            
            
        self.variables = variables
#         # DOESN'T CURRENTLY LIMIT WHICH VARIABLES WILL BE FOUND ON EACH SERVER
        
#         return self

    
class stations(localReader):
#     def stations(self, dataset_ids=None, stations=None, kw=None):
#         '''
#         '''
        
        
    def __init__(self, kwargs):
        loc_kwargs = {'catalog_name': kwargs.get('catalog_name', None),
                     'filenames': kwargs.get('filenames', None),
                     'parallel': kwargs.get('parallel', True)}
        localReader.__init__(self, **loc_kwargs)
        
        kw = kwargs.get('kw', None)
        dataset_ids = kwargs.get('dataset_ids', None)
        stations = kwargs.get('stations', None)

        self.approach = 'stations'
        

        
# #         self.catalog_name = os.path.join('..','catalogs',f'catalog_stations_{pd.Timestamp.now().isoformat()[:19]}.yml')
        
# #         # we want all the data associated with stations
# #         self.standard_names = None
        
#         # UPDATE SINCE NOW THERE IS A DIFFERENCE BETWEEN STATION AND DATASET
#         if dataset_ids is not None:
#             if not isinstance(dataset_ids, list):
#                 dataset_ids = [dataset_ids]
# #             self._stations = dataset_ids
#             self._dataset_ids = dataset_ids 
    
# #         assert (if dataset_ids is not None) 
#         # assert that dataset_ids can't be something if axds_type is layer_group
#         # use stations instead, and don't use module uuid, use layer_group uuid
        
#         if stations is not None:
#             if not isinstance(stations, list):
#                 stations = [stations]
#         self._stations = stations
            
#         self.dataset_ids
            
        
        # CHECK FOR KW VALUES AS TIMES
        if kw is None:
            kw = {'min_time': '1900-01-01', 'max_time': '2100-12-31'}
            
        self.kw = kw
#         print(self.kwself.)
        
            
#         return self