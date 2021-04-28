import pandas as pd
import xarray as xr
import logging
from os import path

# Capture warnings in log
logging.captureWarnings(True)

# formatting for logfile
formatter = logging.Formatter('%(asctime)s %(message)s','%a %b %d %H:%M:%S %Z %Y')
name = 'reader_erddap'
logfilename = path.join('logs', name + '.log')
loglevel=logging.WARNING

# set up logger file
handler = logging.FileHandler(logfilename)
handler.setFormatter(formatter)
logger = logging.getLogger(name)
logger.setLevel(loglevel)
logger.addHandler(handler)


class ThreddsReader(object):
    

    def __init__(self, server=None):
        
        self.name
        
        
        
        
    
    @property
    def dataset_ids(self):
        '''Find dataset_ids for server.'''
        
        if not hasattr(self, '_dataset_ids'):
            
            pass
            
        return self._dataset_ids
        
    
    def meta_by_dataset(self, dataset_id):

    
      
    @property
    def meta(self):
        
        if not hasattr(self, '_meta'):
            pass
            
           
        return self._meta       
    
    
    def data_by_dataset(self, dataset_id):



    @property
    def data(self):
        
        if not hasattr(self, '_data'):
            pass

        return self._data

    
    # Search for stations by region
    def region(self, kw, standard_names):
        pass
        

    
    def stations(self, dataset_ids=None, stations=None, kw=None):
        pass
