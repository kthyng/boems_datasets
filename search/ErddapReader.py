from erddapy import ERDDAP
from joblib import Parallel, delayed
import multiprocessing
import pandas as pd
import xarray as xr
import logging
from os import path

# Capture warnings in log
logging.captureWarnings(True)

# formatting for logfile
formatter = logging.Formatter('%(asctime)s %(message)s','%a %b %d %H:%M:%S %Z %Y')
name = 'reader_erddap'
# logfilename = path.join(name + '.log')
logfilename = path.join('..','logs', name + '.log')
loglevel=logging.WARNING

# set up logger file
handler = logging.FileHandler(logfilename)
handler.setFormatter(formatter)
logger_erd = logging.getLogger(name)
logger_erd.setLevel(loglevel)
logger_erd.addHandler(handler)


class ErddapReader(object):
    

    def __init__(self, known_server='ioos', protocol=None, server=None, parallel=True):
        
#         # run checks for KW 
#         self.kw = kw

        self.parallel = parallel
        
        # either select a known server or input protocol and server string
        if known_server == 'ioos':
            protocol = 'tabledap'
            server = 'http://erddap.sensors.ioos.us/erddap'
        elif known_server == 'coastwatch':
            protocol = 'griddap'
            server = 'https://coastwatch.pfeg.noaa.gov/erddap/'
        else:
            known_server = 'not pre-known server'
            statement = 'either select a known server or input protocol and server string'
            assert (protocol is not None) & (server is not None), statement
        
        self.e = ERDDAP(server=server)
        self.e.protocol = protocol
        self.e.server = server
                  
        # columns for metadata
        self.columns = ['geospatial_lat_min', 'geospatial_lat_max', 
               'geospatial_lon_min', 'geospatial_lon_max', 
               'time_coverage_start', 'time_coverage_end',
               'defaultDataQuery', 'subsetVariables',  # first works for timeseries sensors, 2nd for gliders
               'keywords',  # for hf radar
               'id', 'infoUrl', 'institution', 'featureType', 'source', 'sourceUrl']
        
        # name
        self.name = 'erddap_%s' % (known_server)
        
# #         self.data_type = data_type
#         self.standard_names = standard_names
#         # DOESN'T CURRENTLY LIMIT WHICH VARIABLES WILL BE FOUND ON EACH SERVER


    
    
    @property
    def dataset_ids(self):
        '''Find dataset_ids for server.'''
        
        if not hasattr(self, '_dataset_ids'):
            
            # This should be a region search
            if (self.standard_names is not None) and (self._stations is None): 
        
                # find all the dataset ids which we will use to get the data
                # This limits the search to our keyword arguments in kw which should 
                # have min/max lon/lat/time values
                dataset_ids = []
                for standard_name in self.standard_names:

                    # find and save all dataset_ids associated with standard_name
                    # if standard_name is not found, this will return all search 
                    # results. Since we want to know if a standard_name was not 
                    # found, we also do the subsequent search that doesn't include
                    # the standard_name constraint, and compare the length of the 
                    # results. If they are the same length, then we conclude that 
                    # standard_name was not found in the search.
                    search_url = self.e.get_search_url(response="csv", **self.kw, 
                                                       standard_name=standard_name, 
                                                       items_per_page=10000)
                    search_url2 = self.e.get_search_url(response="csv", **self.kw, 
                                                       items_per_page=10000)

                    try:
                        search = pd.read_csv(search_url)
                        search2 = pd.read_csv(search_url2)
                        assert len(search) != len(search2), "standard_name was not found in the search, don't use these dataset_ids"

                        dataset_ids.extend(search["Dataset ID"])
                    except Exception as e:
                        logger_erd.exception(e)
                        logger_erd.warning("standard_name was not found in the search, don't use these dataset_ids")
                        logger_erd.warning('1 search_url %s\n2 search_url %s\n' % (search_url, search_url2))
                        # should go into logger
            #             print('standard_name %s not found' % standard_name)


                # only need a dataset id once since we will check them each for all standard_names
                self._dataset_ids = list(set(dataset_ids))
            
            # This should be a search for the station names
            elif self._stations is not None:
                
                # search by station name for each of stations
                dataset_ids = []
                for station in self._stations:
                    # if station has more than one word, AND will be put between to search for multiple 
                    # terms together
                    url = self.e.get_search_url(response="csv", items_per_page=5, search_for=station)

                    df = pd.read_csv(url)
    
                    # first try for exact station match
                    try:
                        dataset_id = [dataset_id for dataset_id in df['Dataset ID'] if station.lower() in dataset_id.lower().split('_')][0]

                    # if that doesn't work, trying for more general match and just take first returned option
                    except Exception as e:
                        logger_erd.exception(e)
                        logger_erd.warning('When searching for a dataset id to match station name %s, the first attempt to match the id did not work.' % (station))
                        dataset_id = df.iloc[0]['Dataset ID']
        
#                         if 'tabs' in org_id:  # don't split
#                             axiom_id = [axiom_id for axiom_id in df['Dataset ID'] if org_id.lower() == axiom_id.lower()]
#                         else:
#                             axiom_id = [axiom_id for axiom_id in df['Dataset ID'] if org_id.lower() in axiom_id.lower().split('_')][0]
                
#                     except:
#                         dataset_id = None
                
                    dataset_ids.append(dataset_id)
                    
                self._dataset_ids = list(set(dataset_ids))
                
            else:
                logger_erd.warning('Neither stations nor region approach were used in function dataset_ids.')
                
            
        return self._dataset_ids
        
    
    def meta_by_dataset(self, dataset_id):

        info_url = self.e.get_info_url(response="csv", dataset_id=dataset_id)
        info = pd.read_csv(info_url)

        items = []

        for col in self.columns:

            try:
                item = info[info['Attribute Name'] == col]['Value'].values[0]
                dtype = info[info['Attribute Name'] == col]['Data Type'].values[0]
            except:
                if col == 'featureType':
                    # this column is not present in HF Radar metadata but want it to
                    # map to data_type, so input 'grid' in that case.
                    item = 'grid'
                else:
                    item = 'NA'

            if dtype == 'String':
                pass
            elif dtype == 'double':
                item = float(item)
            elif dtype == 'int':
                item = int(item)
            items.append(item)
            
        if self.standard_names is not None:
            # In case the variable is named differently from the standard names, 
            # we back out the variable names here for each dataset. This also only 
            # returns those names for which there is data in the dataset.
            varnames = self.e.get_var_by_attr(
                dataset_id=dataset_id,
                standard_name=lambda v: v in self.standard_names
            )
        else:
            varnames = None

        ## include download link ##
        self.e.dataset_id = dataset_id
        if self.e.protocol == 'tabledap':
            if self.standard_names is not None:
                self.e.variables = ["time","longitude", "latitude", "station"] + varnames
            # set the same time restraints as before
            self.e.constraints = {'time<=': self.kw['max_time'], 'time>=': self.kw['min_time'],}
            download_url = self.e.get_download_url(response='csvp')

        elif self.e.protocol == 'griddap':
            # the search terms that can be input for tabledap do not work for griddap
            # in erddapy currently. Instead, put together an opendap link and then 
            # narrow the dataset with xarray.
            # get opendap link
            download_url = self.e.get_download_url(response='opendap')
        
        # add erddap server name
        return {dataset_id: [self.e.server, download_url] + items + [varnames]}
    
      
    @property
    def meta(self):
        
        if not hasattr(self, '_meta'):
            
            if self.parallel:
            
                # get metadata for datasets
                # run in parallel to save time
                num_cores = multiprocessing.cpu_count()
                downloads = Parallel(n_jobs=num_cores)(
                    delayed(self.meta_by_dataset)(dataset_id) for dataset_id in self.dataset_ids
                )
                
            else:

                downloads = []
                for dataset_id in self.dataset_ids:
                    downloads.append(self.meta_by_dataset(dataset_id))

            # make dict from individual dicts
            from collections import ChainMap
            meta = dict(ChainMap(*downloads)) 

            # Make dataframe of metadata
            # variable names are the column names for the dataframe
            self._meta = pd.DataFrame.from_dict(meta, orient='index', 
                                                columns=['database','download_url'] \
                                                + self.columns + ['variable names'])
           
        return self._meta       
    
    
    def data_by_dataset(self, dataset_id):

        download_url = self.meta.loc[dataset_id, 'download_url']
        # data variables in ds that are not the variables we searched for
        varnames = self.meta.loc[dataset_id, 'variable names']

        if self.e.protocol == 'tabledap':

            try:

                # fetch metadata if not already present
                # found download_url from metadata and use
                dd = pd.read_csv(download_url, index_col=0, parse_dates=True)
                
                # Drop cols and rows that are only NaNs.
                dd = dd.dropna(axis='index', how='all').dropna(axis='columns', how='all')

                if varnames is not None:
                    # check to see if there is any actual data
                    # this is a bit convoluted because the column names are the varnames 
                    # plus units so can't match 1 to 1.
                    datacols = 0  # number of columns that represent data instead of metadata
                    for col in dd.columns:
                        datacols += [varname in col for varname in varnames].count(True)
                    # if no datacols, we can skip this one.
                    if datacols == 0:
                        dd = None
                    
            except Exception as e:
                logger_erd.exception(e)
                logger_erd.warning('no data to be read in for %s' % dataset_id)
                dd = None
        
        elif self.e.protocol == 'griddap':

            try:
                dd = xr.open_dataset(download_url, chunks='auto').sel(time=slice(self.kw['min_time'],self.kw['max_time']))

                if ('min_lat' in self.kw) and ('max_lat' in self.kw):
                    dd = dd.sel(latitude=slice(self.kw['min_lat'],self.kw['max_lat']))

                if ('min_lon' in self.kw) and ('max_lon' in self.kw):
                    dd = dd.sel(longitude=slice(self.kw['min_lon'],self.kw['max_lon']))

                # use variable names to drop other variables (should. Ido this?)
                if varnames is not None:
                    l = set(dd.data_vars) - set(varnames)
                    dd = dd.drop_vars(l)
                
            except Exception as e:
                logger_erd.exception(e)
                logger_erd.warning('no data to be read in for %s' % dataset_id)
                dd = None
                
        return (dataset_id, dd)


    @property
    def data(self):
        
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

    
    # Search for stations by region
    def region(self, kw, standard_names):
        
        self._stations = None
        
        # run checks for KW 
        # check for lon/lat values and time
        self.kw = kw
                          
#         self.data_type = data_type
        if not isinstance(standard_names, list):
            standard_names = [standard_names]
        self.standard_names = standard_names
        # DOESN'T CURRENTLY LIMIT WHICH VARIABLES WILL BE FOUND ON EACH SERVER
        
        return self

    
    def stations(self, dataset_ids=None, stations=None, kw=None):
        '''
        
        Use keyword dataset_ids if you already know the database-
        specific ids. Otherwise, use the keyword stations and the 
        database-specific ids will be searched for. The station 
        ids can be input as something like "TABS B" and will be 
        searched for as "TABS AND B" and has pretty good success.
        '''
        
        # we want all the data associated with stations
        self.standard_names = None
        
        if dataset_ids is not None:
            if not isinstance(dataset_ids, list):
                dataset_ids = [dataset_ids]
            self._dataset_ids = dataset_ids 
        
        if stations is not None:
            if not isinstance(stations, list):
                stations = [stations]
            self._stations = stations
            self.dataset_ids
            
        
        # CHECK FOR KW VALUES AS TIMES
        if kw is None:
            kw = {'min_time': '1900-01-01', 'max_time': '2100-12-31'}
            
        self.kw = kw
#         print(self.kwself.)
        
            
        return self