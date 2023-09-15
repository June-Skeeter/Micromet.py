# Wrapper for the Klujn et al. 2015 flux footprint model

import os
import utm_zone
import numpy as np
import pandas as pd
import configparser
import geopandas as gpd
from functools import partial
import matplotlib.pyplot as plt
from multiprocessing import Pool
from Klujn_2015_Model import FFP
from shapely.geometry import Polygon

import getNARR

import rasterio
from rasterio import features
from rasterio.transform import from_origin

class RunClimatology():

    def __init__(self,Site):
                
        self.Name = Site
        inis = ['config_files/FFP.ini',f'config_files/site_specific/{Site}.ini']
        self.ini = configparser.ConfigParser()
        self.ini.read(inis)
        
        # Dump Site_Info to a Dataframe
        Site = pd.DataFrame(data=dict(self.ini['Site_Info']),index=[0])
        for c in Site.columns:
            try:
                Site[c]=Site[c].astype('float64')
            except:
                pass
        
        self.lon_lat = list(Site[['lon','lat']].values[0])
        self.EPSG = utm_zone.epsg(self.lon_lat)
        
        self.Site_WGS = gpd.GeoDataFrame(Site, geometry=gpd.points_from_xy(Site.lon, Site.lat), crs="EPSG:4326")
        self.Site_UTM = self.Site_WGS.to_crs(self.EPSG)
        self.Site_UTM = self.Site_UTM.reset_index(drop=True)

        self.z = Site['zm'].values[0]

        # =====================================================================================
        # Define grid parameters for model
        # Domain is the upwind_fetch (m) in all directions
        # Will create a grid centered on [0 0 zm]
        self.domain = int(self.ini['FFP_Parameters']['upwind_fetch'])
        # dx Cell size of domain [m], Small dx results in higher spatial resolution and higher computing time
        self.dx = int(self.ini['FFP_Parameters']['resolution'])
        # Percentage of source area for which to provide contours, must be between 10% and 90%        
        self.rs = [float(rs) for rs in self.ini['FFP_Parameters']['rs'].split(',')]

        self.nx = int(self.domain*2 / self.dx)

        x = np.linspace(-self.domain, self.domain, self.nx)# + 1)
        self.x_2d, self.y_2d = np.meshgrid(x, x)

        # Polar coordinates
        # Set theta such that North is pointing upwards and angles increase clockwise
        self.rho = np.sqrt(self.x_2d**2 + self.y_2d**2)
        self.theta = np.arctan2(self.x_2d, self.y_2d)

        # Apply a symmetric mask to restrict summations to a radius of upwind_fetch around [0 0 zm]
        symetric_Mask = self.rho.copy()
        symetric_Mask[self.rho>self.domain] = np.nan
        self.symetric_Mask = symetric_Mask*0 + 1

        # initialize raster for footprint climatology
        self.fclim_2d = np.zeros(self.x_2d.shape)

        # basemap is an optional input, requires a 'path to vector layer' pluss a 'classification' key
        self.rasterizeBasemap(self.ini['Site_Info']['basemap'],self.ini['Site_Info']['basemap_class'])

        # ==================================
        # Define input keys

        self.read_Met()
        
    def read_Met(self,Date_Range_Set=None,Time_Range_Set=None):

        self.vars = {
            'canopy_height':'canopy_height',# - can be float or array (m)
            'h':'hpbl',# - height of planetary boundary layer (m)
            'ol':'L',# - Obukhov length (m)
            'sigmav':'V_SIGMA',# - standard deviation of horizontal wind (m/s)
            'ustar':'USTAR',# - friction velocity (m/s)
            'wind_dir':'wind_dir',# - wind direction in degrees from north (deg)
        }

        self.vars_metadata = {
            'canopy_height':'Time series: of plant canopy height array (m)',
            'h':'Time series: height of planetary boundary layer (m)',
            'ol':'Time series: Obukhov length (m)',
            'sigmav':'Time series: standard deviation of horizontal wind (m/s)',
            'ustar':'Time series: friction velocity (m/s)',
            'wind_dir':'Time series: wind direction in degrees from north (deg)',
        }
        
        if self.ini['FFP_Parameters']['verbose'] == "True":
            print('Requires the following inputs, expecting them to be named as specified:\n')
            for key,value in self.vars.items():
                print(self.vars_metadata[key])
                print(f'Labelled as "{value}" in input self.dataset\n')

        df = pd.read_csv(self.ini['Site_Info']['dpath'],
                 parse_dates=[self.ini['Site_Info']['timestamp']],
                 index_col=self.ini['Site_Info']['timestamp'])

        df.dropna(how='all')

        df[self.vars['canopy_height']].fillna(self.ini['Site_Info']['canopy_height'])

        if Date_Range_Set is not None:
            if type(Date_Range_Set[0]) != type(Date_Range_Set):
                Date_Range_Set = [Date_Range_Set]
            for i,Date_Range in enumerate(Date_Range_Set):
                Range_Set = pd.date_range(start=Date_Range[0],end=Date_Range[1],freq='30T',inclusive='both')
                df.loc[df.index.isin(Range_Set),'Subset'] = i

        else:
           df['Subset'] = ''
            
        if Time_Range_Set is not None:
            if type(Time_Range_Set[0]) != type(Time_Range_Set):
                Time_Range_Set = [Time_Range_Set]
            for i,Time_Range in enumerate(Time_Range_Set):
                Range_Set = df.index[df.index.indexer_between_time(Time_Range[0],Time_Range[1])]
                df.loc[df.index.isin(Range_Set),'Subset']+=chr(ord('@')+i+1)

        self.Subset = df.loc[df['Subset'].isna()==False]

        if df[self.vars['h']].isnull().sum() == df.shape[0]:
            self.NARR = getNARR.extractNARR(self.Name,[2022,2023])

    def rasterizeBasemap(self,basemap,basemap_class):
        x,y = self.Site_UTM.geometry.x[0],self.Site_UTM.geometry.y[0]
        west = x-(self.nx*self.dx)/2
        north = y+(self.nx*self.dx)/2
        self.Transform = from_origin(west,north,self.dx,self.dx)
        
        if os.path.isfile(basemap):
            print('Rasterizing bassemap')
            # Read basemap layer and reproject if not already in the proper WGS 1984 Zone
            self.baseVector = gpd.read_file(basemap).to_crs(self.EPSG)
            self.baseVector = gpd.clip(self.baseVector,self.Site_UTM.buffer(self.domain))
            if basemap_class != 'None' and basemap_class != '':
                self.baseVector = self.baseVector.dissolve(by=basemap_class).reset_index()
            else:
                self.baseVector = self.baseVector.dissolve().reset_index(drop=True)
                basemap_class = 'aoi'
                self.baseVector[basemap_class] = basemap_class
            self.baseVector.index+=1
            self.baseRasterKey = self.baseVector[basemap_class].to_dict()
            self.Fc_Names = [self.baseRasterKey[i]+'_Fc' for i in self.baseVector.index.values]

            shapes = ((geom,value) for geom,value in zip(self.baseVector['geometry'],self.baseVector.index))

            with rasterio.open(f"{self.ini['Paths']['RasterOutput']}/Footprint_Basemap_{self.dx}m.tif",'w+',driver='GTiff',width = self.nx, height = self.nx,#+1,
                            count = 1,dtype=np.float32,transform = self.Transform,crs = ({'init': f'EPSG:{self.EPSG}'})) as out:
                out_arr = out.read(1)
                self.baseRaster = features.rasterize(shapes=shapes,fill = 100,out = out_arr,transform = self.Transform,default_value=-1)
                self.baseRaster = self.baseRaster * self.symetric_Mask
                out.write(self.baseRaster,1)

        else: 
            print('Basemap not provided, creating default')
            self.baseRaster = self.symetric_Mask
            self.Fc_Names = []
            self.baseRasterKey = {f'Contribution within {self.domain} m':''}

    def Filter(self):
        d = int(self.ini['FFP_Parameters']['exclude_wake'])
        b = self.Site_UTM['bearing'][0]
        Exclude = {
            'under':{
                'z0':0,
                'zm/ol':-15.5,
                'zm-d':self.data['z0']*12.5,
                self.vars['ustar']:.1,
                self.vars['sigmav']:0,
                self.vars['h']:10,
                self.vars['h']:self.data['zm-d'],
                self.vars['wind_dir']:0,
            },
            'over':{
                'zm-d':self.data[self.vars['h']],
                self.vars['wind_dir']:360
            },
            'between':{
                self.vars['wind_dir']:[b-180-d,b-180+d,b+180-d,b+180+d]
            }
        }
        self.data['process'] = 1

        for key,value in Exclude['under'].items():
            flagged = self.data.loc[self.data[key]<value].shape[0]
            if flagged > 0:
                print(f'{flagged} records flagged for low {key}')         
                try:
                    print(f"{key} {self.data[key].min()} below {value.min()}")
                except:
                    print(f"{key} {self.data[key].min()} below {value}")
                    pass
            self.data.loc[self.data[key]<value,'process']=0
            
        for key,value in Exclude['over'].items():
            flagged = self.data.loc[self.data[key]>value].shape[0]
            if flagged > 0:
                print(f'{flagged} records flagged for high {key}')
                try:
                    print(f"{key} {self.data[key].max()} exceeds {value.max()}")
                except:
                    print(f"{key} {self.data[key].max()} exceeds {value}")
                    pass
            self.data.loc[self.data[key]>value,'process']=0

        for key,value in Exclude['between'].items():
            flagged = self.data.loc[(((self.data[key]>value[0]) & (self.data[key]<value[1]))|
                                        ((self.data[key]>value[2]) & (self.data[key]<value[3])))].shape[0]
            if flagged > 0:
                print(f'{flagged} records flagged for bad {key}')
            self.data.loc[(((self.data[key]>value[0]) & (self.data[key]<value[1]))|
                                        ((self.data[key]>value[2]) & (self.data[key]<value[3]))),'process']=0
        
    def run(self,data): 
        
        self.data = data.reset_index()
        
        if 'canopy_height' not in self.data:
            self.data['canopy_height']=self.Site_UTM['canopy_height'][0]

        self.data['z0'] = self.data[self.vars['canopy_height']]*float(self.ini['Assumptions']['roughness_length'])
        self.data['zm-d'] = self.z-(self.data[self.vars['canopy_height']]*float(self.ini['Assumptions']['displacement_height']))
        self.data['zm/ol'] = self.data['zm-d']/self.data[self.vars['ol']]
        self.Filter()
        print(f"Processing: {self.data.loc[self.data['process']==1].shape[0]} out of {self.data.shape[0]} input records")

        if (__name__ == 'FFP_Asssment' or __name__ == '__main__') and int(self.ini['Multi_Processing']['processes'])>1:
            
            batchsize=int(np.ceil(self.data.shape[0]))
            if batchsize > int(self.ini['Multi_Processing']['BatchSize']):
                batchsize = int(self.ini['Multi_Processing']['BatchSize'])

            ix = 0
            while self.data[ix:ix+batchsize].shape[0]>0:
                batch = self.data[ix:ix+batchsize].copy()
                print(f'Processing Batch {ix}:{ix+batchsize}')
                ustar = batch[self.vars['ustar']]
                sigmav = batch[self.vars['sigmav']]
                h = batch[self.vars['h']]
                ol = batch[self.vars['ol']]
                wind_dir = batch[self.vars['wind_dir']]
                z0 = batch['z0']
                zm = batch['zm-d']
                index = batch.index
                ix += batchsize

                
                with Pool(processes=int(self.ini['Multi_Processing']['processes'])) as pool:
                    for out in pool.starmap(partial(FFP,theta=self.theta,rho=self.rho,x_2d=self.x_2d,basemap=self.baseRaster),
                                        zip(index,ustar,sigmav,h,ol,wind_dir,z0,zm)):
                        self.processOutputs(out)
                    pool.close()

        else:
            for i,row in self.data.iterrows():
                out = FFP(i,row[self.vars['ustar']],row[self.vars['sigmav']],row[self.vars['h']],
                    row[self.vars['ol']],row[self.vars['wind_dir']],row['z0'],row['zm-d'],
                    self.theta,self.rho,self.x_2d,basemap=self.baseRaster)
                self.processOutputs(out)

        self.summarizeClimatology()

    def processOutputs(self,out):
        self.fclim_2d = self.fclim_2d + out[1] * self.symetric_Mask
        if len(out) >2 and len(self.Fc_Names) > 0:
            self.data.loc[self.data.index==out[0],self.Fc_Names]=out[2]
            self.data.loc[self.data.index==out[0],f'Contribution within {self.domain} m']=out[1].sum()
        else:
            self.data.loc[self.data.index==out[0],f'Contribution within {self.domain} m']=out[1].sum()

    def summarizeClimatology(self):
        self.fclim_2d = self.fclim_2d/self.data.shape[0]
        if self.ini['Paths']['RasterOutput']!='None':
            with rasterio.open(f"{self.ini['Paths']['RasterOutput']}{self.Name}_FP_Clim_{self.dx}m.tif",'w+',driver='GTiff',width = self.nx, height = self.nx,#+1,
                        count = 1,dtype=np.float32,transform = self.Transform,crs = ({'init': f'EPSG:{self.EPSG}'})) as out:
                out.write(self.fclim_2d,1)
            
        self.countours()
        

    def countours(self):
        pclevs = np.empty(len(self.rs))
        pclevs[:] = np.nan
        ars = np.empty(len(self.rs))
        ars[:] = np.nan

        sf = np.sort(self.fclim_2d, axis=None)[::-1]
        msf = np.ma.masked_array(sf, mask=(np.isnan(sf) | np.isinf(sf))) 
        
        csf = msf.cumsum().filled(np.nan)
        for ix, r in enumerate(self.rs):
            dcsf = np.abs(csf - r)
            pclevs[ix] = sf[np.nanargmin(dcsf)]
            ars[ix] = csf[np.nanargmin(dcsf)]

        self.contour_levels = {'r':[],'r_true':[],'geometry':[]}

        for r, r_thresh, lev in zip(self.rs, ars, pclevs):
            geom = self.getGeom(lev)
            if geom is not None:
                self.contour_levels['r'].append(r)
                self.contour_levels['r_true'].append(r_thresh)
                self.contour_levels['geometry'].append(Polygon((geom)))
    
        self.contour_levels = gpd.GeoDataFrame(data = {'r':self.contour_levels['r'],'r_true':self.contour_levels['r_true']},geometry=self.contour_levels['geometry'],crs=self.EPSG)
        
        if self.ini['Paths']['ShapefileOutput']!='None':
            self.contour_levels.to_file(f"{self.ini['Paths']['RasterOutput']}{self.Name}_FP_Clim_Contours.shp")
        
        
        if os.path.isdir(self.ini['Paths']['WebmapOutput']):
            self.WGS = self.contour_levels.to_crs('WGS1984')
            self.WGS['info'] = (self.WGS['r']*100).astype(int).astype(str)+ ' % Flux Source Area Contour'       
            self.WGS = self.WGS.sort_values(by='r',ascending=False)

            MapTemplate = open(self.ini['Paths']['MapTemplate'],'r')
            MapFmt = MapTemplate.read().replace('Tower_Coords',str(self.lon_lat))

            MapFmt = MapFmt.replace('FP_Json',self.WGS.to_json())
            MapFmt = MapFmt.replace('Site_Json',self.Site_WGS.to_json())
            with open(f"{self.ini['Paths']['WebmapOutput']}{self.Name}_Footprint_Climatology_Map.html",'w+') as out:
                out.write(MapFmt)

            self.WGS.to_file(f"{self.ini['Paths']['WebmapOutput']}{self.Name}_FP_Clim_Contours.geojson",driver='GeoJSON')

    def getGeom(self,lev):
        cs = plt.contour(self.x_2d, self.y_2d, self.fclim_2d, [lev])
        plt.close()
        segs = cs.allsegs[0][0]
        xr = [vert[0] for vert in segs]
        yr = [vert[1] for vert in segs]
        #Set contour to None if it's found to reach the physical domain
        if self.x_2d.min() >= min(segs[:, 0]) or max(segs[:, 0]) >= self.x_2d.max() or \
        self.y_2d.min() >= min(segs[:, 1]) or max(segs[:, 1]) >= self.y_2d.max():
            return None
        
        else:
            return([[x+self.Site_UTM.geometry.x[0], y+self.Site_UTM.geometry.y[0]] for x,y in zip(xr,yr)])

        