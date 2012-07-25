# -*- coding: cp936 -*-
# mod09a1_oceanmask.pro
#
# Examine sur_refl_500m_state_flags (passed in)
# For pixels that are not 'fillval' extract land/water flag
# convert to land/water flag from:
# modis_mod09a1_hdf_stateflags_oceanmask_pt.pro
# Xiangming Xiao
# September 30, 2003
#
# Bits 5–8	Land-water mask
# Shallow ocean	 0
# Land (Nothing else but land)	 1
# Ocean coastlines and lake shorelines	 2
# Shallow inland water	 3
# Ephemeral water	 4
# Deep inland water	 5
# Moderate or continental ocean	 6
# Deep ocean	 7
#
#--------------------------------------------------------------------------
# Delong Zhao
# zhaodelong@gmail.com
# August 18, 2011
#--------------------------------------------------------------------------



import os, sys, time, numpy, fnmatch
from osgeo import gdal
from osgeo.gdalconst import *
import multiprocessing

# start timing
startTime = time.time()

# information on MOD09A1 file
ncol = 2400
nrow = 2400
fillval = 255


def return_band(mod09a1_file_name, band_number):
    #open mod09a1
    fn_mod09a1 = mod09a1_file_name

    if band_number < 8 and band_number > 0:
        #print 'Get band: ' + str(band_number)
        fn_mod09a1 = 'HDF4_EOS:EOS_GRID:'+fn_mod09a1+':MOD_Grid_500m_Surface_Reflectance:sur_refl_b0'+str(band_number)
        #print fn_mod09a1
    elif band_number == 11:
        #print 'Get band: ' + str(band_number)
        fn_mod09a1 = 'HDF4_EOS:EOS_GRID:'+fn_mod09a1+':MOD_Grid_500m_Surface_Reflectance:sur_refl_state_500m'
        #print fn_mod09a1
    else:
        print 'Band number out of range'
        sys.exit(1)
        
        
    ds_mod09a1 = gdal.Open(fn_mod09a1,GA_ReadOnly)
    if ds_mod09a1 is None:
        print "Could not open " + fn_mod09a1
        sys.exit(1)

    geoTransform = ds_mod09a1.GetGeoTransform()
    proj = ds_mod09a1.GetProjection()
        
    rasterband = ds_mod09a1.GetRasterBand(1)
    type(rasterband)
    band = rasterband.ReadAsArray(0,0,ncol,nrow)
    band = band.astype(numpy.uint16)

    return band,geoTransform,proj

    ds_mod09a1 = None
    band = None

def output_file(output_name,output_array,geoTransform,proj):
    format = "GTiff"
    driver = gdal.GetDriverByName( format )
    outDataset = driver.Create(output_name,2400,2400,1,GDT_Byte)
    outBand = outDataset.GetRasterBand(1)
    outBand.WriteArray(output_array,0,0)
    outBand.FlushCache()
    outBand.SetNoDataValue(fillval)
    outDataset.SetGeoTransform(geoTransform )
    outDataset.SetProjection(proj)

def process_oceanmask(mod09a1_file_name,oceanmask_output_name):
    fn_mod09a1 = mod09a1_file_name
    
    stateflags,geoTransform,proj  = return_band(fn_mod09a1,11) # band 11 -- 500m State Flags
    
    goodpix_mask = numpy.where(stateflags==65535,1,0)
    
    stateflags = numpy.right_shift(stateflags,3)
    stateflags = numpy.bitwise_and(stateflags,7)

    ocean = stateflags

    numpy.putmask(ocean,goodpix_mask,fillval)

    output_file(oceanmask_output_name,ocean,geoTransform,proj)

    stateflags = None
    ocean = None

def creatoutputfolder(path,root_dir,output_dir,product):
    mod09a1_dir = os.path.dirname(path)
    name = os.path.basename(path)
    mod09a1 = os.path.join(mod09a1_dir,name)
    #print 'Processing: ', mod09a1

    relative_path = os.path.relpath(mod09a1_dir,root_dir)
    #print relative_path

    product_dir = os.path.join(output_dir,product)
    product_dir = os.path.join(product_dir,relative_path)
    if not os.path.exists(product_dir):
        os.makedirs(product_dir)
        print '    Creating dir: ',product_dir

def product_output_name(output_dir,mod09a1_name,relative_path,product):
    product_dir = os.path.join(output_dir,product)
    product_dir = os.path.join(product_dir,relative_path)
    #print product_dir
    product_output_name = mod09a1_name[:-4]+'.'+product+'.tif'
    #print product_output_name
    #print product_dir
    product_path_file = os.path.join(product_dir,product_output_name)
    print '    Output file: ',product_path_file
    return product_path_file

def findfiles(root_dir):
    toprocess = []  # list to store paths for processing in
    for root,dir,files in os.walk(root_dir):
        for name in files:
            if fnmatch.fnmatch(name,'*.hdf'):
               toprocess.append( os.path.join(root, name) )
               print os.path.join(root, name)
    return toprocess

def doprocess(mod09a1):
    t1 = time.time()
    mod09a1_dir = os.path.dirname(mod09a1)
    name = os.path.basename(mod09a1)
    #print 'Processing: ', mod09a1

    relative_path = os.path.relpath(mod09a1_dir,root_dir)
    #print relative_path

    
    landwatermask = product_output_name(output_dir,name,relative_path,'landwatermask')
    #print output_dir
    #print '    Processing: ',landwatermask
    process_oceanmask(mod09a1,landwatermask)
    #print '\n'
    
    print 'Processing done: ',landwatermask
    t2 = time.time()
    print 'One process took :' +str(t2 - t1)+ ' seconds'
    print '\n'


    

def run(root_dir_input,output_dir_input):

    global output_dir
    output_dir = output_dir_input
    global root_dir
    root_dir = root_dir_input

    filelist = findfiles(root_dir)
    print root_dir
    
    for f in filelist:
        creatoutputfolder(f,root_dir,output_dir,'landwatermask')

    
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    pool.map(doprocess,filelist)
    print 'Using ' +str(multiprocessing.cpu_count())+' cores.'
    print 'Processed : ' +str(len(filelist))+' hdf files.'
    
    


'''
def make_product_dir(output_dir,mod09a1_name,relative_path,product):
    product_dir = os.path.join(output_dir,product)
    product_dir = os.path.join(product_dir,relative_path)
    if not os.path.exists(product_dir):
        os.makedirs(product_dir)
    product_output_name = mod09a1_name[:-4]+'.'+product+'.tif'
    product_path_file = os.path.join(product_dir,product_output_name)
    print '    Processing: ',product_path_file
    return product_path_file


def main(root_dir,output_dir):

    
    #input mod09a1 root directory
    root_dir = root_dir

    root_dir_search = root_dir
    #root_dir_search = "Q:\\modis\\mod09a1\\2007"


    output_dir = output_dir
    
    for root,dir,files in os.walk(root_dir_search):
        for name in files:
            if fnmatch.fnmatch(name,'*.hdf'):
                t1 = time.time()
                mod09a1 = os.path.join(root,name)
                print 'Processing: ', mod09a1

                relative_path = os.path.relpath(root,root_dir)
                
                oceanmask = make_product_dir(output_dir,name,relative_path,'landwatermask')
                process_oceanmask(mod09a1,oceanmask)
                t2 = time.time()
                print '     took ' +str(t2 - t1)+ ' seconds for cloudmask product per image.'
                print '\n'
                

if __name__ == '__main__':

    main(root_dir,output_dir)

    endTime = time.time()
    print '\n\nThe script took ' +str(endTime - startTime)+ ' seconds'
    

'''

