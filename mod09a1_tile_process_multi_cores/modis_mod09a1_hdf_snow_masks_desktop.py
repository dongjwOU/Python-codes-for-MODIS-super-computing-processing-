#
# mod09a1_snowmask.pro
#
# generate snow cover map. It uses the algorithm from Hall et al 1998, 1995
#
# cloud mask --  examine sur_refl_500m_state_flags file
# and find out those pixels that are good in all bands
# Bit 0-1   cloud state
#       00 - clear
#       01y
#       10 - mixed
#       11 - not set, assume clear
# Bit 2 cloud shadow
#   0 -- no
#   1 -- yes
# Delong Zhao
# zhaodelong@gmail.com
# August 18, 2011
#--------------------------------------------------------------------------



import os, sys, time, numpy, fnmatch
from osgeo import gdal
from osgeo.gdalconst import *
import multiprocessing

# start timing
#startTime = time.time()

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

def output_file_byte(output_name,output_array,geoTransform,proj):
    format = "GTiff"
    driver = gdal.GetDriverByName( format )
    outDataset = driver.Create(output_name,2400,2400,1,GDT_Byte)
    outBand = outDataset.GetRasterBand(1)
    outBand.WriteArray(output_array,0,0)
    outBand.FlushCache()
    outBand.SetNoDataValue(fillval)
    outDataset.SetGeoTransform(geoTransform )
    outDataset.SetProjection(proj)

def output_file_float(output_name,output_array,geoTransform,proj):
    format = "GTiff"
    driver = gdal.GetDriverByName( format )
    outDataset = driver.Create(output_name,2400,2400,1,GDT_Float32)
    outBand = outDataset.GetRasterBand(1)
    outBand.WriteArray(output_array,0,0)
    outBand.FlushCache()
    outBand.SetNoDataValue(-2.0)
    outDataset.SetGeoTransform(geoTransform)
    outDataset.SetProjection(proj)

def normalize(band1,band2):
    var1 = numpy.subtract(band1,band2)
    var2 = numpy.add(band1,band2)

    numpy.seterr(all='ignore')
    ndvi = numpy.divide(var1,var2)

    return ndvi

def process_ndsi(mod09a1_file_name,ndsi_output_name):
    # ndsi = (green - swir1) / (green + swir1)   from Hall et al., 1998
    # band 1 -- red      (620 - 670nm)
    # band 2 -- nir1  (841 - 875 nm)
    # band 3 -- blue  (459 - 479 nm)
    # band 4 -- green (545 - 565 nm)
    # band 5 -- nir2  (1230 - 1250 nm)
    # band 6 -- swir1 (1628 - 1652 nm)
    # band 7 -- swir2 (2105 - 2155 nm)
    #open mod09a1
    fn_mod09a1 = mod09a1_file_name
    
    swir1,geoTransform,proj  = return_band(fn_mod09a1,6) # band 6
    green = return_band(fn_mod09a1,4)[0] # band 4 -- nir1	(841 - 875 nm)

    swir1 = swir1.astype(numpy.float)
    green = green.astype(numpy.float)

    ocean_mask = numpy.where(swir1 == -28672, 1, 0)
    swir1_mask = numpy.where(swir1 <= 1 , 1, 0)
    green_mask = numpy.where(green <= 1, 1, 0)
    
    
    ndsi = normalize(green,swir1)

    min_ndsi_mask = numpy.where(ndsi < -1.0, 1, 0)
    max_ndsi_mask = numpy.where(ndsi > 1.0, 1, 0)

    numpy.putmask(ndsi, min_ndsi_mask, -1.0)
    numpy.putmask(ndsi, max_ndsi_mask, 1.0)
    numpy.putmask(ndsi, swir1_mask, -2.0)
    numpy.putmask(ndsi, green_mask, -2.0)
    numpy.putmask(ndsi, ocean_mask, -3.0)
    
    #output file
    
    output_file_float(ndsi_output_name,ndsi,geoTransform,proj)

    return ndsi
        
    swir1 = None
    green = None
    lswi = None
    min_ndsi_mask = None
    max_ndsi_mask = None
    ocean_mask = None

def process_snowmask(mod09a1_file_name,snowmask_output_name,ndsi):
    fn_mod09a1 = mod09a1_file_name
    
    stateflags,geoTransform,proj  = return_band(fn_mod09a1,11) # band 11 -- 500m State Flags

    stateflags_copy = stateflags
    
    goodpix_mask = numpy.where(stateflags==65535,1,0)
    stateflags = numpy.right_shift(stateflags,3)
    stateflags = numpy.bitwise_and(stateflags,7)
    numpy.putmask(stateflags,goodpix_mask,0)

    landmask = numpy.where(stateflags>0,1,0)
    snow = stateflags
    numpy.putmask(snow,landmask,1)

    #print snow

    
    nir2 = return_band(fn_mod09a1,5)[0]# band 5 -- nir2  (1230 - 1250 nm)

    snowpix_mask1 = numpy.where(ndsi > 0.4,1,0)
    snowpix_mask2 = numpy.where(nir2 > 1100,1,0)
    snowpix_mask = numpy.logical_and(snowpix_mask1,snowpix_mask2)

    numpy.putmask(snow,snowpix_mask,2)

    cloud = numpy.bitwise_and(stateflags_copy,7)
    cloudpix_mask1 = numpy.where(snow>0,1,0)
    cloudpix_mask2 = numpy.where(cloud!=0,1,0)
    cloudpix_mask3 = numpy.where(cloud!=7,1,0)
    cloudpix_mask = numpy.logical_and(cloudpix_mask1,cloudpix_mask2)
    cloudpix_mask = numpy.logical_and(cloudpix_mask,cloudpix_mask3)

    numpy.putmask(snow,cloudpix_mask,1) 

    #print snow
    output_file_byte(snowmask_output_name,snow,geoTransform,proj)

    stateflags = None
    stateflags_copy = None
    snow = None

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
    #print '    Output file: ',product_path_file
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

    
    ndsi_ouput_name = product_output_name(output_dir,name,relative_path,'ndsi')
    #print output_dir
    print '    Processing: ',ndsi_ouput_name
    ndsi = process_ndsi(mod09a1,ndsi_ouput_name)
    #print '\n'

    snowmask = product_output_name(output_dir,name,relative_path,'snowmask')
    process_snowmask(mod09a1,snowmask,ndsi)
    
    print 'Processing done: ',snowmask
    t2 = time.time()
    print 'One process took :' +str(t2 - t1)+ ' seconds'
    #print '\n'


    

def run(root_dir_input,output_dir_input):

    global output_dir
    output_dir = output_dir_input
    global root_dir
    root_dir = root_dir_input

    filelist = findfiles(root_dir)
    print root_dir
    
    for f in filelist:
        creatoutputfolder(f,root_dir,output_dir,'ndsi')
        creatoutputfolder(f,root_dir,output_dir,'snowmask')

    
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
    output_dir = output_dir
    

    
    for root,dir,files in os.walk(root_dir_search):
        for name in files:
            if fnmatch.fnmatch(name,'*.hdf'):
                t1 = time.time()
                mod09a1 = os.path.join(root,name)
                print 'Processing: ', mod09a1
                relative_path = os.path.relpath(root,root_dir)                
                ndsifile = make_product_dir(output_dir,name,relative_path,'ndsi')
                ndsi = process_ndsi(mod09a1,ndsifile)                
                
                snowmask = make_product_dir(output_dir,name,relative_path,'snowmask')
                process_snowmask(mod09a1,snowmask,ndsi)
                t2 = time.time()
                print '     took ' +str(t2 - t1)+ ' seconds for ndsi product'


                print '\n'
                

if __name__ == '__main__':

    main(root_dir,output_dir)

    endTime = time.time()
    print '\n\nThe script took ' +str(endTime - startTime)+ ' seconds'
    

'''


