
import os, sys, time, numpy,fnmatch,datetime,glob
from osgeo import gdal
from osgeo.gdalconst import *

# start timing
#startTime = time.time()

landvalue = 1 # in our new python oceanmask code, we use 1 for land, old idl code use 8 for land. so define this value according to different product
snowvalue = 2 #snow value = 2, so nonsnow use not 2
nocloudvalue = 1

eversnow_value = 240
evercloud_value = 250
noland_value = 255

def open_mask(mask_name):
    #open mask
    fn_mask = mask_name
    ds_mask = gdal.Open(fn_mask, GA_ReadOnly)
    if ds_mask is None:
        print 'Could not open ' + fn_mask
        sys.exit(1)

    cols_mask = ds_mask.RasterXSize
    rows_mask = ds_mask.RasterYSize
    bands_mask = ds_mask.RasterCount
    #print cols_mask,rows_mask,bands_mask
    
    band_mask = ds_mask.GetRasterBand(1)
    
    data_mask = band_mask.ReadAsArray(0, 0, cols_mask, rows_mask)

    return data_mask

def process_evergreen(evergreen_output_name,cloudmask_dir,snowmask_dir,oceanmask_dir,files):
    print len(files)
    data_evergreen = numpy.zeros((2400,2400),dtype=int)
    data_evergreen005 = numpy.zeros((2400,2400),dtype=int)
    data_evergreen01 = numpy.zeros((2400,2400),dtype=int)
    ever_snow = numpy.zeros((2400,2400),dtype=int)
    ever_cloud = numpy.zeros((2400,2400),dtype=int)
    
    
    for f in files:
        data_lswi = open_mask(f)
        date_lswi = data_lswi.astype(numpy.float)
        name = f.split('\\')[-1]

        #print name[:-8]

        
        for root2,dir,files2 in os.walk(cloudmask_dir):
            for name2 in files2:
                if name2[:-13] == name[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                    cloudmask_name = os.path.join(root2,name2)
                    #print 'cloud_mask  : ',cloudmask_name

        for root2,dir,files2 in os.walk(snowmask_dir):
            for name2 in files2:
                if name2[:-12] == name[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                    snowmask_name = os.path.join(root2,name2)
                    #print 'snow_mask  : ',snowmask_name
                            


        data_cloudmask = open_mask(cloudmask_name)
        snowmask = open_mask(snowmask_name)

        snowmask_count = numpy.where(snowmask==snowvalue,1,0)
        data_cloudmask_count = numpy.where(data_cloudmask != nocloudvalue,1,0)
        
        
        ever_snow += snowmask_count
        ever_cloud += data_cloudmask_count
        
        
        #data_drought = numpy.where(numpy.less(data_lswi,0)& (data_cloudmask==1)& (oceanmask==landvalue)&(snowmask!=snowvalue),1,0)
        data_evergreen += numpy.where(numpy.less(data_lswi,0)& (data_cloudmask==1)&(snowmask!=snowvalue),1,0)
        data_evergreen005 += numpy.where(numpy.less(data_lswi,0.05)& (data_cloudmask==1)&(snowmask!=snowvalue),1,0)
        data_evergreen01 += numpy.where(numpy.less(data_lswi,0.1)& (data_cloudmask==1)&(snowmask!=snowvalue),1,0)
        print f + ' done'

    #print data_evergreen
    #print data_evergreen005
    #print data_evergreen01
    
    
    own_mask_data_evergreen005= numpy.where(data_evergreen005>0,1,0)
    numpy.putmask(data_evergreen005,own_mask_data_evergreen005, data_evergreen005+46)
    own_mask_data_evergreen01= numpy.where(data_evergreen01>0,1,0)
    numpy.putmask(data_evergreen01,data_evergreen01 >0, data_evergreen01+92)

    #print data_evergreen
    #print data_evergreen005
    #print data_evergreen01   
    
    evergreen0_mask = numpy.where(data_evergreen == 0,1,0)
    numpy.putmask(data_evergreen, evergreen0_mask, data_evergreen005)
    evergreen005_mask = numpy.where(data_evergreen == 0,1,0)
    numpy.putmask(data_evergreen, evergreen005_mask, data_evergreen01)
    

    for root2,dir,files2 in os.walk(oceanmask_dir):
        for name2 in files2:
            if name2[:-17] == files[1].split('\\')[-1][:-8] and fnmatch.fnmatch(name2, '*.tif'):
                oceanmask_name = os.path.join(root2,name2)
                #print 'ocean_mask  : ',oceanmask_name

    #eversnow_value = 240
    #evercloud_value = 250
    #noland_value = 255    

    oceanmask = open_mask(oceanmask_name)
    land_only_mask = numpy.where(oceanmask != landvalue,1,0)
    numpy.putmask(data_evergreen,land_only_mask,noland_value)

    ever_snow_mask = numpy.where(ever_snow == len(files),1,0)
    ever_cloud_mask = numpy.where(ever_cloud == len(files),1,0)

    numpy.putmask(data_evergreen,ever_snow_mask,eversnow_value)
    numpy.putmask(data_evergreen,ever_cloud_mask,evercloud_value)
    

    print data_evergreen

    

    #open source file type
    fn_cloudmask = files[1]
    ds_cloudmask = gdal.Open(fn_cloudmask, GA_ReadOnly)
    if ds_cloudmask is None:
        print 'Could not open ' + fn_cloudmask
        sys.exit(1)
    
    #output dataset
    out_fn = evergreen_output_name
    #Get the Imagine driver and register it
    #Works for reading and creating new Imagine files
    src_ds = ds_cloudmask
    driver = src_ds.GetDriver()
    outDataset = driver.CreateCopy(out_fn, src_ds,0)
    #outDataset= driver.Creat(out_fn, 2400,2400,1)
    outBand = outDataset.GetRasterBand(1)
    outBand.WriteArray(data_evergreen,0,0)
    #outDataset.SetProjection(ds_drought_wkt)    
    

def creatoutputfolder(f,root_dir,target_dir):
    f_dir = os.path.dirname(f)
    f_name = os.path.basename(f)
    lswi_dir = os.path.join(root_dir,'lswi') 

    relative_path = os.path.relpath(f_dir,lswi_dir)
    #print relative_path

    product_dir = os.path.join(target_dir,relative_path)
    if not os.path.exists(product_dir):
        os.makedirs(product_dir)
        print '    Creating dir: ',product_dir

def findfiles(search_dir):
    toprocess = []  # list to store paths for processing in
    for root,dir,files in os.walk(search_dir):
        for name in files:
            if fnmatch.fnmatch(name,'*.tif'):
               toprocess.append( os.path.join(root, name) )
               #print os.path.join(root, name)
    return toprocess


        
    
'''    
def main(root_dir):
    root_dir = root_dir
    lswi_root_dir = os.path.join(root_dir,'lswi')
    #lswi_root_dir_test = 'Q:\\modis\\products\\mod09a1\\geotiff\\lswi\\2007' #testing use this
    cloudmask_root_dir = os.path.join(root_dir,'cloudmask')
    snowmask_root_dir = os.path.join(root_dir,'snowmask')
    oceanmask_root_dir = os.path.join(root_dir,'landwatermask')

    #evi_year = os.path.join(evi_dir,'2005')
    evergreen_dir = os.path.join(root_dir,'evergreen')
    
    
    for root,dir,files in os.walk(lswi_root_dir):
        t1 = time.time()
        if root[-6]=='h':
            print root
            files = glob.glob(root+'\\*.tif')
            files.sort()
            lswi_first_name = files[0]
            print lswi_first_name
            a = lswi_first_name.split('\\')[-1]
            evergreen_output_name = a[:-8]+'lswi_count.tif'            
            relative_path = os.path.relpath(root,lswi_root_dir)
            ds_dir = os.path.join(evergreen_dir,relative_path)
            if not os.path.exists(ds_dir):
                os.makedirs(ds_dir)
            evergreen_output_name = os.path.join(ds_dir,evergreen_output_name)
            print evergreen_output_name

            cloudmask_dir = os.path.join(cloudmask_root_dir,relative_path)
            snowmask_dir = os.path.join(snowmask_root_dir,relative_path)
            oceanmask_dir = os.path.join(oceanmask_root_dir,relative_path)

            print cloudmask_dir
            print snowmask_dir
            print oceanmask_dir

            process_evergreen(evergreen_output_name,cloudmask_dir,snowmask_dir,oceanmask_dir,files)
            
        t2 = time.time()
        print '     took ' +str(t2 - t1)+ ' seconds for evergreen product'
        print '\n'
            
                       
#main
if __name__ == '__main__':
    
    main(root_dir)
#endMain
    
# figure out how long the script took to run
endTime = time.time()
print '\n\nThe script took ' +str(endTime - startTime)+ ' seconds'
'''
