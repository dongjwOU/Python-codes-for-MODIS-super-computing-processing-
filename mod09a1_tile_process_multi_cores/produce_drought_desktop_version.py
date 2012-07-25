
import os, sys, time, numpy,fnmatch
from osgeo import gdal
from osgeo.gdalconst import *
import multiprocessing

# start timing
#startTime = time.time()

landvalue = 1 # in our new python oceanmask code, we use 1 for land, old idl code use 8 for land. so define this value according to different product
snowvalue = 2 #snow value = 2, so nonsnow use not 2

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

def process_drought(lswi_name,cloudmask_name,oceanmask_name,snowmask_name,drought_output_name):
    #open lswi
    data_lswi = open_mask(lswi_name)
    date_lswi = data_lswi.astype(numpy.float)
    
    #open cloudmask
    fn_cloudmask = cloudmask_name
    ds_cloudmask = gdal.Open(fn_cloudmask, GA_ReadOnly)
    if ds_cloudmask is None:
        print 'Could not open ' + fn_cloudmask
        sys.exit(1)

    cols_cloudmask = ds_cloudmask.RasterXSize
    rows_cloudmask = ds_cloudmask.RasterYSize
    bands_cloudmask = ds_cloudmask.RasterCount
    #print cols_cloudmask,rows_cloudmask,bands_cloudmask
    
    band_cloudmask = ds_cloudmask.GetRasterBand(1)    
    data_cloudmask = band_cloudmask.ReadAsArray(0, 0, cols_cloudmask, rows_cloudmask)

    oceanmask = open_mask(oceanmask_name)
    snowmask = open_mask(snowmask_name)
    
    #drought calculation
    #data_drought = numpy.zeros((rows_lswi,cols_lswi))
    data_drought = numpy.where(numpy.less(data_lswi,0)& (data_cloudmask==1)& (oceanmask==landvalue)&(snowmask!=snowvalue),1,0)
    #data_drought = numpy.where(numpy.less(data_lswi,0),1,0)

    #print data_lswi[1000,1000], data_evi[1000,1000],data_drought[1000,1000]
    #print data_drought
    #print numpy.sum(data_drought)
    #print numpy.sum(data_drought)/(2400.0*2400)

    #output dataset
    out_fn = drought_output_name
    #Get the Imagine driver and register it
    #Works for reading and creating new Imagine files
    src_ds = ds_cloudmask
    driver = src_ds.GetDriver()
    outDataset = driver.CreateCopy(out_fn, src_ds,0)
    #outDataset= driver.Creat(out_fn, 2400,2400,1)
    outBand = outDataset.GetRasterBand(1)
    outBand.WriteArray(data_drought,0,0)
    #outDataset.SetProjection(ds_drought_wkt)
    
    # Once we're done, close properly the dataset
    src_ds = None
    ds_lswi = None
    ds_cloudmask = None
    outDataset = None

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

def doprocess(f_lswi):
    t1 = time.time()
    f_lswi_dir = os.path.dirname(f_lswi)
    f_lswi_name = os.path.basename(f_lswi)
    #print 'Processing: ', mod09a1

    lswi_root_dir = os.path.join(root_dir,'lswi')
    cloudmask_root_dir = os.path.join(root_dir,'cloudmask')
    snowmask_root_dir = os.path.join(root_dir,'snowmask')
    oceanmask_root_dir = os.path.join(root_dir,'landwatermask')
    drought_dir = os.path.join(root_dir,'drought')

    relative_path = os.path.relpath(f_lswi_dir,lswi_root_dir)
    #print relative_path  
    
    drought_file_dir = os.path.join(drought_dir,relative_path)
    drought_output_name = f_lswi_name[:-8]+'drought.tif'
    drought_output_name = os.path.join(drought_file_dir,drought_output_name)
    #print 'drought output: ',drought_output_name

    cloudmask_dir = os.path.join(cloudmask_root_dir,relative_path)
    #print cloudmask_dir
    for root2,dir,files2 in os.walk(cloudmask_dir):
        for name2 in files2:
            if name2[:-13] == f_lswi_name[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                cloudmask_name = os.path.join(root2,name2)
                #print 'cloud_mask  : ',cloudmask_name
                #print name2[:-13]
    
    snowmask_dir = os.path.join(snowmask_root_dir,relative_path)
    for root2,dir,files2 in os.walk(snowmask_dir):
        for name2 in files2:
            if name2[:-12] == f_lswi_name[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                snowmask_name = os.path.join(root2,name2)
                #print 'snow_mask  : ',snowmask_name
                            
    oceanmask_dir = os.path.join(oceanmask_root_dir,relative_path)
    for root2,dir,files2 in os.walk(oceanmask_dir):
        for name2 in files2:
            if name2[:-17] == f_lswi_name[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                oceanmask_name = os.path.join(root2,name2)
                #print 'ocean_mask  : ',oceanmask_name

    if  f_lswi is not None and cloudmask_name is not None and snowmask_name is not None and oceanmask_name is not None and drought_output_name is not None:
        #print "Processing:", drought_output_name
        process_drought(f_lswi,cloudmask_name,oceanmask_name,snowmask_name,drought_output_name)
        
    else:
        print 'Missing files'
    
    
    
    print 'Processing done: ',drought_output_name

    lswi_name = None
    cloudmask_name = None
    drought_output_name = None
    t2 = time.time()
    print 'One process took :' +str(t2 - t1)+ ' seconds'
    #print '\n'


def run(input_root_dir):
    global root_dir
    root_dir = input_root_dir
    evi_dir = os.path.join(root_dir,'evi')    
    lswi_root_dir = os.path.join(root_dir,'lswi')
    cloudmask_root_dir = os.path.join(root_dir,'cloudmask')
    snowmask_root_dir = os.path.join(root_dir,'snowmask')
    oceanmask_root_dir = os.path.join(root_dir,'landwatermask')
    drought_dir = os.path.join(root_dir,'drought')

    filelist = findfiles(lswi_root_dir)
    filelist.sort()
    

    for f in filelist:
        creatoutputfolder(f,root_dir,drought_dir)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    #pool = multiprocessing.Pool(1)
    pool.map(doprocess,filelist)
    print '\n\n\n\n\nUsing ' +str(multiprocessing.cpu_count())+' cores.'
    print 'Processed : ' +str(len(filelist))+' files.'    

'''    
def main(root_dir):
    #evi_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.evi.tif'    
    #lswi_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.lswi.tif'
    #cloudmask_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.cloudmask.tif'
    #drought_output_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.drought.tif'
    #process_drought(evi_name,lswi_name,cloudmask_name,drought_output_name)

    lswi_name = None
    cloudmask_name = None
    drought_output_name = None
    
    root_dir = root_dir
    lswi_root_dir = os.path.join(root_dir,'lswi')
    #lswi_root_dir_test = 'Q:\\modis\\products\\mod09a1\\geotiff\\lswi\\2004\\h10v05' #testing use this
    cloudmask_root_dir = os.path.join(root_dir,'cloudmask')
    snowmask_root_dir = os.path.join(root_dir,'snowmask')
    oceanmask_root_dir = os.path.join(root_dir,'landwatermask')

    #evi_year = os.path.join(evi_dir,'2005')

    drought_dir = os.path.join(root_dir,'drought')
    
    for root,dir,files in os.walk(lswi_root_dir ):
        #print root
        for name in files:
            #print root
            if fnmatch.fnmatch(name,'*.tif'):
                t1 = time.time()
                #print name,root
                lswi = name
                lswi_name = os.path.join(root,name)
                print '\nlswi         : ',lswi_name

                relative_path = os.path.relpath(root,lswi_root_dir ) 
                ds_dir = os.path.join(drought_dir,relative_path)
                if not os.path.exists(ds_dir):
                    os.makedirs(ds_dir)
                #print ds_dir
                drought_output_name = lswi[:-8]+'drought.tif'
                drought_output_name = os.path.join(ds_dir,drought_output_name)
                print 'drought output: ',drought_output_name

                cloudmask_dir = os.path.join(cloudmask_root_dir,relative_path)
                for root2,dir,files2 in os.walk(cloudmask_dir):
                    for name2 in files2:
                        if name2[:-13] == lswi[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                            cloudmask_name = os.path.join(root2,name2)
                            print 'cloud_mask  : ',cloudmask_name

                snowmask_dir = os.path.join(snowmask_root_dir,relative_path)
                for root2,dir,files2 in os.walk(snowmask_dir):
                    for name2 in files2:
                        if name2[:-12] == lswi[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                            snowmask_name = os.path.join(root2,name2)
                            print 'snow_mask  : ',snowmask_name
                            
                oceanmask_dir = os.path.join(oceanmask_root_dir,relative_path)
                for root2,dir,files2 in os.walk(oceanmask_dir):
                    for name2 in files2:
                        if name2[:-17] == lswi[:-8] and fnmatch.fnmatch(name2, '*.tif'):
                            oceanmask_name = os.path.join(root2,name2)
                            print 'ocean_mask  : ',oceanmask_name

                if  lswi_name is not None and cloudmask_name is not None and snowmask_name is not None and oceanmask_name is not None and drought_output_name is not None:
                    print "Processing:", drought_output_name
                    process_drought(lswi_name,cloudmask_name,oceanmask_name,snowmask_name,drought_output_name)
                    lswi_name = None
                    cloudmask_name = None
                    drought_output_name = None
                else:
                    print 'Missing files'
                t2 = time.time()
                print '     took ' +str(t2 - t1)+ ' seconds for drought product'
                print '\n'

                



                        
#main
if __name__ == '__main__':
    
    main(root_dir)
#endMain
    
# figure out how long the script took to run
endTime = time.time()
print '\n\nThe script took ' +str(endTime - startTime)+ ' seconds'
'''
