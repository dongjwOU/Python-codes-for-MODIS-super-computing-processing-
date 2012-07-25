
import os, sys, time, numpy,fnmatch
from osgeo import gdal
from osgeo.gdalconst import *
import multiprocessing


# start timing
#startTime = time.time()

oceanvalue = 0
oceanvalue = 7 # in our new python oceanmask code, we use 1 for land, old idl code use 8 for land. so define this value according to different product
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

def process_flood(evi_name,lswi_name,cloudmask_name,oceanmask_name,snowmask_name,flood_output_name):
    #open evi
    fn_evi = evi_name
    ds_evi = gdal.Open(fn_evi, GA_ReadOnly)
    '''
    if ds_evi is None:
        print 'Could not open ' + fn_evi
        sys.exit(1)
    '''
    
    cols_evi = ds_evi.RasterXSize
    rows_evi = ds_evi.RasterYSize
    bands_evi = ds_evi.RasterCount
    #print cols_evi,rows_evi,bands_evi

    band_evi = ds_evi.GetRasterBand(1)
    
    data_evi = band_evi.ReadAsArray(0, 0, cols_evi, rows_evi)
    date_evi = data_evi.astype(numpy.float)
    
    #open lswi
    fn_lswi = lswi_name
    ds_lswi = gdal.Open(fn_lswi, GA_ReadOnly)
    if ds_lswi is None:
        print 'Could not open ' + fn_lswi
        sys.exit(1)

    cols_lswi = ds_lswi.RasterXSize
    rows_lswi = ds_lswi.RasterYSize
    bands_lswi = ds_lswi.RasterCount
    #print cols_lswi,rows_lswi,bands_lswi

    band_lswi = ds_lswi.GetRasterBand(1)

    data_lswi = band_lswi.ReadAsArray(0, 0, cols_lswi, rows_lswi)
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
    
    #flood calculation
    data_flood = numpy.zeros((rows_lswi,cols_lswi))
    data_flood = data_lswi+0.05-data_evi
    data_flood = numpy.where(numpy.greater(data_flood,0)& (data_cloudmask==1)& (oceanmask!= oceanvalue)&(oceanmask!= 0)&(oceanmask!= 6)&(snowmask!=2),1,0)

    #print data_lswi[1000,1000], data_evi[1000,1000],data_flood[1000,1000]
    #print data_flood
    #print numpy.sum(data_flood)
    #print numpy.sum(data_flood)/(2400.0*2400)

    #output dataset
    out_fn = flood_output_name
    #Get the Imagine driver and register it
    #Works for reading and creating new Imagine files
    src_ds = ds_cloudmask
    driver = src_ds.GetDriver()
    outDataset = driver.CreateCopy(out_fn, src_ds,0)
    #outDataset= driver.Creat(out_fn, 2400,2400,1)
    outBand = outDataset.GetRasterBand(1)
    outBand.WriteArray(data_flood,0,0)
    #outDataset.SetProjection(ds_flood_wkt)
    
    # Once we're done, close properly the dataset
    src_ds = None
    ds_evi = None
    ds_lswi = None
    ds_cloudmask = None
    outDataset = None

def creatoutputfolder(f,root_dir,flood_dir):
    f_dir = os.path.dirname(f)
    f_name = os.path.basename(f)
    evi_dir = os.path.join(root_dir,'evi') 

    relative_path = os.path.relpath(f_dir,evi_dir)
    #print relative_path

    product_dir = os.path.join(flood_dir,relative_path)
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

def doprocess(f_evi):
    t1 = time.time()
    f_evi_dir = os.path.dirname(f_evi)
    f_evi_name = os.path.basename(f_evi)
    #print 'Processing: ', mod09a1

    evi_dir = os.path.join(root_dir,'evi')    
    lswi_root_dir = os.path.join(root_dir,'lswi')
    cloudmask_root_dir = os.path.join(root_dir,'cloudmask')
    snowmask_root_dir = os.path.join(root_dir,'snowmask')
    oceanmask_root_dir = os.path.join(root_dir,'landwatermask')
    flood_dir = os.path.join(root_dir,'flood')

    relative_path = os.path.relpath(f_evi_dir,evi_dir)
    #print relative_path  
    
    flood_file_dir = os.path.join(flood_dir,relative_path)
    #print ds_dir
    flood_output_name = f_evi_name[:-7]+'flood.tif'
    flood_output_name = os.path.join(flood_file_dir,flood_output_name)
    print 'flood output: ',flood_output_name

    lswi_dir = os.path.join(lswi_root_dir,relative_path)
    #print ,lswi_dir
    #print f_evi_name[:-7]
    print 'evi       :',f_evi

    
    
    for root1,dir,files1 in os.walk(lswi_dir):
        for name1 in files1:
            if name1[:-8] == f_evi_name[:-7] and fnmatch.fnmatch(name1, '*.tif'):
                lswi_name = os.path.join(root1,name1)
                print 'lswi        : ',lswi_name
    
    cloudmask_dir = os.path.join(cloudmask_root_dir,relative_path)
    #print cloudmask_dir
    for root2,dir,files2 in os.walk(cloudmask_dir):
        for name2 in files2:
            if name2[:-13] == f_evi_name[:-7] and fnmatch.fnmatch(name2, '*.tif'):
                cloudmask_name = os.path.join(root2,name2)
                print 'cloud_mask  : ',cloudmask_name
                #print name2[:-13]
    
    snowmask_dir = os.path.join(snowmask_root_dir,relative_path)
    for root2,dir,files2 in os.walk(snowmask_dir):
        for name2 in files2:
            if name2[:-12] == f_evi_name[:-7] and fnmatch.fnmatch(name2, '*.tif'):
                snowmask_name = os.path.join(root2,name2)
                print 'snow_mask  : ',snowmask_name
                            
    oceanmask_dir = os.path.join(oceanmask_root_dir,relative_path)
    for root2,dir,files2 in os.walk(oceanmask_dir):
        for name2 in files2:
            if name2[:-17] == f_evi_name[:-7] and fnmatch.fnmatch(name2, '*.tif'):
                oceanmask_name = os.path.join(root2,name2)
                print 'ocean_mask  : ',oceanmask_name

    if f_evi is not None and lswi_name is not None and cloudmask_name is not None and snowmask_name is not None and oceanmask_name is not None and flood_output_name is not None:
        print "Processing:", flood_output_name
        process_flood(f_evi,lswi_name,cloudmask_name,oceanmask_name,snowmask_name,flood_output_name)
           
        
    else:
        print 'Missing files'
    
    
    
    print 'Processing done: ',flood_output_name

    lswi_name = None
    cloudmask_name = None
    flood_output_name = None
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
    flood_dir = os.path.join(root_dir,'flood')

    filelist = findfiles(evi_dir)
    filelist.sort()
    

    for f in filelist:
        creatoutputfolder(f,root_dir,flood_dir)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    #pool = multiprocessing.Pool(1)
    pool.map(doprocess,filelist)
    print 'Using ' +str(multiprocessing.cpu_count())+' cores.'
    print 'Processed : ' +str(len(filelist))+' files.'    

    

'''   
def main(root_dir):
    #evi_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.evi.tif'    
    #lswi_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.lswi.tif'
    #cloudmask_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.cloudmask.tif'
    #flood_output_name = 'MOD09A1.A2006257.h21v08.005.2008108052801.flood.tif'
    #process_flood(evi_name,lswi_name,cloudmask_name,flood_output_name)

    evi_name = None    
    lswi_name = None
    cloudmask_name = None
    flood_output_name = None
    
    #input mod09a1 root directory

    output_dir = root_dir

    evi_dir = os.path.join(root_dir,'evi')
    
    lswi_root_dir = os.path.join(root_dir,'lswi')
    cloudmask_root_dir = os.path.join(root_dir,'cloudmask')
    snowmask_root_dir = os.path.join(root_dir,'snowmask')
    oceanmask_root_dir = os.path.join(root_dir,'landwatermask')

    #evi_year = os.path.join(evi_dir,'2005')

    flood_dir = os.path.join(root_dir,'flood')
    
    for root,dir,files in os.walk(evi_dir):
        #print root
        for name in files:
            #print root
            if fnmatch.fnmatch(name,'*.tif'):
                t1 = time.time()
                #print name,root
                evi = name
                evi_name = os.path.join(root,name)
                print '\nevi         : ',evi_name

                relative_path = os.path.relpath(root,evi_dir) 
                ds_dir = os.path.join(flood_dir,relative_path)
                if not os.path.exists(ds_dir):
                    os.makedirs(ds_dir)
                #print ds_dir
                flood_output_name = evi[:-7]+'flood.tif'
                flood_output_name = os.path.join(ds_dir,flood_output_name)
                print 'flood output: ',flood_output_name

                lswi_dir = os.path.join(lswi_root_dir,relative_path)
                #print ,lswi_dir
                #print evi[:-7]
                for root1,dir,files1 in os.walk(lswi_dir):
                    for name1 in files1:
                        if name1[:-8] == evi[:-7] and fnmatch.fnmatch(name1, '*.tif'):
                            lswi_name = os.path.join(root1,name1)
                print 'lswi        : ',lswi_name

                cloudmask_dir = os.path.join(cloudmask_root_dir,relative_path)
                for root2,dir,files2 in os.walk(cloudmask_dir):
                    for name2 in files2:
                        if name2[:-13] == evi[:-7] and fnmatch.fnmatch(name2, '*.tif'):
                            cloudmask_name = os.path.join(root2,name2)
                            print 'cloud_mask  : ',cloudmask_name

                snowmask_dir = os.path.join(snowmask_root_dir,relative_path)
                for root2,dir,files2 in os.walk(snowmask_dir):
                    for name2 in files2:
                        if name2[:-12] == evi[:-7] and fnmatch.fnmatch(name2, '*.tif'):
                            snowmask_name = os.path.join(root2,name2)
                            print 'snow_mask  : ',snowmask_name
                            
                oceanmask_dir = os.path.join(oceanmask_root_dir,relative_path)
                for root2,dir,files2 in os.walk(oceanmask_dir):
                    for name2 in files2:
                        if name2[:-17] == evi[:-7] and fnmatch.fnmatch(name2, '*.tif'):
                            oceanmask_name = os.path.join(root2,name2)
                            print 'ocean_mask  : ',oceanmask_name

                if evi_name is not None and lswi_name is not None and cloudmask_name is not None and snowmask_name is not None and oceanmask_name is not None and flood_output_name is not None:
                    print "Processing:", flood_output_name
                    process_flood(evi_name,lswi_name,cloudmask_name,oceanmask_name,snowmask_name,flood_output_name)
                    evi_name = None    
                    lswi_name = None
                    cloudmask_name = None
                    flood_output_name = None
                else:
                    print 'Missing files'
                t2 = time.time()
                print '     took ' +str(t2 - t1)+ ' seconds for flood product'
                print '\n'

                



                        
#main
if __name__ == '__main__':
    
    main(root_dir)
#endMain
    
# figure out how long the script took to run
endTime = time.time()
print '\n\nThe script took ' +str(endTime - startTime)+ ' seconds'
'''
