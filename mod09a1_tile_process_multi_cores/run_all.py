import modis_mod09a1_hdf_veindex_pt_desktop as veg
import modis_mod09a1_hdf_cloud_masks_desktop as cloud
import modis_mod09a1_hdf_ocean_masks_desktop as ocean
import modis_mod09a1_hdf_snow_masks_desktop as snow
import produce_flood_desktop_version_ocean_snow as flood
import produce_drought_desktop_version as drought
import produce_evergreen_desktop_version_ocean_snow as evergreen
import time

mod09a1_path = "/data/vol11/chandra_test/mod09a1"
geotiff_path = "/data/vol11/chandra_test/products/mod09a1/geotiff"
startTime = time.time()


vegstartTime = time.time()
veg.run(mod09a1_path,geotiff_path)
vegendtime = time.time()

cloudstartTime = time.time()
cloud.run(mod09a1_path,geotiff_path)
cloudendtime = time.time()

oceanstartTime = time.time()
ocean.run(mod09a1_path,geotiff_path)
oceanendtime = time.time()

snowstartTime = time.time()
snow.run(mod09a1_path,geotiff_path)
snowendtime = time.time()

floodstartTime = time.time()
flood.run(geotiff_path)
floodendtime = time.time()

droughtstartTime = time.time()
drought.run(geotiff_path)
droughtendtime = time.time()

evergreenstartTime = time.time()
evergreen.run(geotiff_path)
evergreenendtime = time.time()

endTime = time.time()


print '\n\n\n\n NDVI,EVI, LSWI script took ' +str(vegendtime - vegstartTime)+ ' seconds'

print ' cloud script took ' +str(cloudendtime - cloudstartTime)+ ' seconds'
print ' ocean script took ' +str(oceanendtime - oceanstartTime)+ ' seconds'
print ' snow script took ' +str(snowendtime - snowstartTime)+ ' seconds'
print ' flood script took ' +str(floodendtime - floodstartTime)+ ' seconds'
print ' drought script took ' +str(droughtendtime - droughtstartTime)+ ' seconds'
print ' evergreen script took ' +str(evergreenendtime - evergreenstartTime)+ ' seconds'

print '\n\nThe script total took ' +str(endTime - startTime)+ ' seconds'


