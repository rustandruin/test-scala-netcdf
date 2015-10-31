This code shows how to call the NetCDF-Java binding from Scala using SBT:
 - how to setup the repository in SBT
 - how to remove the common-logging dependence that conflicts with Spark's SLF4J dependence
 - how to use the library to load climate datasets:
   - load 2d and 3d datasets into Breeze arrays
   - load the lat and lon grids
 - how to use the library to write climate datasets:
   - write 2d and 3d datasets from Breeze arrays
   - write the lat and lon grids

Note: to test the correctness of the output NetCDF files I used the python
bindings of NetCDF, which are useful but painful to install. Of course you don't need to
run this code, but if want to you'll need to install NetCDF. Here're the steps
I used, roughly, to install from scratch on a Docker Ubuntu 15.04 image. Caveat
Emptor!:
1. apt-get install libnetcdf-dev netcdf-bin python-numpy libhdf5-serial-dev 
3. install netcdf python by downloading the git repo, editing the setup.cfg to
NOT use nc-config, then manually setting the hdf5_incdir and hdf5_libdir
directories to what were installed by the libhdf5-8 package (this was implictly
installed), and edit the setup.py to look for libhdf5_serial and
libhdf5_serial_hl instead of libhdf5 and libhdf5_hl
