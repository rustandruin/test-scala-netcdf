# TODO: load the corresponding row from the parquet file and check that it is the same as the values the python netCDF4 bindings give
import netCDF4
import numpy as np
import msgpack
import subprocess
import glob

hdfsfname = "/user/root/CFSRArawtars/pgbh02.gdas.19790101-19790105.tar"
recordname = "pgbh02.gdas.1979010100.grb2"
ncfname = "pgbh02.gdas.1979010100.nc"
msgpackfname = "samples.msgpack"

msgpackfin = open(msgpackfname, "r")
parquetvalues = np.asarray(msgpack.unpack(msgpackfin))
msgpackfin.close()

variable_names = [ "TCDC_P8_L234_GLL0_avg", "TCDC_P8_L224_GLL0_avg", "TCDC_P8_L214_GLL0_avg", "TCDC_P8_L200_GLL0_avg", "TCDC_P0_L244_GLL0", "CSDLF_P8_L1_GLL0_avg", "CSULF_P8_L8_GLL0_avg", "CSULF_P8_L1_GLL0_avg", "ULWRF_P8_L8_GLL0_avg", "ULWRF_P8_L1_GLL0_avg", "DLWRF_P8_L1_GLL0_avg", "ULWRF_P0_L1_GLL0", "DLWRF_P0_L1_GLL0", "CPRAT_P8_L1_GLL0_avg", "ACPCP_P8_L1_GLL0_acc", "NCPCP_P8_L1_GLL0_acc", "APCP_P8_L1_GLL0_acc", "PRATE_P8_L1_GLL0_avg", "CAPE_P0_2L108_GLL0", "CAPE_P0_L1_GLL0", "CIN_P0_2L108_GLL0", "CIN_P0_L1_GLL0", "PLI_P0_2L108_GLL0", "CWAT_P0_L200_GLL0", "PWAT_P0_L200_GLL0", "PWAT_P0_2L108_GLL0", "TMP_P0_L100_GLL0", "VVEL_P0_L100_GLL0", "VGRD_P0_L100_GLL0", "RH_P0_L100_GLL0", "SPFH_P0_L100_GLL0"]

subprocess.call(["/root/ephemeral-hdfs/bin/hdfs", "dfs", "-get", hdfsfname, "test.tar"])
subprocess.call("tar -xvf test.tar", shell=True)
subprocess.call("rm -f test.tar %s" % " ".join([fname for fname in glob.glob("*.grb2") if fname != recordname]), shell=True)
print "PATH=$PATH:/root/ncl/bin NCARG_ROOT=/root/ncl ncl_convert2nc %s -v %s" % (recordname, ",".join(variable_names))
subprocess.call("PATH=$PATH:/root/ncl/bin NCARG_ROOT=/root/ncl ncl_convert2nc %s -v %s" % (recordname, ",".join(variable_names)), shell=True)
ncfile = netCDF4.Data(ncfname)

values = np.asarray([])
for varname in variable_names:
  curvar = ncfile.variables[varname]
  if curvar.ndim == 2:
    mask = np.ma.masked_equal(curvar[:,:], curvar._FillValue).mask
    if mask:
      print("Some masked values in this 2d variable: %s" % varname)
    values.extend(curvar[:,:].flatten())
  elif curvar.ndim == 3:
    mask = np.ma.masked_equal(curvar[...], curvar._FillValue).mask
    if mask:
      print("Some masked values in this 3d variable: %s" % varname)
    values.extend(curvar[:,:,:].flatten())
  else:
    print("What you talking 'bout, Willis?")

print np.linalg.norm(values - parquetvalues)

