import numpy as np
from netCDF4 import Dataset
rootgrp = Dataset("/home/gittens/test-scala-netcdf/test.nc", "r")
varname = "TMP_P0_L100_GLL0"
curvar = rootgrp.variables[varname]
slice = np.asarray(curvar)[1,::,::]

