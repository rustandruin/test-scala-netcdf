name := "GribClimateConverter"
version := "0.0.1"
scalaVersion := "2.10.4"
resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.5.0" % "provided",
  "com.github.fommil.netlib" % "all" % "1.1.2",
  "com.databricks" %% "spark-avro" % "2.0.1",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided", // depends on the version of Hadoop you're using! use hadoop version command to determine
  "org.apache.commons" % "commons-compress" % "1.5",
  "log4j" % "log4j" % "1.2.14", 
  "edu.ucar" % "cdm" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "grib" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "netcdf4" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "org.msgpack" %% "msgpack-scala" % "0.6.11"
)

lazy val runTest = taskKey[Unit]("Test loading and saving of netcdf data")
runTest <<= (assembly in Compile) map {
  (jarFile: File) => s"spark-submit --driver-memory 4G --class org.apache.spark.mllib.linalg.distributed.netCDFTest ${jarFile} test.nc output.nc" !
}

val fieldnames = "TCDC_P8_L234_GLL0_avg,TCDC_P8_L224_GLL0_avg,TCDC_P8_L214_GLL0_avg,TCDC_P8_L200_GLL0_avg,TCDC_P0_L244_GLL0,CSDLF_P8_L1_GLL0_avg,CSULF_P8_L8_GLL0_avg,CSULF_P8_L1_GLL0_avg,ULWRF_P8_L8_GLL0_avg,ULWRF_P8_L1_GLL0_avg,DLWRF_P8_L1_GLL0_avg,ULWRF_P0_L1_GLL0,DLWRF_P0_L1_GLL0,CPRAT_P8_L1_GLL0_avg,ACPCP_P8_L1_GLL0_acc,NCPCP_P8_L1_GLL0_acc,APCP_P8_L1_GLL0_acc,PRATE_P8_L1_GLL0_avg,CAPE_P0_2L108_GLL0,CAPE_P0_L1_GLL0,CIN_P0_2L108_GLL0,CIN_P0_L1_GLL0,PLI_P0_2L108_GLL0,CWAT_P0_L200_GLL0,PWAT_P0_L200_GLL0,PWAT_P0_2L108_GLL0,TMP_P0_L100_GLL0,VVEL_P0_L100_GLL0,VGRD_P0_L100_GLL0,RH_P0_L100_GLL0,SPFH_P0_L100_GLL0"

lazy val runConvert = taskKey[Unit]("Convert Grib2 to parquet format")
runConvert <<= (assembly in Compile) map {
  (jarFile : File) => s"./runconvert.sh ${jarFile} ${fieldnames}" !
}

lazy val runTranspose = taskKey[Unit]("Transpose climate fat matrix to skinny matrix")
runTranspose <<= (assembly in Compile) map {
  (jarFile : File) => s"./runtranspose.sh ${jarFile}" !
}

val filename = "pgbh02.gdas.1979010100.grb2"
lazy val runTestDump = taskKey[Unit]("Extract a record from the parquet matrix and export it in msgpack format")
runTestDump <<= (assembly in Compile) map {
  (jarFile : File) => s"./runtestdump.sh ${jarFile} ${filename}" !
}

lazy val runTestMatrixMultiply = taskKey[Unit]("Test multiplying by A and A^T")
runTestMatrixMultiply <<= (assembly in Compile) map {
  (jarFile : File) => s"./runtestmultiply.sh ${jarFile}" !
}
