version := "0.0.1"
scalaVersion := "2.10.4"
resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided",
  "com.github.fommil.netlib" % "all" % "1.1.2",
  "edu.ucar" % "cdm" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "grib" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "netcdf4" % "4.5.5" exclude("commons-logging", "commons-logging")
)

lazy val runTest = taskKey[Unit]("Test loading and saving of netcdf data")
runTest <<= (assembly in Compile) map {
  (jarFile: File) => s"spark-submit --driver-memory 4G --class org.apache.spark.mllib.linalg.distributed.netCDFTest ${jarFile} test.nc output.nc" !
}
