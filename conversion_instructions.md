# GRIB to Parquet Conversion
 This assumes the tar files containing the GRIB files have been uploaded into S3.
 
## Launch the Cluster
Launch a cluster with 16 TB of HDFS; I typically use the Oregon region because I want to use placement groups, which require all the nodes to be in the same zone, which in turn requires high availability. This is typically best in Oregon for the r3.8xlarge instances, plus they are much cheaper there.

Always use the latest version of Spark because Parquet support is hinky and gets better. I used Spark 1.5.2, and found that you need to use `hadoop-version=yarn` instead of `hadoop-version=2` because otherwise `distcp` doesn't work for copying data over from S3.
```bash
$SPARKHOME/ec2/spark-ec2 -k GittensOregon -i ~/.ssh/GittensOregon.pem --region=us-west-2 --instance-type=r3.8xlarge -s 29 --placement-group=pcavariants --copy-aws-credentials --hadoop-major-version=yarn --spot-price=2.8 launch largeclimatecluster
```
This assumes you have defined the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, and propagates them to the EC2 instances so Hadoop can access S3. I use the spot-price 2.8 because this is the current on-demand price for r3.8xlarge instances, so is the highest sensible bid. Of course you'll need to create a placement-group named pca-variants through the EC2 dashboard before running this command.

If you see that you're stuck at less than 29 slaves granted for several minutes, check the status of the spot-requests in the EC2 dashboard. If you see some errors in any of the requests (typically, bad parameters --- this seems to be a bug somwhere in either EC2 or Spark's ec2 launch script --- or insufficient availability), then you'll need to Ctrl+C to cancel the spark-ec2 script, then run `$SPARKHOME/ec2/spark-ec2 destroy --region=us-west-2 largeclimatecluster` to release all the slaves that were granted, and rerun the launch command. It may take several tries for this to succeed, so be patient. If you don't ask for a placement group, you'll typically need less tries.

Once the launch script has finished (maybe 30 minutes after you've been granted all the slaves?), take note of the dns name of the master that's printed out in the final few lines, or alternatively, use `$SPARKHOME/ec2/spark-ec2 get-master --region=us-west-2 largeclimatecluster` to print the master's dns name. Then `export MASTER=...` for convenience. SSH into the cluster with `ssh -i ~/.ssh/GittensOregon.pem root@$MASTER.`

Note the follow ports that you can load in your browser at `http://$MASTER:$PORT`:
- Spark UI: 8080
- HDFS UI: 50070
- Yarn UI: 8088
- Ganglia UI: 5080

## Retrieve the GRIB tar files

_FIRST_! Reduce the `dfs.replication` factor in `ephemeral-hdfs/conf/hdfs-site.xml` from 3 to 1 and restart HDFS. Otherwise you will run out of HDFS space during processing (If you accidentally forget to do this, you can stop HDFS and change `dfs.replication` and restart similarly to below, then use `hdfs dfs -setrep -R -w 1 /` to reduce the replication factor of all files already in HDFS). I like to work in multiple `screen` sessions so I can seamlessly recover from dropped connections, so I first set the `PATH` before running any other commands. 
```sh
export PATH=$PATH:/root/spark/bin:/root/ephemeral-hdfs/bin/
screen -S gettars
ephemeral-hdfs/sbin/stop-yarn.sh; ephemeral-hdfs/sbin/stop-dfs.sh
vim ephemeral-hdfs/conf/hdfs-site.xml # make dfs.replication = 1
ephemeral-hdfs/sbin/start-dfs.sh; ephemeral-hdfs/sbin/start-yarn.sh
hadoop distcp s3n://agittens/CFSRArawtars CFSRArawtars
```
Now `Ctrl-A D` to detach this screen session. The transfer may take an hour or more to complete, and can be monitored through the Yarn UI.

## Setup the conversion code
We need to install sbt and retreive and compile the conversion code. We'll do this concurrently in another screen session. We set up the git user name and email in case we want to make commits (skippable if you're not making any commits), and create a log directory and upload a file to HDFS, both of the which the conversion code assumes have been done before it is run. Next we run the `sbt assembly` command to get sbt to pull in all the required packages and compile the code.

```sh
screen -S setupconversioncode
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo 
yum -y install sbt
git clone https://github.com/rustandruin/test-scala-netcdf.git
git config --global user.name "Alex Gittens"
git config --global user.email "gittens@icsi.berkeley.edu"
cd test-scala-netcdf
mkdir eventLogs
hdfs dfs -put completefilelist
sbt assembly
```
After entering the last command, detach the screen with `Ctrl+A D` so we can continue the setup while this is running.

## Retrieve NCL and install on all nodes

The conversion code assumes NCL is installed on all nodes. Use `uname -a` and `gcc -v` to determine your GCC version and whether your platform is 64- or 32-bit. Next, go to `ncl.ucar.edu` and follow the instructions for downloading the appropriate _non-openpdap_ binary of ncl to `/root` on the main node --- namely, download on your local machine the wget script file that is given to you, then transfer this to the main node and run it: I transfer it by opening it on my local machine, copying all, then pasting into a vim window on the main node (with `:set paste` turned on first) and saving it as wgetscript.sh, then running `bash wgetscript.sh` in the `/root` directory. From this point, we install NCL and copy it to all the nodes (it requires csh)

```sh
cd /root
mkdir ncl
tar -xvz -C ncl -f ncl_ncarg-6.3.0.Linux_RHEL6.4_x86_64_nodap_gcc447.tar.gz # or whatever the name of the file retrieved is
rm -f ncl_ncarg-6.3.0.Linux_RHEL6.4_x86_64_nodap_gcc447.tar.gz
yum -y install csh
pscp.pssh -vr -h /root/spark/conf/slaves -p 5 ncl /root/ncl
pssh -h /root/spark/conf/slaves 'yum -y install csh'
```

## Run the conversion
Once all the data has been retrieved (use `screen -r gettars` to check for the completion indicated by a return to the command prompt) to the local HDFS, run the conversion code (still inside the screen session).

```sh
cd /root/test-scala-netcdf
sbt runConvert
```
This will probably take 18 -- 24 hours to generate the final parquet file needed to compute the EOFs of the climate data. Use `Ctrl-A D` to detach the screen, logout from the master, and you can monitor the progress through the Spark UI.     