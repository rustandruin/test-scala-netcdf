pssh -h /root/spark/conf/slaves 'rm -r /root/spark/work/*'
pssh -h /root/spark/conf/slaves 'rm -r /tmp/*.grb2'
pssh -h /root/spark/conf/slaves 'rm -r /tmp/*.nc'
pssh -h /root/spark/conf/slaves 'rm -r /mnt/*.grb2'
pssh -h /root/spark/conf/slaves 'rm -r /mnt/*.nc'

