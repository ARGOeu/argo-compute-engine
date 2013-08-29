# Fire up AR engine daily at 3PM
0 15 * * * /usr/libexec/ar-compute/ar-compute.sh >> /var/log/ar-compute/log
