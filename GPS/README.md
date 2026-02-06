# To know which GPS antenna you are using

external : **,3

internal : **,2
 ```
gpsctl -x '$PGCMD,33,1*6C' /dev/ttyS0
/dev/ttyS0 identified as a MTK-3301 AXN_2.31_3339_13101700-5632 at 9600 baud.
pi@raspberrypi:~ $ gpspipe -r | grep PGTOP
$PGTOP,11,3*6F
$PGTOP,11,3*6F
$PGTOP,11,3*6F
 ```
We are using the external GPS antenna for better results.

# Setting the SHM offset

the SHM offset is a fixed time correction (in seconds) that tells Chrony to add or subtract from the GPS’s time signal to make it accurate and to compensate for the delay of messages.
Initially in GPS_setup.sh, the offset is 0 but we have to change it for better results.

For that we use statistics, after an hour or more of running chronny, run this command
```
sudo cat /var/log/chrony/statistics.log > GPS/GPS_statistics/chrony_statistics.log #keep the last ~100 lines only
python3 GPS/GPS_statistics/statistics.py
```
We get for example:
```
chrony Statistics Summary:
------------------------------
Number of IP Addresses: 22
Time Range: 2025-06-30 12:04:15 to 2025-07-02 14:11:37

Average Estimated Offset by IP:

NMEA: 4.23e-01

Median Estimated Offset by IP:

NMEA: 4.26e-01
```
So in our case, we take the value in the middle:
4.245 
we modify it in /etc/chrony/chrony.conf
```
sudo nano /etc/chrony/chrony.conf
"modify: refclock SHM 0 refid NMEA offset 4.245 precision 1e-3 poll 0 filter 3
sudo systemctl restart chrony
```
To understand more, follow this tutorial we used: https://austinsnerdythings.com/2025/02/14/revisiting-microsecond-accurate-ntp-for-raspberry-pi-with-gps-pps-in-2025/
