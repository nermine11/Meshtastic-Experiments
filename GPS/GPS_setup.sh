#!/bin/bash

#Update rPi and install packages
sudo apt update
sudo apt upgrade
# this isn't really necessary, maybe if you have a brand new pi
# sudo rpi-update
sudo apt install pps-tools gpsd gpsd-clients chrony

#Set up the Pi to release the console pins
#free the UART port (in our case /dev/ttyAMA0) for GPS communication,
sudo raspi-config nonint do_serial_cons 1

#Enable PPS
grep -qxF '# the next 3 lines are for GPS PPS signals' /boot/firmware/config.txt || \
echo '# the next 3 lines are for GPS PPS signals' | sudo tee -a /boot/firmware/config.txt > /dev/null
# Enable GPIO-based PPS (Pulse-Per-Second) on pin 4 for GPS timing
grep -qxF 'dtoverlay=pps-gpio,gpiopin=4' /boot/firmware/config.txt || \
echo 'dtoverlay=pps-gpio,gpiopin=4' | sudo tee -a /boot/firmware/config.txt > /dev/null
# Enable the UART serial interface hardware to communicate with the GPS module
grep -q '^enable_uart=' /boot/firmware/config.txt && \
sudo sed -i 's|^enable_uart=.*|enable_uart=1|' /boot/firmware/config.txt || \
echo 'enable_uart=1' | sudo tee -a /boot/firmware/config.txt > /dev/null
# Set default serial communication speed to 9600 baud to matches our GPS module
grep -q '^init_uart_baud=' /boot/firmware/config.txt && \
sudo sed -i 's|^init_uart_baud=.*|init_uart_baud=9600|' /boot/firmware/config.txt || \
echo 'init_uart_baud=9600' | sudo tee -a /boot/firmware/config.txt > /dev/null

#In /etc/modules, add ‘pps-gpio’ to a new line.
grep -qxF 'pps-gpio' /etc/modules || \
echo 'pps-gpio' | sudo tee -a /etc/modules > /dev/null

#Edit /etc/default/gpsd:
#Installing a GPS Daemon (gpsd)
#serial might be /dev/ttyS0
sudo sed -i 's|^DEVICES=""|#DEVICES=""|' /etc/default/gpsd
grep -q '^DEVICES=' /etc/default/gpsd && \
sudo sed -i 's|^DEVICES=.*|DEVICES="/dev/ttyAMA0 /dev/pps0"|' /etc/default/gpsd || \
echo 'DEVICES="/dev/ttyAMA0 /dev/pps0"' | sudo tee -a /etc/default/gpsd > /dev/null
# -n means start without a client connection (i.e. at boot)
grep -q '^GPSD_OPTIONS=' /etc/default/gpsd && \
sudo sed -i 's|^GPSD_OPTIONS=.*|GPSD_OPTIONS="-n"|' /etc/default/gpsd || \
echo 'GPSD_OPTIONS="-n"' | sudo tee -a /etc/default/gpsd > /dev/null

# also start in general
grep -q '^START_DAEMON=' /etc/default/gpsd && \
sudo sed -i 's|^START_DAEMON=.*|START_DAEMON="true"|' /etc/default/gpsd || \
echo 'START_DAEMON="true"' | sudo tee -a /etc/default/gpsd > /dev/null

# Automatically hot add/remove USB GPS devices via gpsdctl
grep -q '^USBAUTO=' /etc/default/gpsd && \
sudo sed -i 's|^USBAUTO=.*|USBAUTO="false"|' /etc/default/gpsd || \
echo 'USBAUTO="false"' | sudo tee -a /etc/default/gpsd > /dev/null


#configure chrony to use both NMEA and PPS signals
# SHM refclock is shared memory driver, it is populated by GPSd and read by chrony
# it is SHM 0
# refid is what we want to call this source = NMEA
# offset = 0.000 means we do not yet know the delay
# precision is how precise this is. not 1e-3 = 1 millisecond, so not very precision
# poll 0 means poll every 2^0 seconds = 1 second poll interval
# filter 3 means take the average/median (forget which) of the 3 most recent readings. NMEA can be jumpy so we're averaging here
grep -q '^refclock SHM 0 refid NMEA' /etc/chrony/chrony.conf && \
sudo sed -i '/^refclock SHM 0 refid NMEA/ c\refclock SHM 0 refid NMEA offset 0.000 precision 1e-3 poll 0 filter 3' /etc/chrony/chrony.conf || \
echo 'refclock SHM 0 refid NMEA offset 0.000 precision 1e-3 poll 0 filter 3 ' | sudo tee -a /etc/chrony/chrony.conf > /dev/null

# PPS refclock is PPS specific, with /dev/pps0 being the source
# refid PPS means call it the PPS source
# lock NMEA means this PPS source will also lock to the NMEA source for time of day info
# offset = 0.0 means no offset... this should probably always remain 0
# poll 3 = poll every 2^3=8 seconds. polling more frequently isn't necessarily better
# trust means we trust this time. the NMEA will be kicked out as false ticker eventually, so we need to trust the combo
grep -q '^refclock PPS /dev/pps0 refid PPS' /etc/chrony/chrony.conf && \
sudo sed -i '/^refclock PPS \/dev\/pps0 refid PPS/ c\refclock PPS /dev/pps0 refid PPS lock NMEA offset 0.0 poll 3 trust' /etc/chrony/chrony.conf || \
echo 'refclock PPS /dev/pps0 refid PPS lock NMEA offset 0.0 poll 3 trust' | sudo tee -a /etc/chrony/chrony.conf > /dev/null

# also enable logging by uncommenting the logging line
grep -qxF 'log tracking measurements statistics' /etc/chrony/chrony.conf || \
echo 'log tracking measurements statistics' | sudo tee -a /etc/chrony/chrony.conf > /dev/null

ensure start on boot
sudo systemctl stop gpsd.socket
sudo systemctl disable gpsd.socket
sudo systemctl enable gpsd.socket
sudo systemctl start gpsd.socket
sudo systemctl enable gpsd
sudo systemctl unmask gpsd
sudo service gpsd restart

#Restart chrony
sudo systemctl restart chrony
