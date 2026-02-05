#!/bin/bash

##############################################################
# This script has to be placed in /home/pi/Meshtastic
##############################################################

# update
sudo apt update
sudo apt install -y python3-venv python3-numpy python3-matplotlib


# install and activate the virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the Python packages needed inside the venv
sudo venv/bin/pip install --upgrade python3-pip
sudo venv/bin/pip install --upgrade pytap2
sudo venv/bin/pip install --upgrade "meshtastic[cli]"
