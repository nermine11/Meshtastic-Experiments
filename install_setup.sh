#!/bin/bash

##############################################################
##############################################################

# update
sudo apt update
sudo apt install -y python3-venv 


# install and activate the virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the Python packages needed inside the venv
sudo venv/bin/pip install --upgrade pip
sudo venv/bin/pip install --upgrade pytap2
sudo venv/bin/pip install --upgrade "meshtastic[cli]"
