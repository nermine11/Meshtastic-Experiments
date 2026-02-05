# start sending packets in a loop on boot
cat <<EOF | sudo tee /etc/systemd/system/send_on_boot.service
[Unit]
Description=Send Packets

[Service]
User=pi
WorkingDirectory=/home/pi/Meshtastic-Experiments
ExecStart=/home/pi/Meshtastic-Experiments/venv/bin/python /home/pi/Meshtastic-Experiments/send.py 2 5 ^all
Restart=no

[Install]
WantedBy=multi-user.target
EOF

cat <<EOF | sudo tee /etc/systemd/system/send_on_boot.timer
[Unit]
Description=timer for Sending packets

[Timer]
OnBootSec=5min
Unit=send_on_boot.service

[Install]
WantedBy=timers.target
EOF
