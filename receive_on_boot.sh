# start sending packets in a loop on boot
cat <<EOF | sudo tee /etc/systemd/system/send_packets.service
[Unit]
Description=Send Packets

[Service]
User=pi
WorkingDirectory=/home/pi/Meshtastic-Experiments
ExecStart=/home/pi/Meshtastic-Experiments/venv/bin/python /home/pi/Meshtastic-Experiments/receive.py
Restart=no

[Install]
WantedBy=multi-user.target
EOF

cat <<EOF | sudo tee /etc/systemd/system/send_packets.timer
[Unit]
Description=timer for Sending packets

[Timer]
OnBootSec=5min
Unit=send_packets.service

[Install]
WantedBy=timers.target
EOF
