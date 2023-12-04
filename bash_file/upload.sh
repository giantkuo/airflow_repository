#!/bin/sh
sshpass -f password_file sftp -P 3040 pokemon@140.112.183.104 << !
        cd /nas-data/Animal/chicken_sensor
        mkdir hub1
        put ../data_tmp/*.csv hub1
        bye
!
rm -r ../data_tmp/*.csv
