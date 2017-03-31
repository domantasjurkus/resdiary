#!/bin/bash
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz -O ~/spark.tgz
tar -xvzf ~/spark.tgz -C ~/
pip install -r requirements.txt