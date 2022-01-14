#!/bin/bash

echo "hello Dataeaze!"

# To start spark master
start-master.sh

# To start spark slaves
start-slaves.sh

# To check status of java processes
jps


# To submit the application
spark-submit --master spark://localhost:7077 /home/sunbeam/Desktop/Dataeaze-Coding-Test/spark_code.py


# To execute this script
#	terminal> bash spark_bash_script.sh
# OR
# 	terminal> chmod +x spark_bash_script.sh
#	terminal> ./spark_bash_script.sh