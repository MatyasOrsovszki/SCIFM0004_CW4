# SCIFM0004_CW4
 
Project to parallelise HZZ analysis using RabbitMQ message broker and Docker-compose to run multiple consumers processing data distributed by a producer and plotted by a collector.

To run please use: ./run.sh --consumers {number of consumers to run - default 12} --debug {True of False - default False, determines if info should be output by each consumer, producer, and collector} --href {default is set from the datahref.txt file, user can change the url within the file and saving it, or providing it here with the --href flag}

Example use:
./run.sh 
- Runs with default values

./run.sh --consumers 24 --debug True --href https://somedata.com/data/
- Runs with 24 consumers, debugging information set to print, and using data from https://somedata.com/data/

Graphing will be output in the same folder as run.sh is in, within a folder called output (this will be created if not available).