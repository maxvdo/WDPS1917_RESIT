module load prun
module load python/3.5.2

export PYTHONPATH=/home/jurbani/trident/build-python
export SPARK_HOME=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export HADOOP_HOME=/cm/shared/package/hadoop/hadoop-2.7.6/
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/:$LD_LIBRARY_PATH
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#export PYSPARK_PYTHON=/cm/shared/package/python/3.5.2/bin/python
#export PYPSPARK_DRIVER_PYTHON=/cm/shared/package/python/3.5.2/bin/python

#ES_BIN=/var/scratch2/wdps1917/elasticsearch-2.4.1/bin/elasticsearch
ES_PORT=9200
ES_NODE=node027
SPARK_MASTER_NUM=051
MASTER_RESERVE_ID=693699
MASTER_SCRIPT_PATH=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/sbin/start-master.sh
SLAVE_SCRIPT_PATH=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/sbin/start-slave.sh
STOP_MASTER=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/sbin/stop-master.sh
STOP_SLAVE=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/sbin/stop-slave.sh
START_ALL_CONF=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/sbin/start-all.sh
STOP_ALL_CONF=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/sbin/stop-all.sh

SLAVE_RESERVES=693700,693701,693702

#Launching master script
prun -np 1 -reserve $MASTER_RESERVE_ID bash $MASTER_SCRIPT_PATH
#prun -np 1 -reserve $MASTER_RESERVE_ID bash $START_ALL_CONF #Define worker nodes in conf/slaves but somehow uses different python versions which and defining with PYSPARK_PYTHON does not work as they are not loaded in for the workers so no access.


#Start sloves and connect to master
IFS=,
for SLAVE_RESERVE in $SLAVE_RESERVES
do
	prun -np 1 -reserve $SLAVE_RESERVE bash $SLAVE_SCRIPT_PATH spark://node$SPARK_MASTER_NUM.cm.cluster:7077
done

#prun -t 18000 -np 1 python3.5 run_entity_linking_spark.py $SPARK_MASTER_NUM $ES_NODE:$ES_PORT "_spark" ./data/sample.warc.gz ./data/own_annotations.tsv > .log.txt
bash /var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7/bin/spark-submit run_entity_linking_spark.py $SPARK_MASTER_NUM $ES_NODE:$ES_PORT "_spark" ./data/sample.warc.gz ./data/own_annotations.tsv 

prun -np 1 python3.5 score_extended.py ./data/own_annotations.tsv ./predictions/predictions__spark.txt 
 
prun -np 1 -reserve $MASTER_RESERVE_ID bash $STOP_MASTER
#prun -np 1 -reserve $MASTER_RESERVE_ID bash $STOP_ALL_CONF

#IFS=,
for SLAVE_RESERVE in $SLAVE_RESERVES
do
	prun -np 1 -reserve $SLAVE_RESERVE bash $STOP_SLAVE
done
