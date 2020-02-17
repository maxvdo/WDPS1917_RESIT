module load prun
module load python/3.5.2

export SPARK_HOME=/var/scratch2/wdps1917/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

#Reserve master node and n slave nodes, define amount of slave nodes in the for loop range

for VARIABLE in {0..3}
do
	preserve -t 18000 -np 1 
done

preserve -llist
