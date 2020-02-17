ES_PORT=9200
ES_BIN=/var/scratch2/wdps1917/elasticsearch-2.4.1/bin/elasticsearch
IN_FILE= ./data/sample.warc.gz
OUT_FILE= ./data/own_annotations.tsv

>".es_log*"

module load prun
module load python/3.5.2

prun -t 10800 -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

echo "python path"
export PYTHONPATH=/home/jurbani/trident/build-python

echo "entity_linking starting"
prun -t 18000 -np 1 python3.5 run_entity_linking.py $ES_NODE:$ES_PORT "_" $IN_FILE $OUT_FILE > .log.txt

#echo "score"
#prun -np 1 python3.5 score_extended.py ./data/own_annotations.tsv ./predictions/predictions__.txt 
 
#kill $ES_PID
