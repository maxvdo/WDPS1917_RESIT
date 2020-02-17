

module load prun
module load python/3.5.2

export PYTHONPATH=/home/jurbani/trident/build-python

prun -t 18000 -np 1 python3.5 run_entity_linking.py node027:9200 "_" ./data/sample.warc.gz ./data/own_annotations.tsv > .log.txt

prun -np 1 python3.5 score_extended.py ./data/own_annotations.tsv ./predictions/predictions__.txt 

