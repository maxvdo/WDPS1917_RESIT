--------------------------------------------

# RESIT ASSIGNMENT WEB DATA PROCESSING SYSTEMS #

### MAX VAN DEN OETELAAR (2662601) ###

In the folder, you will find:

- Python scripts to run the assignment (described below).
- *"predictions"* folder, which include the named and linked entities (clueweb_id + entity label + freebase_id). I also included the predictions
   file from the last time I successfully ran the code. This is called *"predictions_reference"* and was run using the *run_all_normal.sh* (without spark).
- *"results"* folder, which includes a summary of the results. I also included the results file from the last time I successfully ran the code. 
   This is called *"results_predictions_reference"* and was run using the *run_all_normal.sh* (without spark).
- *"data"* folder, which includes the sample and annotation files.


The repository for the project: https://github.com/maxvdo/WDPS1917_RESIT.git

--------------------------------------------
 
## Python version ##
For our tests on DAS4, we used python 3.5.2 and locally 3.7.0 

------------------------------------------------------------------------------------------------------------
## Packages ##

### Pre-processing ###

- WARC package was used to open the warc files and easily extract content and key-ID for convenience
- This can be installed for PYTHON 3 with for example pip3.5 install --user warc3-wet
- For python 2, which was not tested, it's possible to use pip install warc
- The gzip package is used in combination with WARC package as well
- The "Beautifulsoup" package was used to extract the text and make it more clean
- Install command: pip install beautifulsoup4
- If an error occurs when using it and parsing, you may need to download lxml package: pip3.5 install lxml --user 

### NER detection ###
- NLTK package components (such as tokenize, POS tag and NE chunker) are used to perform NER. In particular, we used the following models to download their models one time only:
- pip3.5 install NLTK --user 

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')

- Note: If an error occurs about NLTK still, then you might have to install the package six: E.g pip3.5 install --user six

### Elasticsearch ###
- requests package needed pip3.5 install --user requests

### Trident ###
- Trident python bindings are used to sparql queries
- so set export PYTHONPATH=/home/jurbani/trident/build-python to import trident
- This is the reason why python 3.5 was used
- Numpy also needs to be the latest version in order to work

Other packages that are usually standard:
- difflib package to import SequenceMatcher to do comparison between strings
- operator package to import itemgetter to sort or to get the maximal tuples based on ith element

------------------------------------------------------------------------------------------------------------
## Folder and files ##

- data folder
    - sample.warc.gz, this is the default file (path) to do predictions with run_entity_linking_file.py
    - sample.annotations.tsv, some annotations of the sample file where we predict with 560 golden entities
    - own_annotations.tsv, some more (manually) annotations on top of the sample.annoations.tsv one with 886 Golden entities

- elasticsearch.py :
Contains the function to request a search and return its results

- functions.py
Contains all the functions used in the main file run_entity_linking.py. Functions are to clean the text, to do elasticsearch and some operations and functions to check if entities are discarded

- run_entity_linking.py
The main file to run everything, reads input such as elasticsearch domain and optional labels for filenames and optional paths to data. It defaults to the data repository that we assume and specified above. When predicting it automatically creates a folder predictions and saves it in there, and writes line by line while predicting (so you will have results when interrupted mid-run). The file has a standard name of predictions_. Results are saved in results folder with same name.

- run_entity_linking_spark.py
The main file to run with spark, with the first argument being the master URL or number of the master node on das4. The rest of the arguments are in the same sequence as in run_entity_linking.py

- score_extended.py
Used the same way as score.py, originally with golden file as first argument and predictions as second argument. Additionally, it shows the amount of mappings and how many were correct/incorrect.

------------------------------------------------------------------------------------------------------------

## Launch Scripts ##

- run_all_normal.sh
Script that runs all steps without use of spark. It starts an elasticsearch server from folder in /var/scratch2/wdps1917, just copied from the one in urbani path. It assumes defaults paths of the data folder as described above (arguments can be modified).

- prep_commands_spark.sh *(NOT USED)*
There are different ways to start a spark cluster using standalone or yarn manager for example. Here we manually start master and slaves, as with yarn it just kept running for us and did not end.
This script reserves the necessary nodes for master and slave nodes. In this script you can define the x amount of worker nodes in the for-loop that you want to reserve. Then it uses preserve -llist to give an overview of the nodes and can also look at the reserve IDs to be specified in run_all_spark.sh to run commands on specific nodes. So this is run before run_all_spark.sh.

- run_all_spark.sh *(NOT USED)*
After reserving the nodes and following the method to start things manually, we must define the nodes in this script to distinguish which one is master and which ones are slave nodes. We do this by defining the node number of the master and its reserve ID such that we can launch the start-master script (path also to be defined if different) on it. We also must define the reserve IDs of the slave nodes to start the start-slave script and stop it at the end. The reserve IDs can be found by using preserve -llist. 
Also, here we assume the elasticsearch server is started beforehand and we define the node and the port. Having defined all this in this script, we can run this script. It will start master and slave nodes, execute the main script of run_entity_linking_spark.py and stop the master and slave nodes afterwards.
Another way to start the cluster is to run the start-all script on the master and define the slave nodes in the conf/slaves file of the spark application folder. This method did not work for us while testing because master and worker nodes used different python versions. We tried to solve this with defining PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the directory of the python/3.5.2 which is a shared package. But workers do no have access to this because the module of python/3.5.2 is not loaded in for them. It could be the case that this way of starting the master and slave nodes does not give a proper connection, which would explain the missing speedup to be described in the results section. In this default script we have the "_spark" prefix to distinguish from the other.

Note: In run_entity_linking and run_entity_linking_spark, we save the predictions in predictions folder and the score in results folder as described above. The score is saved by directing the output with the ">" command. To still print the score, we run the score_extended.py separately outside it again. Here it assumes the same prefix as used before. If the argument in run_entity_linking is changed, one could see another output if old predictions are present. So one can check the scores in results folder just to be sure. 

------------------------------------------------------------------------------------------------------------
## Running the code

If one assumes the default paths and structure as described above then you can simply run:

###Without Spark:
- `bash run_all_normal.sh`.
    - Elastic Search PORT can be changed in *ES_PORT*.
    - Elastic Search server instance can be changed in *ES_BIN*.
    - Input warc file can be changed in *IN_FILE*.
    - **In some cases there will be `permission denied` for the required files in IN_FILE and OUT_FILE, in that case add the respective file paths to the designated arguments in the python execution script of run_all_normal.sh. eg: prun -t 18000 -np 1 python3.5 run_entity_linking.py $ES_NODE:$ES:PORT "_" ./data/sample.warc.gz ./data/own_annotations.tsv > .log.txt **
- This will produce `predictions_.txt` in the *predictions* folder and `results_predictions_.txt` in the *results* folder.
- The above code runs and gives f1 score based on handmade annotations in `/data/own_annotations.tsv`.
- *To get score based on user defined warc files, run `score.py` as:*
    - `python3.5 score.py {gold_file_path} ./data/predictions__.txt`
    - `python3.5 score.py {gold_file_path} {annotated_result_path}`, for a different annotated output path.


The reason we also give an annotations file is because at the end, run_entity_linking computes the score, saves it in the results folder with the same prefix and also writes the amount of time in seconds it took to compute. So here we get predictions/predictions_test_run.txt and results/results_predictions_test_run.txt.

With Spark:
As mentioned before we reserve nodes beforehand by running bash prep_commands_spark.sh
Then we note down the node number of the master and the reserve IDs of master and slave nodes in run_all_spark.sh
This is needed to start the master and slave nodes, which needs the master URL to connect.
Then similar to run_entity_linking.py, but with a master URL or master node number as first argument, we run the main script:

python3.5 run_entity_linking_spark.py <Master URL or master node number> <Required Elasticsearch domain> <optional label after prefix> <optional warc path> <optional annotations path>
or
spark-submit run_entity_linking_spark.py <Master URL or master node number> <Required Elasticsearch domain> <optional label after prefix> <optional warc path> <optional annotations path>

Note that we assume 5 hours max in our script file, with the sample file a little more than 1 hour should be enough. Time can be changed if needed on a node.

Our files on das4 can be found at:

*`/var/scratch2/wdps1917/WDPS1917_RESIT` folder on das4* 

------------------------------------------------------------------------------------------------------------
## Original (old) method ##

First we pre-process the text because we want to make sure what we put in our NER part is really visible text. In order to do this, we make use of the Beautifulsoup, gzip and warc packages to extract the correct information. For example, we remove HTML tags from text and get the proper TREC-ID from the document to be used in the prediction.

This text is then given to our NLTK tagger, which is a tagger that is relatively fast compared to others. It can detect entities reasonably well and also does this with multiple spans. Furthermore, it is famous package that is supported well and easily installed.

In case the NLTK tagger recognises a wrong or useless entity, we check if there are any useless symbols such as "<",">","/",":" (or other symbols that might give trouble to elasticsearch).

NLTK can regonise entities well, but also "recognises" a lot more than other models (low precision). To filter these, we look at the response when we put it into elasticsearch.

We skip the entity if elasticsearch returns no results, or if it does not pass the following 2 criteria:
- First check on the top k labels/terms that elasticsearch returns and look at their score, if none of them are higher than the minimal score of 2, we discard it.
- For each result, we do similarity matching with the entity mention and the entity label from elasticsearch to see how well they match. If this value is below a 0.82 ratio we also discard it.

If the top k results all pass this test, it will keep on going through each element of the response from elasticsearch. Word labels from this response are matched with a freebase ID. We use OrderedDict to keep track of the rank of freebase_IDs from the response and the set of word labels that match a certain freebase ID. The idea is that a popular freebase ID will likely have more word labels/terms linked to it than a less known freebase ID.
For the top q results of the response from elasticsearch, usually q >= k, we also keep track of elasticsearch scores and these similarity matching ratios. These will be ranked based on score and given a certain score value to the corresponding freebase ID.

After having a set of freebase IDs for a word and its set of labels, we give score points for the top r freebase IDs from elasticsearch. Score points are given based on the elasticsearch score and similarity ratio ranking as mentioned above and queries.
For example, the elasticsearch scores are ranked and each freebase_id gets a score based on their ranking. Number one gets 1, number two gets 1 - (1/ranks) etc. The same is done for the ratio.

Then, we do some general sparql queries such as:

- Title similarity matching with the entity mention, which we found important and thus is multiplied times 3 as a weight. With this title matching, we also look at subwords ratios to see if we can obtain a higher max ratio. 

- The amount in the set of labels linked to a freebase ID from elasticsearch is also quite important. We divide the amount of labels by 5 (if 100 labels then divide by 5 is 20), then divide by 20 and times 2. We do this in order to let it have a maximal score of 2, as it is less important than title matching ratio. 

- Some more general queries are for example the amount of equivalent webpages (same score assigning as how many labels) (a standard small value addition of 1/r).

After that, we run some more specific queries based on their entity label from the tagger. This could be "GPE", "GSP", "LOCATION", "PERSON", "ORGANISATION" or "FACILITY. It gets a score addition of 3 if it matches with its label category. If it does not get a matching result, then it moves to a secondary category type. In that case, the addition that it will get if it gets a match is reduced by 1. All the queries can be found below (they are also present in the run_entity_linking.py file).

Finally, the freebase_id with the highest score is used for prediction.

------------------------------------------------------------------------------------------------------------
## Results, old and attempted improvements

Originally, for the 1464 documents and using our own annotations labels, we have:

- 886 golden entities, 
- 37899 predictions
- 731 mappings 
- 452 of them are correct mappings. 

This leads to:
- precision of 0.011926
- recall of 0.51015
- F1 score of 0.0233

The precision is low because we predict a lot but we only have so much annotations. This originally takes around 4900 seconds.

With implementation of spark *(not used in das cluster)*, we ran it using 3 workers, same results as same method, but the times are less consistent. One run it ran within 3500 seconds and other time in 5200 seconds. We mainly have some doubts if the cluster is correctly setup and making use of workers rather than the program not working. (Not sure how to look at the WEB UI with das4). As of right now, we know 3 ways of starting the cluster and launching application, but for 2 of the 3 ways we do not know how to solve, which is why we went the first method of manually starting the launch scripts. But method 2 should be closely related to method 1 but gives error of not using the same python version as mentioned before.

Thus to be sure to increase the scalability a bit, we also remove some queries and ranking to make it a bit faster:
- We reduced the query label order to maximum of 2 categories. If an entity does not match with 2 categories it will not go a third or fourth one to execute its queries.
- We do not look at subwords of titles anymore to do the similarity ratio matching. We only compare once between the two strings.
- We do not look at twitter and media presence anymore for general queries
- For PERSON queries, we do not query anymore if it is a nndb, celebrity and if in entertainment sector
- We do not sort and give scores for ranking of elastic scores and matching ratio similarity of elasticsearch response anymore.

With the use of spark we then get:
- 37899 predictions, same as before, but slightly less performance results, 
- 731 mappings
- 398 correct

This leads to
- precision of 0.0105
- recall 0.4492
- F1 of 0.0205

This finished in 3700 seconds.

Without use of spark:
- It took around 3750 seconds.

------------------------------------------------------------------------------------------------------------
## FINAL RESULTS ##

Finally, I reduced the elastic search results from 1000 to 5 based on the top scores. In the code, this is denoted by 'r'. Again, I ran it without the use of Spark. I got the following results:
 

-  886 golden entities
-  38941 predictions
-  731 mappings 
-  379 of them are correct mappings. 

This leads to:
- precision of 0.0099
- recall of 0.4277
- F1 score of 0.0195

This finished in 2147 seconds.

Slightly worse results, but we do decrease the execution time to run it (which was the biggest problem of the previous version of the assignment).

------------------------------------------------------------------------------------------------------------
