#pyspark
import pyspark
from operator import add

#Reading input
import gzip, warc

#Preprocess and NER Tag
import re
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.chunk import ne_chunk
#import spacy

#Elasticsearch
import elasticsearch,requests,os,sys

#Trident
import numpy as np
import trident
import json

#Comparison (and sorting)
from difflib import SequenceMatcher
#import itertools
from operator import itemgetter
#from functions_spark import *
import time
from collections import OrderedDict

def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('\{<.*?>\}')
    return re.sub(clean, '', text)


def read_warc(path = './data/sample.warc.gz'):
    # Beautiful soup HTML to text
    with gzip.open(path, mode='rb') as gzf:
        #cleantexts = []
        #doc_ids = []
        text_and_ids = []

        for i, record in enumerate(warc.WARCFile(fileobj=gzf)):
            if i == 0:
                continue
            # cleantexts.append(BeautifulSoup(record.payload.read(), 'lxml').text)

            soup = BeautifulSoup(record.payload.read(), 'lxml')
            for script in soup(["script", "style"]):
                script.extract()  # rip it out
            text = soup.get_text()

            # break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            # break multi-headlines into a line each

            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

            # drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            text = remove_html_tags(text)

            #cleantexts.append(text)
            #doc_ids.append(record.header.get('WARC-TREC-ID'))
            doc_id = record.header.get('WARC-TREC-ID')
            text_and_ids.append( (text,doc_id) )

    return text_and_ids#cleantexts,doc_ids

def tag_with_NLTK(text_and_id):
    text,id = text_and_id
    set_entities_tag = {(' '.join(c[0] for c in chunk), chunk.label() ) for chunk in ne_chunk(pos_tag(word_tokenize(text))) if hasattr(chunk, 'label') }

    return (set_entities_tag,id)

def words_check(set_entities_tag_and_id):
    set_entities_tag,id = set_entities_tag_and_id
    final_set = set()

    for word, label in set_entities_tag:

        if len(word) < 1 or check_skip_constraints(word):
            continue
        final_set.add((word,label))

    return final_set,id

def check_skip_constraints(entity):
    entity_skip_symbols = ['\n', "<", ">", '(', ')', "/", ":", "=", "NOT", "AND", "OR", "UTF-8", "NoneType"]
    for symbol in entity_skip_symbols:
        if symbol in entity:
            return True

    return False

def do_elasticsearch(set_entities_tag_and_id):
    set_entities_tag,id = set_entities_tag_and_id
    url = 'http://%s/freebase/label/_search' % elasticsearch_domain
    final_set = []
    for word, label in set_entities_tag:
        response = requests.get(url, params={'q': word, 'size': 1000})
        if response:
            response = response.json()
            #return response.get('hits', {}).get('hits', [])
            response_result = response.get('hits', {}).get('hits', [])
            final_set.append((word,label,response_result))

    return final_set,id

def check_first_k_else_all(set_entities_tag_and_elastic_and_id):

    set_entities_tag_and_elastic,id = set_entities_tag_and_elastic_and_id
    #entity_word, response
    # response = do_elasticsearch("Flash Player",extended_info=True) make sure extended_info = True on response argument input
    # Probably also give a paramete q giving the scores/rank for top q results
    # Assuming k is equal to q or smaller
    k = 5
    q = 10
    min_score_value = 2
    min_ratio_value = 0.82
    final_set = []

    for entity_word, label, response in set_entities_tag_and_elastic:
        elastic_scores = []
        matching_ratio = []
        id_labels =  OrderedDict()#{}
        cancel_search = True
        for i, hit in enumerate(response):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            id_labels.setdefault(freebase_id, set()).add(freebase_label) #Becomes a set of values for freebase id
            # print(freebase_label)
            if i < q:
                score_value = hit.get('_score', {})
                # print(score_value)
                elastic_scores.append( (freebase_id,score_value) )

                ratio_similiarity = SequenceMatcher(None, entity_word.lower(), freebase_label.lower()).ratio()
                # print(ratio_similiarity)
                matching_ratio.append( (freebase_id,ratio_similiarity) )

                if i < k:
                    if cancel_search and (score_value > min_score_value):
                        cancel_search = False

                    if cancel_search and (ratio_similiarity > min_ratio_value):
                        cancel_search = False

            if i == k and cancel_search:
                break
        if not cancel_search:
            final_set.append( (entity_word,label,id_labels,elastic_scores, matching_ratio) )

    return final_set,id


def queries_and_get_best_id(set_info_and_id):
    set_info,id= set_info_and_id
    full_output = ""
    #---------------------- Queries-----------------------------------------------------------------------------------------
    # General
    sparql_query_title = "select distinct ?obj where { \
        <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/key/wikipedia.en_title> ?obj . \
        } "  # % (freebase_id)

    sparql_query_n_webpage = "select distinct ?obj where { \
        <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> ?obj . \
        } "  # % (freebase_id)

    # Is social media active (Not consistent for persons) #Avg 2 or 1
    sparql_query_media_presence = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/ns/common.topic.social_media_presence> ?obj .\
        } "  # % (freebase_id)

    # Not yahoo and disney , Avg 1
    # Is social media active (Not consisten for persons?)
    sparql_query_twitter = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/key/authority.twitter> ?obj .\
        } "  # % (freebase_id)

    # Avg 1 or 2
    # Is social media active (Not consisten for persons?)  #New york times of Flash player bijv
    sparql_query_website = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/ns/common.topic.official_website> ?obj .\
    } "  # % (freebase_id)

    # ------------Location-----------------------------------------------------------------------------------------------------

    sparql_query_location = "select distinct * where {\
     { <http://rdf.freebase.com/ns%s> ?rel <http://rdf.freebase.com/ns/location.location>  .  }\
     UNION \
     { <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/ns/base.biblioness.bibs_location.loc_type> ?type . } \
      } "  # % (freebase_id,freebase_id)

    # ----------Person------------------------------------------------------------------------------------------------------

    sparql_query_person = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> ?rel <http://rdf.freebase.com/ns/people.person> .\
        } "  # % (freebase_id)

    # Scenario schrijver Tony Gilroy is a not a celebrity and not a notable name either.
    sparql_query_person_nndb = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> ?rel <http://rdf.freebase.com/ns/user.narphorium.people.nndb_person> .\
        } "  # % (freebase_id)

    # Jeremy Renner is notable name but does not have this type celebrity
    sparql_query_person_celeb = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> ?rel <http://rdf.freebase.com/ns/base.popstra.celebrity> .\
        } "  # % (freebase_id)

    # Tony Gilroy (and Matt damon) has these properties, politician not:
    sparql_query_entertainment = "select distinct * where {\
     { <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/key/source.entertainmentweekly.person> ?name  .  }\
     UNION \
     { <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/key/source.filmstarts.personen> ?name2 . } \
      } "  # % (freebase_id,freebase_id)

    # --------Organization------------------------------------------------------------------------------------------------------

    # Should be the first one if company
    sparql_query_company = "select distinct * where { \
        <http://rdf.freebase.com/ns%s> <http://rdf.freebase.com/key/authority.crunchbase.company> ?obj . \
        } "  # % (freebase_id)
    #Skipped other ones
    #--------Other---------------------------------------------------------------------------------------------------------------

    # Also Flash player etc.
    sparql_query_inanimate = "select distinct * where { \
       <http://rdf.freebase.com/ns%s> ?rel <http://rdf.freebase.com/ns/base.type_ontology.inanimate> .\
        } "  # % (freebase_id)

    # ------------------------------------------------------------------------------------------------------------------------

    query_label_order = {"GPE": ["GPE", "ORGANIZATION"],
                         "GSP": ["GPE", "ORGANIZATION"],
                         "LOCATION": ["GPE", "ORGANIZATION"],
                         "PERSON": ["PERSON", "GPE"],
                         "ORGANIZATION": ["ORGANIZATION", "GPE"],
                         "FACILITY": ["ORGANIZATION","GPE"],
                         }
    r = 10
    small_value = 0.5
    title_multiplier = 3
    label_multiplier = 2
    matching_label_value = 3
    non_match_value = 2
    score_step_value = 1 / r
    sparql_path = '/home/jurbani/data/motherkb-trident'
    db = trident.Db(sparql_path)

    final_set = set()
    for word_info in set_info:
        word, label, set_response, elastic_scores, matching_ratio = word_info
        elastic_id_labels = list(set_response)

        total_scores = {}
        label_order = query_label_order[label]

        for freebase_id in elastic_id_labels[:r]: 

            current_score_of_id = 0

            # Do Sparql query
            modified_id = freebase_id[:2] + "." + freebase_id[3:]

            # General
            query = sparql_query_title % (modified_id)
            #output_query = sparql(sparql_domain, query)
            output_query = json.loads(db.sparql(query))
            results = int(output_query["stats"].get("nresults",-1))
            if results == -1: #Incorrect ID otherwise
                total_scores[freebase_id] = 0
                continue

            #print(word,freebase_id)
            #print("results title ", results)
            if results > 0:
                #print("heb title gevonden\n")
                title = output_query["results"]["bindings"][0]["obj"]["value"]  # Assumming only 1 title on index 0
                title = title.translate({ord('"'): None, ord('$'): " ", ord('_'): " "})
                lowered_title = title.lower()
                lowered_word = word.lower()
                max_ratio = SequenceMatcher(None, lowered_word, lowered_title).ratio()

                # Extra check for aliases or per word
                #splitted_title = lowered_title.split()
                #n_words = len(splitted_title)
                #if n_words == len(word):
                #    initials = ""
                #    for w in splitted_title:
                #        letter = w[0]
                #        initials += letter
                #    ratio = SequenceMatcher(None, lowered_word, initials).ratio()
                #    if ratio > max_ratio:
                #        max_ratio = ratio
                #elif n_words > 1:
                #    for w in splitted_title:
                #        ratio = SequenceMatcher(None, w, lowered_title).ratio()
                #        if ratio > max_ratio:
                #            max_ratio = ratio

                #tuple_info_ratio = (freebase_id, max_ratio)
                #sorted_tuples_title_ratio.append(tuple_info_ratio)
                #total_scores[freebase_id] = max_ratio * 30
                current_score_of_id += max_ratio * title_multiplier #


            # Add n_labels to list, where score is given for ranking on that is given after the for loop
            n_labels = len(set_response[freebase_id])
            #tuple_info = (freebase_id, n_labels)
            #sorted_tuples_n_labels.append(tuple_info)
            #total_scores[freebase_id] += min(int(n_labels / 5),20) / 10
            #total_scores[freebase_id] = n_labels
            current_score_of_id += (min(int(n_labels / 5),20) / 20 ) * label_multiplier

            query = sparql_query_n_webpage % (modified_id)
            #output_query = sparql(sparql_domain, query)
            output_query = json.loads(db.sparql(query))
            n_results = int(output_query["stats"]["nresults"])
            #print("Results webpage ",n_results)
            #tuple_info_n_results = (freebase_id, n_results)
            #sorted_tuples_n_pages.append(tuple_info_n_results)
            current_score_of_id += min(int(n_results / 5),20) / 20

            #query = sparql_query_media_presence % (modified_id)
            #output_query = sparql(sparql_domain, query)
            #output_query = json.loads(db.sparql(query))
            #n_results = int(output_query["stats"]["nresults"])
            #print("results media ",n_results)
            #current_score_of_id += (n_results > 0) * small_value

            #query = sparql_query_twitter % (modified_id)
            #output_query = sparql(sparql_domain, query)
            #output_query = json.loads(db.sparql(query))
            #n_results = int(output_query["stats"]["nresults"])
            #print("results twitter ", n_results)
            #current_score_of_id += (n_results > 0) * small_value

            query = sparql_query_website % (modified_id)
            #output_query = sparql(sparql_domain, query)
            output_query = json.loads(db.sparql(query))
            n_results = int(output_query["stats"]["nresults"])
            #print("results website ", n_results)
            current_score_of_id += (n_results > 0) * small_value

            match = False

            for i, label in enumerate(label_order):

                addition = matching_label_value - i #value 3 if correct, 2 if second, 1 if third or lower

                if label == "GPE":
                    query = sparql_query_location % (modified_id, modified_id)
                    #output_query = sparql(sparql_domain, query)
                    output_query = json.loads(db.sparql(query))
                    n_results = int(output_query["stats"]["nresults"])
                    #print("results location ",n_results)
                    if n_results:
                        current_score_of_id += addition
                        match = True

                elif label == "PERSON":
                    query = sparql_query_person % (modified_id)
                    #output_query = sparql(sparql_domain, query)
                    output_query = json.loads(db.sparql(query))
                    n_results = int(output_query["stats"]["nresults"])
                    #print("results person ",n_results)
                    if n_results:
                        current_score_of_id += addition
                        match = True

                    #query = sparql_query_person_nndb % (modified_id)
                    #output_query = sparql(sparql_domain, query)
                    #output_query = json.loads(db.sparql(query))
                    #n_results = int(output_query["stats"]["nresults"])
                    #print("results nndb ", n_results)
                    #if n_results:
                    #    current_score_of_id += small_value #addition
                    #    match = True

                    #query = sparql_query_person_celeb % (modified_id)
                    #output_query = sparql(sparql_domain, query)
                    #output_query = json.loads(db.sparql(query))
                    #n_results = int(output_query["stats"]["nresults"])
                    #print("results celeb ", n_results)
                    #if n_results:
                    #    current_score_of_id += small_value #addition
                    #    match = True

                    #query = sparql_query_entertainment % (modified_id, modified_id)
                    #output_query = sparql(sparql_domain, query)
                    #output_query = json.loads(db.sparql(query))
                    #n_results = int(output_query["stats"]["nresults"])
                    #print("results enter ", n_results)
                    #if n_results:
                    #    current_score_of_id += small_value #addition
                    #    match = True
                elif label == "ORGANIZATION":
                    query = sparql_query_company % (modified_id)
                    #output_query = sparql(sparql_domain, query)
                    output_query = json.loads(db.sparql(query))
                    n_results = int(output_query["stats"]["nresults"])
                    #print("results org ", n_results)
                    if n_results:
                        current_score_of_id += addition
                        match = True

                elif label == "OTHER":
                    query = sparql_query_inanimate % (modified_id)
                    #output_query = sparql(sparql_domain, query)
                    output_query = json.loads(db.sparql(query))
                    n_results = int(output_query["stats"]["nresults"])
                    #print("Other results ",n_results)
                    if n_results:
                        current_score_of_id += 1

                if match:
                    break

            total_scores[freebase_id] = current_score_of_id

        #elastic_scores.sort(key=itemgetter(1))
        #matching_ratio.sort(key=itemgetter(1))

        #for i, tuple_value in enumerate(elastic_scores):
        #    freebase_id = tuple_value[0]
        #    if freebase_id in total_scores:
        #        score = ((i + 1) * score_step_value)
        #        total_scores[freebase_id] += score

        #for i, tuple_value in enumerate(matching_ratio):
        #    freebase_id = tuple_value[0]
        #    if freebase_id in total_scores:
        #        score = ((i + 1) * score_step_value)
        #        total_scores[freebase_id] += score

        best_id_key = max(total_scores.items(), key=itemgetter(1))[0]
        final_set.add((word,best_id_key))
        #line = doc_key + '\t' + word + '\t' + best_id_key + "\n"
        #full_output += line

    return final_set, id

def prepare_output(set_word_freebaseid_and_docid):
    word_freebaseid_pairs, doc_key = set_word_freebaseid_and_docid
    full_output = ""

    for word,freebase_id in word_freebaseid_pairs:
        line = doc_key + '\t' + word + '\t' + freebase_id + "\n"
        full_output += line

    return full_output

if __name__ == '__main__':

    try:
        master = str(sys.argv[1])
        conf = pyspark.SparkConf()
        if len(master) > 3: #Full url
            conf.setMaster(master)
            #spark = pyspark.sql.SparkSession.builder.master(master).appName('wdps1917').getOrCreate()
        else: #Only number
            full = 'spark://node' + master + '.cm.cluster:7077'
            conf.setMaster(full)
            #spark = pyspark.sql.SparkSession.builder.master(full).appName('wdps1917').getOrCreate()
        conf.setAppName('wdps1917')
        sc = pyspark.SparkContext(conf=conf)
        #sc = spark.sparkContext()

        elasticsearch_domain  = sys.argv[2]
    except Exception as e:
        print('Usage: python3.5 run_entity_linking.py <Spark Master URL or node number> <Elasticsearch_Domain> <optional_label_for_prediction>  <optional_warc_path> <optional_annotations_path')
        sys.exit(0)

    start = time.time()

    #Default paths
    annotations_path = "./data/sample.annotations.tsv"
    warc_path = "./data/sample.warc.gz"
    strategy = ""
    if len(sys.argv) > 3:
        strategy  = sys.argv[3] #Name label for file

        if len(sys.argv) == 5:
            warc_path = sys.argv[4]

        if len(sys.argv) == 6:
            annotations_path = sys.argv[5]

    # Hyperparameters, how many results of (elasticsearch and trident) to compare (for discard and popularity)
    k = 5
    q = 10
    r = 10
    min_score = 2
    min_ratio_value = 0.82
    small_value = 0.5
    title_multiplier = 3
    label_multiplier = 2
    matching_label_value = 3
    non_match_value = 2
    score_step_value = 1 / r

    #sparql_domain = "localhost:1234"
    sparql_path = '/home/jurbani/data/motherkb-trident'
    db = trident.Db(sparql_path)

    #cleantexts,doc_ids = read_warc(warc_path)
    #n_docs = len(cleantexts)
    cleantexts_and_ids = read_warc(warc_path)
    #n_docs = len(cleantexts_and_ids)

    #cleantexts_rdd = sc.parallelize( cleantexts[:15] ) #Texts of all docs
    #set_entities_tag_rdd = cleantexts_rdd.map(tag_with_NLTK) #Set of (word,label) for each text, label being the entity tag
    cleantexts_and_ids_rdd = sc.parallelize( cleantexts_and_ids) #Texts of all docs (and ids) only used for writing at the end
    set_entities_tag_rdd = cleantexts_and_ids_rdd.map(tag_with_NLTK) #Set of (word,label) for each text, label being the entity tag

    set_entities_tag_rdd = set_entities_tag_rdd.map(words_check) #Set of (word,label) for each text filtered
    set_entities_tag_and_elastic_rdd = set_entities_tag_rdd.map(do_elasticsearch) #Set of (word,label,elastic_response) for each text
    all_rdd = set_entities_tag_and_elastic_rdd.map(check_first_k_else_all) #Set of (word,label,elastic_id_labels,elastic_scores,matching_ratio) for each text
    word_and_id_rdd = all_rdd.map(queries_and_get_best_id) #Set of (word,best_freebase_id) for each text
    output_rdd = word_and_id_rdd.coalesce(1).map(prepare_output) #All lines for predictions

    if not os.path.exists(os.path.dirname("./predictions/")):
        os.makedirs(os.path.dirname("./predictions/"))
    file_name = "predictions_" + strategy + ".txt"
    file_path = "./predictions/" + file_name

    #output_rdd.saveAsTextFile( "test_out")#file_path )
    predictions = output_rdd.reduce(add)
    #l = output_rdd.collect()
    #print(len(l))
    
    file = open(file_path, "w+", encoding="utf-8")
    file.write(predictions)
    file.close()

    end = time.time()
    time_info = "Finished in %d seconds " % ((end - start))
    print( time_info )

    if not os.path.exists(os.path.dirname("./results/")):
        os.makedirs(os.path.dirname("./results/"))

    os.system("(python score_extended.py " + annotations_path + " " + file_path + "& echo " + "\"" + time_info + "\")" + "> ./results/results_" + file_name)
    sc.stop()
