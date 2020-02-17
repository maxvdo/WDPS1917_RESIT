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
#from sparql import sparql
import numpy as np
import trident
import json

#Comparison (and sorting)
from difflib import SequenceMatcher
#import itertools
from operator import itemgetter
from functions import *
import time

if __name__ == '__main__':

    try:
        elasticsearch_domain  = sys.argv[1]
    except Exception as e:
        print('Usage: python3.5 run_entity_linking.py <Elasticsearch_Domain> <optional_label_for_prediction>  <optional_warc_path> <optional_annotations_path')
        sys.exit(0)
    
    start = time.time()
    
    annotations_path = "./data/sample.annotations.tsv"
    warc_path = "./data/sample.warc.gz"
    strategy = ""
    if len(sys.argv) > 2:
        strategy  = sys.argv[2]
        
        if len(sys.argv) == 4:
            warc_path = sys.argv[3]
            #cleantexts, doc_ids = read_warc(warc_path)
            
        if len(sys.argv) == 5:
            annotations_path = sys.argv[4]
    #else:
    cleantexts, doc_ids = read_warc(warc_path)  #Assuming path = './data/sample.warc.gz'

    n_docs = len(cleantexts)

    #sparql_domain = "localhost:1234"
    sparql_path = '/home/jurbani/data/motherkb-trident'
    db = trident.Db(sparql_path)

    # Hyperparameters, how many results of (elasticsearch and trident) to compare (for discard and popularity)
    k = 5
    q = 10
    r = 5
    min_score = 2
    min_ratio_value = 0.82
    small_value = 0.5
    title_multiplier = 3
    label_multiplier = 2
    matching_label_value = 3
    non_match_value = 2
    score_step_value = 1 / r

    # Tagger
    tagger = "NLTK"

    entity_skip_symbols = ['\n', "<", ">", '(', ')', "/", ":", "=", "NOT", "AND", "OR", "UTF-8", "NoneType"];


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

    # Handig voor bijv ook voor Flash player

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

    if not os.path.exists(os.path.dirname("./predictions/")):
        os.makedirs(os.path.dirname("./predictions/"))

    #strategy = "" 
    file_name = "predictions_" + strategy + ".txt"
    file_path = "./predictions/" + file_name

    file = open(file_path, "w+", encoding="utf-8");

    # Document level
    for doc_idx, text in enumerate(cleantexts):

        #if ( (doc_idx + 1) % 1) == 0:
        #    print("Processing document %d out of %d" % (doc_idx+1,n_docs))

        doc_key = doc_ids[doc_idx];

        # NER
        if tagger == "NLTK":
            # set_entities_tag = tag_with_NLTK(text)
            set_entities_tag = {(' '.join(c[0] for c in chunk), chunk.label()) for chunk in
                                ne_chunk(pos_tag(word_tokenize(text))) if hasattr(chunk, 'label')}
        #elif tagger == "Spacy":
        #    set_entities_tag = {(ent.text.strip(), ent.label_) for ent in spacy_model(text).ents if
        #                        ent.label_ not in skip}
        # elif tagger == "Stanford":
        # set_entities_tag = tag_with_stanford(text)

        # print(set_entities_tag)

        # Go through each entity found
        for word, label in set_entities_tag:

            # Skip entity if have following constraints
            if len(word) < 1 or check_skip_constraints(word,entity_skip_symbols): #or len(word.split()) > 7) :
                continue

            response = do_elasticsearch(word, domain=elasticsearch_domain, extended_info=True)

            # If no elasticsearch results
            if len(response) == 0:
                continue

            info = check_first_k_else_all(word, response, k=k,q=q,min_score_value=min_score,min_ratio_value=min_ratio_value)

            # If it did not pass the minimal requirements of scores
            if len(info) == 0:
                continue

            set_response, elasic_scores, matching_ratio = info

            #print(word,label,"\n")

            # For top q results, see which terms has most terms redirect to id and satisfy sparql queries:
            elasticsearch_freebase_ids = list(set_response)
            #sorted_tuples_n_labels = []
            #sorted_tuples_title_ratio = []
            #sorted_tuples_n_pages = []
            total_scores = {}

            label_order = query_label_order[label];

            for freebase_id in elasticsearch_freebase_ids[:r]:
                
                # Add n_labels to list, where score is given for ranking on that is given after the for loop
                #n_labels = len(set_response[freebase_id])
                #tuple_info = (freebase_id, n_labels)
                #sorted_tuples_n_labels.append(tuple_info)
                #total_scores[freebase_id] += min(int(n_labels / 5),20) / 10
                #total_scores[freebase_id] = n_labels
                
                current_score_of_id = 0; 
                
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
                ##output_query = sparql(sparql_domain, query)
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

                total_scores[freebase_id] = current_score_of_id;
                
            #elasic_scores.sort(key=itemgetter(1))
            #matching_ratio.sort(key=itemgetter(1))
                
            '''
            # best_id_key = max_id
            # If normalize on rank, sort them based on amount of labels
            #sorted_tuples_n_labels.sort(key=itemgetter(1))  # Ascending, The worst one is at index 0
            #sorted_tuples_title_ratio.sort(key=itemgetter(1))  # Ascending, The worst one is at index 0
            #sorted_tuples_n_pages.sort(key=itemgetter(1))  # Ascending, The worst one is at index 0
            #elasic_scores.sort(key=itemgetter(1))
            #matching_ratio.sort(key=itemgetter(1))

            for i, tuple_value in enumerate(sorted_tuples_n_labels):
                freebase_id = tuple_value[0]
                score = ((i + 1) * score_step_value) * label_multiplier
                total_scores[freebase_id] += score

            for i, tuple_value in enumerate(sorted_tuples_title_ratio):
                freebase_id = tuple_value[0]
                score = ((i + 1) * score_step_value) * title_multiplier
                total_scores[freebase_id] += score
            
            for i, tuple_value in enumerate(sorted_tuples_n_pages):
                freebase_id = tuple_value[0]
                score = ((i + 1) * score_step_value)
                total_scores[freebase_id] += score
            '''
            #for i, tuple_value in enumerate(elasic_scores):
            #    freebase_id = tuple_value[0]
            #    if freebase_id in total_scores:
            #        score = ((i + 1) * score_step_value)
            #        total_scores[freebase_id] += score
            
            #for i, tuple_value in enumerate(matching_ratio):
            #    freebase_id = tuple_value[0]
            #    if freebase_id in total_scores:
            #        score = ((i + 1) * score_step_value)
            #        total_scores[freebase_id] += score
            #'''

            best_id_key = max(total_scores.items(), key=itemgetter(1))[0]

            # highest_index = scores.index(max(scores))
            line = doc_key + '\t' + word + '\t' + best_id_key + "\n"
            # print(line)
            file.write(line)
            # print(highest_value)

    file.close()
    end = time.time()
    time_info = "Finished in %d seconds " % ((end - start))
    print( time_info )

    if not os.path.exists(os.path.dirname("./results/")):
        os.makedirs(os.path.dirname("./results/"))

    os.system("(python score_extended.py " + annotations_path + " " + file_path + "& echo " + "\"" + time_info + "\")" + "> ./results/results_" + file_name)
