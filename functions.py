import re,gzip,warc,requests
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from collections import OrderedDict

def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('\{<.*?>\}')
    return re.sub(clean, '', text)


def read_warc(path = './data/sample.warc.gz'):
    # Beautiful soup HTML to text
    with gzip.open(path, mode='rb') as gzf:
        count = 0
        cleantexts = []
        # url_list= []
        doc_ids = []

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

            cleantexts.append(text)
            # url_list.append(record.header.get('WARC-Target-URI'))
            doc_ids.append(record.header.get('WARC-TREC-ID'))

    return cleantexts,doc_ids

def do_elasticsearch(query="Vrije Universiteit", domain="localhost:9200", print_output=False, extended_info=False,size=5):
    if extended_info:
        url = 'http://%s/freebase/label/_search' % domain
        response = requests.get(url, params={'q': query, 'size': size})
        if response:
            response = response.json()
            return response.get('hits', {}).get('hits', [])
        else:
            return []
    else:
        id_labels = elasticsearch.search(domain, query)

        if print_output:
            for entity, labels in id_labels.items():
                print(entity, labels)

        return id_labels


def check_first_k_else_all(entity_word, response, k=5,q=10,min_score_value=2, min_ratio_value=0.82):
    # response = do_elasticsearch("Flash Player",extended_info=True) make sure extended_info = True on response argument input
    # Probably also give a paramete q giving the scores/rank for top q results
    # Assuming k is equal to q or smaller
    elastic_scores = []
    matching_ratio = []
    id_labels =  OrderedDict()#{}
    cancel_search = True
    for i, hit in enumerate(response):
        freebase_label = hit.get('_source', {}).get('label')
        freebase_id = hit.get('_source', {}).get('resource')
        id_labels.setdefault(freebase_id, set()).add(freebase_label)
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
                return []
    return [id_labels, elastic_scores, matching_ratio]


def check_skip_constraints(entity, list_symbols):
    for symbol in list_symbols:
        if symbol in entity:
            return True

    return False
