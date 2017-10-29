import couchdb
import logging
import string
import time
import re


def import_keywords(file_path):
    kw_lst = []
    with open(file_path, 'r') as infile:
        for line in infile:
            if line.startswith('#') or not line.strip():
                continue
            kw_lst.append(line.rstrip())
    return kw_lst


remove_punctuation_map = dict()
for char in string.punctuation:
    remove_punctuation_map[ord(char)] = ' '

def preprocess_twitter(text):
    text = re.sub(r'http\S+', '', text.encode('ascii','ignore').decode('ascii'))
    text = re.sub('\n', ' ', text)
    text = text.lower().translate(remove_punctuation_map)
    tokens = text.split()
    return tokens

def preprocess_instagram(text):
    text = text.lstrip('[')
    text = text.rstrip(']')
    tokens = text.split(', ')
    return tokens


class CouchDBConnection :
    def __init__(self):
        password = 'seven_eleven'
        couchdb_ip = '115.146.94.159'
        self.server = couchdb.Server("http://admin:{}@{}:5984".format(password, couchdb_ip))

server = CouchDBConnection().server
melb_db = server['melbourne']
new_db = server['pt_cluster']
stat_db = server['pt_cluster_builder_state']

general = import_keywords('general')
bus = import_keywords('bus')
tram = import_keywords('tram')
train = import_keywords('train')
ts_single = import_keywords('ts_single')
ts_double = import_keywords('ts_double')

def check_tag(token, tag_dict):
    pt_related = False
    if token in general:
        tag_dict['general'] = True
        pt_related = True
    elif token in bus:
        tag_dict['bus'] = True
        pt_related = True
    elif token in tram:
        tag_dict['tram'] = True
        pt_related = True
    elif token in train:
        tag_dict['train'] = True
        pt_related = True
    elif token in ts_single:
        tag_dict['ts'] = True
        pt_related = True
    return pt_related

def build_cluster_doc(old_doc, source):
    doc_id = old_doc['_id']
    timestamp = old_doc['timestamp']
    date_lst = old_doc['date']
    coordinates = old_doc['coordinates']
    user = old_doc[source]
    text = old_doc['text']
    suburb = old_doc['suburb']
    keyword_num = 0
    keyword_lst = []
    keyword_tag = {'general':False, 'bus':False, 'tram':False, 'train':False, 'ts':False}
    keyword_type = None
    keyword_type_lst = []
    if source == 'twitter':
        tokens = preprocess_twitter(text)
        for token in tokens:
            if check_tag(token, keyword_tag):
                keyword_num += 1
                keyword_lst.append(token)
        for phrase in ts_double:
            if phrase in text:
                keyword_tag['ts'] = True
                keyword_num += 1
                keyword_lst.append(phrase)
    elif source == 'instagram':
        if text != '[]':
            tokens = preprocess_instagram(text)
            for token in tokens:
                if check_tag(token, keyword_tag):
                    keyword_num += 1
                    keyword_lst.append(token)
    for key, value in keyword_tag.items():
        if value == True:
            keyword_type_lst.append(key)
    if len(keyword_type_lst) > 1:
        keyword_type = 'combined'
        new_doc = {
            '_id': doc_id,
            'timestamp': timestamp,
            'date': date_lst,
            'coordinates': coordinates,
            source: user,
            'text': text,
            'suburb': suburb,
            'keyword_number': keyword_num,
            'keywords': keyword_lst,
            'keyword_type': keyword_type,
            'keyword_types': keyword_type_lst
        }
        #logging.info("Got a new combined doc with text {}...".format(text))
        try:
            new_db.save(new_doc)
        except couchdb.http.ResourceConflict as e:
            pass
            #logging.info("{0}, {1}".format(e, doc_id))
    elif len(keyword_type_lst) == 1:
        keyword_type = keyword_type_lst[0]
        new_doc = {
            '_id': doc_id,
            'timestamp': timestamp,
            'date': date_lst,
            'coordinates': coordinates,
            source: user,
            'text': text,
            'suburb': suburb,
            'keyword_number': keyword_num,
            'keywords': keyword_lst,
            'keyword_type': keyword_type,
            'keyword_types': keyword_type_lst
        }
        #if (keyword_type != 'ts' and keyword_type != 'general'):
            #logging.info("Got a new {0} doc with text {1}...".format(keyword_type, text))
        try:
            new_db.save(new_doc)
        except couchdb.http.ResourceConflict as e:
            pass
            #logging.info("{0}, {1}".format(e, doc_id))


logging.basicConfig(format='%(asctime)s %(message)s %(module)s:%(lineno)d ',
                    filename='record.log',
                    level=logging.INFO)

if 'bootstrap_state' in stat_db:
    logging.info("Rebooting from state...")
    bootstrap_docs = stat_db['bootstrap_state']['docs']
    end_id = stat_db['bootstrap_state']['last_id']
    curr_seq = stat_db['bootstrap_state_seq']['seq']
    start_id = stat_db['bootstrap_state_seq']['id']
    logging.info("Rebooting from state - Starting at id : {0}, at seq : {1}".format(start_id, curr_seq))
else:
    logging.info("Starting from scratch...")
    changes = melb_db.changes(descending=True, feed='longpoll', limit=1)
    last_seq = changes['last_seq']
    logging.info("Last sequence is {}".format(last_seq))
    first = melb_db.view('_all_docs', descending=False, limit=1).rows
    start_id = first[0]['id']
    last = melb_db.view('_all_docs', descending=True, limit=3).rows
    end_id = last[2]['id']
    bootstrap_docs = melb_db.view('_all_docs', startkey=start_id, endkey=end_id, limit=10000).rows
    logging.info("Total number of documents fetched is {0}, starting at id : {1}".format(len(bootstrap_docs), start_id))
    curr_seq = 0
    start_id = bootstrap_docs[len(bootstrap_docs)-1]['id']
    bootstat_doc = {'_id': 'bootstrap_state', 'docs': bootstrap_docs, 'last_seq': last_seq, 'last_id': end_id}
    stat_db.save(bootstat_doc)
    currstat_doc = {'_id': 'bootstrap_state_seq', 'seq': curr_seq, 'id': start_id}
    stat_db.save(currstat_doc)
    changestat_doc = {'_id': 'changes_state', 'last_seq': last_seq}
    stat_db.save(changestat_doc)

logging.info("Processing documents from view...")
halt_signal = False
for inx in range(curr_seq, len(bootstrap_docs)):
    doc_id = bootstrap_docs[inx]['id']
    if doc_id == end_id:
        halt_signal = True
        logging.info("Halt signal turns to True...")
        break
    old_doc = melb_db[doc_id]
    if 'instagram' in old_doc:
        build_cluster_doc(old_doc, 'instagram')
    elif 'twitter' in old_doc:
        build_cluster_doc(old_doc, 'twitter')
    doc = stat_db['bootstrap_state_seq']
    doc['seq'] = inx
    doc['updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    stat_db.save(doc)

while halt_signal is False:
    bootstrap_docs = melb_db.view('_all_docs', startkey=start_id, endkey=end_id, limit=10000).rows
    logging.info("Total number of documents fetched is {0}, starting at id : {1}".format(len(bootstrap_docs), start_id))
    start_id = bootstrap_docs[len(bootstrap_docs)-1]['id']
    bootstat_doc = stat_db['bootstrap_state']
    bootstat_doc['docs'] = bootstrap_docs
    stat_db.save(bootstat_doc)
    currstat_doc = stat_db['bootstrap_state_seq']
    currstat_doc['seq'] = 0
    currstat_doc['id'] = start_id
    stat_db.save(currstat_doc)
    for inx in range(len(bootstrap_docs)):
        doc_id = bootstrap_docs[inx]['id']
        if doc_id == end_id:
            halt_signal = True
            logging.info("Halt signal turns to True...")
            break
        old_doc = melb_db[doc_id]
        if 'instagram' in old_doc:
            build_cluster_doc(old_doc, 'instagram')
        elif 'twitter' in old_doc:
            build_cluster_doc(old_doc, 'twitter')
        doc = stat_db['bootstrap_state_seq']
        doc['seq'] = inx
        doc['updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        stat_db.save(doc)


logging.info("Processing changes feed...")
while True:
    prev_seq = stat_db['changes_state']['last_seq']
    changes = melb_db.changes(descending=False, since=prev_seq, feed='longpoll', include_docs=True)
    changes_docs = changes['results']
    for changes_doc in changes_docs:
        old_doc = changes_doc['doc']
        if 'instagram' in old_doc:
            build_cluster_doc(old_doc, 'instagram')
        elif 'twitter' in old_doc:
            build_cluster_doc(old_doc, 'twitter')
    cs_doc = stat_db['changes_state']
    cs_doc['last_seq'] = prev_seq
    cs_doc['updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    stat_db.save(cs_doc)
    time.sleep(600)
