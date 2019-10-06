import os
import time
import xmltodict
from elasticsearch5 import Elasticsearch

es = Elasticsearch()
#Set the path for the XML files
path = "Parsed files/"
#Set the path for the queries txt
Qfile = "testingQueries.txt"
counter = 0
mode = "other"
#Set mode to 'all' if you want to read the XML files, turn them into dictionaries and upload them to elasticsearch
#Set the mode to 'other' if you have already uploaded the data and you want to do the queries part only
if mode == "all":
    #Creates the index form
    #Sets the english analyzer to elasticsearch before inserting the data
    es.indices.create(
        index='test',
        ignore=400,
        body={
            'mappings': {
                'project': {
                    'properties': {
                        'rcn': {
                            'type': 'integer'
                        },
                        'acronym': {
                            'type': 'string'
                        },
                        'text': {
                            'type': 'string',
                            'analyzer': 'english',
                            'search_analyzer': 'english'
                        },
                        'identifier': {
                            'type': 'string'
                        }
                    }
                }
            }
        }
    )
    #Opens every single XML file in the folder
    for file in os.listdir(path):
        filename = os.path.join(path, file)
        #Reads the file and converts XML into dictionary
        with open(str(filename), 'r', encoding='utf-8') as fd:
            doc = xmltodict.parse(fd.read())
        counter += 1
        #Creates a dictionary with the modified XML data (text = title + objective) from the previous dictionary
        package = {
            'rcn': int(doc['project']['rcn']),
            'acronym': doc['project']['acronym'],
            'text': doc['project']['title'] + ' ' + doc['project']['objective'],
            'identifier': doc['project']['identifier']
        }
        if counter % 100 == 0:
            print("Files processed: "+str(counter))
        #Uploads the new dictionary to elasticsearch with an id determined by the rcn field in the xml file
        es.index(index='test', doc_type='project', id=package['rcn'], body=package)
    print("Files processed: "+str(counter))
    #Closes the indices - changes the settings to TF-IDF - opens the indices
    es.indices.close(index='test')
    es.indices.put_settings(
        index='test',
        body={
            'index': {
                'similarity': {
                    'default': {
                      'type': 'classic'
                    }
                }
            }
        }
    )
    es.indices.open(index='test')
    time.sleep(1)
#Opens the queries file and reads each line
fd = open(Qfile, 'r', encoding='utf-8')
#The file where we write the results for a size equal to 20
fd20 = open("es_results_20.txt", 'w', encoding='utf-8')
#The file where we write the results a size equal to 30
fd30 = open("es_results_30.txt", 'w', encoding='utf-8')
line = fd.readline()
#For each line-query it sends a search request to elasticsearch
counter = 0
while line:
    #Removes the tag (Q#) from the line-query
    #The first line has special characters that need to be removed (we cut 5 slots for the tag)
    #The rest of the lines have no special characters (we cut 4 slots for the tag)
    if counter == 0:
        tag = line[:5]
        tag = tag[1:4]
        line = line[5:]
        counter = 1
    else :
        tag = line[:4]
        tag = tag[:3]
        line = line[4:]
    #We need k+1 results for each test
    result20 = es.search(index='test', doc_type='project', body={'query': {'match': {'text': line}}, 'size': 21})
    result30 = es.search(index='test', doc_type='project', body={'query': {'match': {'text': line}}, 'size': 31})
    print(tag + " - 20:")
    remove = 1
    counter = 0
    for hit in result20['hits']['hits']:
        #The first element is not written in the txt file (it has the same text as the query)
        if remove == 1:
            remove = 0
        else:
            counter += 1
            #Writes the result
            fd20.write(tag + " Q0 " + str(hit['_id']) + " " + str(counter) + " " + str(hit['_score']) + " " + str(hit['_index']) + "\n")
            print(hit['_source']['rcn'])
    print("\n")
    print(tag + " - 30:")
    remove = 1
    counter = 0
    for hit in result30['hits']['hits']:
        # The first element is not written in the txt file (it has the same text as the query)
        if remove == 1:
            remove = 0
        else:
            counter += 1
            #Writes the result
            fd30.write(tag + " Q0 " + str(hit['_id']) + " " + str(counter) + " " + str(hit['_score']) + " " + str(hit['_index']) + "\n")
            print(hit['_source']['rcn'])
    print("\n")
    line = fd.readline()
