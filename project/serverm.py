import flask
import pymongo
from flask import Flask, request, jsonify
from bson import ObjectId
from datetime import datetime
 
app = flask.Flask(__name__)

l = {}
c=0
l1 = {}
class TripleStore:
    """Class to manage triple store operations using MongoDB."""

    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.client.yagodb
        self.collection = self.db["mycollection"]
    
        # self.dic={}
        file_a = "update_log.txt"+str(1)
        with open(file_a, "r") as file_a:
            lines_a = file_a.readlines()
            c=len(lines_a)

            for line_a in lines_a:
                a=line_a.split(":")     
                #print(a)
                a_timestamp=a[0]+":"+a[1]+":"+a[2]
                parts = line_a.split(",")
                a_subject = parts[0].split(":")[-1].strip()
                a_predicate = parts[1].split(":")[-1].strip()
                a_object = parts[2].split(":")[-1].strip()
                l[a_subject,a_predicate]=a_timestamp
        # print("hi")
        # print(c)
	
    def query(self, subject):
        """Retrieve all triples for a given subject."""
        triples = list(self.collection.find({"subject": subject}))
        # Convert ObjectId to string in each document and format as dictionary
        results = []
        for triple in triples:
            triple_dict = {
                'subject': triple['subject'],
                'predicate': triple['predicate'],
                'object': triple['object']
            }
            results.append(triple_dict)

        # print(self.dic)
        return results

    def update(self, subject, predicate, obj):
        """Update or insert a new triple into the database."""
        self.collection.update_one(
            {"subject": subject, "predicate": predicate},
            {"$set": {"object": obj}},
            upsert=True
        )
        
        
        self._write_to_log(subject, predicate, obj, 1)

    def List(self):
        l.update(l1)
    def merge(self,id, source_id):
        """Merge data from another server."""
        file_a = "update_log.txt" + str(id)
        file_b = "update_log.txt" + str(source_id)
        
        # Open both files in read mode
        with open(file_a, "r") as file_a, open(file_b, "r") as file_b:
            lines_a = file_a.readlines()
            lines_b = file_b.readlines()
            
            # updated_lines = []
            for line_a in lines_a:
                # a_timestamp, a_subject, a_predicate, a_object = line_a.strip().split()
                a=line_a.split(":")
                print(a)
                a_timestamp=a[0]+":"+a[1]+":"+a[2]
                parts = line_a.split(",")
                a_subject = parts[0].split(":")[-1].strip()
                a_predicate = parts[1].split(":")[-1].strip()
                a_object = parts[2].split(":")[-1].strip()
                found = False
                key = (a_subject,a_predicate)
                if key in l:
                    tb = l[key]
                    a_time = datetime.strptime(a_timestamp, "%Y-%m-%d %H:%M:%S")
                    if tb:
                        b_time = datetime.strptime(tb, "%Y-%m-%d %H:%M:%S")
                        if b_time > a_time:
                            found = True  # Ignore update
                        
                
                    
    #             for line_b in reversed(lines_b):
    #                 # b_timestamp, b_subject, b_predicate, b_object = line_b.strip().split()
    #                 a=line_b.split(":")
    #                 b_timestamp=a[0]+":"+a[1]+":"+a[2]
    #                 # print(b_timestamp)
    #                 a_time = datetime.strptime(a_timestamp, "%Y-%m-%d %H:%M:%S")
    #                 b_time = datetime.strptime(b_timestamp, "%Y-%m-%d %H:%M:%S")
    # # Extract subject, predicate, and object
    #                 parts = line_b.split(",")
    #                 b_subject = parts[0].split(":")[-1].strip()
    #                 b_predicate = parts[1].split(":")[-1].strip()
    #                 b_object = parts[2].split(":")[-1].strip()
                    # if a_subject == b_subject and a_predicate == b_predicate:
                    #     if b_time > a_time:
                    #         found = True  # Ignore update
                    #         break
                if not found:
                    # with open(file_a, "w") as file_a:
                    #     file_a.writelines(updated_lines)
                    store.update(a_subject,a_predicate,a_object)

        store.List()
            # Print or perform other operations as required
            # print("Line from file_a:", line_a)
            # print("Line from file_b:", line_b)


                

    def _write_to_log(self, subject, predicate, obj, c):
        """Write update information to a log file."""
        k = str(c)
    
        with open("update_log.txt" + k, "a") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            l1[subject,predicate]=timestamp
            file.write(f"{timestamp}: Subject: {subject}, Predicate: {predicate}, Object: {obj}\n")

store = TripleStore()

@app.route('/query/<subject>', methods=['GET'])
def handle_query(subject):
    results = store.query(subject)
    return flask.jsonify(results)

@app.route('/update', methods=['POST'])
def handle_update():
    data = flask.request.json
    print(c)
    # if c>1:
    #     return jsonify({"message":"Log file limit reached, please do some updates manually"}), 500
    file_a="update_log.txt"+str(1)
    with open(file_a,"r") as file_a:
        lines_a = file_a.readlines()
        if (len(lines_a) > 200):
                return jsonify({"message":"Log file limit reached, please do some updates manually"}), 500
    store.update(data['subject'], data['predicate'], data['object'])
    store.List()
    return flask.jsonify({"message": "Triple updated"}), 200

@app.route('/merge', methods=['POST'])
def handle_merge():
    data = flask.request.json
    store.merge(data['id'], data['source_id'])

    return flask.jsonify({"message": "Merge completed"}), 200

if __name__ == '__main__':
    app.run(debug=True,port=5001)
