from datetime import datetime
import flask
import happybase

app = flask.Flask(__name__)
l = {}
l1 = {}
def connect_to_hbase(host='localhost', port=9090):
    """
    Connects to HBase and returns the connection object.
    """
    try:
        connection = happybase.Connection(host=host, port=port)
        connection.open()
        print("Connected to HBase")
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        return None

class TripleStore:
    """ Class to manage triple store operations using HBase. """

    def __init__(self):
        self.connection = connect_to_hbase()
        self.table_name = b'yago_db'  # Adjust the table name
        self.table = self.connection.table(self.table_name)
        file_a = "update_log.txt"+str(3)
        with open(file_a, "r") as file_a:
            lines_a = file_a.readlines()
            for line_a in lines_a:
                a=line_a.split(":")     
                #print(a)
                a_timestamp=a[0]+":"+a[1]+":"+a[2]
                parts = line_a.split(",")
                a_subject = parts[0].split(":")[-1].strip()
                a_predicate = parts[1].split(":")[-1].strip()
                a_object = parts[2].split(":")[-1].strip()
                l[a_subject,a_predicate]=a_timestamp
    def query(self, subject):
        """Retrieve all triples for a given subject."""
        formatted_results = []
        print("Hi")
        for row_key, row_data in self.table.scan():
            if row_data.get(b'a:subject', b'').decode('utf-8') == subject:
                triple_dict = {
                    "subject": subject,
                    "predicate": row_data[b'a:predicate'].decode('utf-8'),
                    "object": row_data[b'a:object'].decode('utf-8')
                }
                formatted_results.append(triple_dict)
                print("Done")
        return formatted_results



    def update(self, subject, predicate, obj):
        print("Updating.....")
        """ Update or insert a new triple into the database. """
        subject_bytes = subject.encode()
        predicate_bytes = predicate.encode()
        obj_bytes = obj.encode()
    
    # Iterate through rows to check if the combination of subject and predicate already exists
        for row_key, row_data in self.table.scan():
            if (row_data.get(b'a:subject') == subject_bytes and
                    row_data.get(b'a:predicate') == predicate_bytes):
                # Update the existing row with the new object
                data = {
                    b'a:subject': subject_bytes,
                    b'a:predicate': predicate_bytes,
                    b'a:object': obj_bytes
                }
                self.table.put(row_key, data)
                self._write_to_log(subject, predicate, obj, 3)
                # count = sum(1 for _ in self.table.scan())
                # print(count)
                return
    
    # If the combination of subject and predicate does not exist, create a new entry
    # Find the next available row key (assuming row keys are sequential)
        count = sum(1 for _ in self.table.scan())
        new_row_key = str(count + 1).encode()        
    # Add a new entry with the generated row key
        data = {
            new_row_key: {
                b'a:subject': subject_bytes,
                b'a:predicate': predicate_bytes,
                b'a:object': obj_bytes
            }
        }
        self.table.put(new_row_key, data[new_row_key])
        self._write_to_log(subject, predicate, obj, 3)
    
    def List(self):
        l.update(l1)
        l1.clear()
    def merge(self,id, source_id):
        print("merging.....")
        """Merge data from another server."""
        file_a = "update_log.txt" + str(id)
        file_b = "update_log.txt" + str(source_id)
        # print("xyz")
        with open(file_a, "r") as file_a, open(file_b, "r") as file_b:
            lines_a = file_a.readlines()
            lines_b = file_b.readlines()

            for line_a in lines_a:
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
    
            if not found:
                self.update(a_subject,a_predicate,a_object)

        self.List()


    def _write_to_log(self, subject, predicate, obj, c):
        """Write update information to a log file."""
        k = str(c)
        with open("update_log.txt" + k, "a") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            l1[subject,predicate]=timestamp
            file.write(f"{timestamp}: Subject: {subject}, Predicate: {predicate}, Object: {obj}\n")

    def close(self):
        """ Close the HBase connection. """
        self.connection.close()

@app.route('/query/<subject>', methods=['GET'])
def handle_query(subject):
    store = TripleStore()
    results = store.query(subject)
    store.close()
    return flask.jsonify(results)

@app.route('/update', methods=['POST'])
def handle_update():
    data = flask.request.json
    store = TripleStore()
    file_a="update_log.txt"+str(3)
    with open(file_a,"r") as file_a:
        lines_a = file_a.readlines()
    if (len(lines_a) > 200):
        return flask.jsonify({"message":"Log file limit reached, please do some updates manually"}), 500
    store.update(data['subject'], data['predicate'], data['object'])
    store.List()

    # store.close()
    return flask.jsonify({"message": "Triple updated"}), 200

@app.route('/merge', methods=['POST'])
def handle_merge():
    data = flask.request.json
    store = TripleStore()
    store.merge(data['id'],data['source_id'])
    
    return flask.jsonify({"message": "Merge completed"}), 200

if __name__ == '__main__':
    app.run(debug=True,port=5003)
