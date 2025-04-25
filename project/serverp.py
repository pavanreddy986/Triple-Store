from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from configparser import ConfigParser

app = Flask(__name__)

l = {}
l1 = {}

class Config:
    """ Configuration handler class for reading database settings. """

    @staticmethod
    def load(filename="database.ini", section="postgresql"):
        parser = ConfigParser()
        parser.read(filename)
        if parser.has_section(section):
            return {param[0]: param[1] for param in parser.items(section)}
        else:
            raise Exception('Section {0} not found in the {1} file.'.format(section, filename))

class TripleStore:
    """ Class to manage triple store operations using a PostgreSQL database. """

    def __init__(self):
        self.db_params = Config.load()
        self.conn = self.connect_db()
        file_a = "update_log.txt"+str(2)
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

    def connect_db(self):
        """ Connect to the PostgreSQL database server. """
        try:
            return psycopg2.connect(**self.db_params)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return None

    def query(self, subject):
        """Retrieve all triples for a given subject."""
        with self.conn.cursor() as cur:
            cur.execute('SELECT * FROM yago2 WHERE subject = %s', (subject,))
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # Get column names
            formatted_results = []
            for row in results:
                formatted_results.append(dict(zip(columns, row)))
            return formatted_results


    def update(self, subject, predicate, object):
        """ Update or insert a new triple into the database. """
        print("xyz")
        try:
            with self.conn.cursor() as cur:
                cur.execute('''
                    WITH upsert AS (
                        UPDATE yago2
                        SET object = %s
                        WHERE subject = %s AND predicate = %s
                        RETURNING *
                    )
                    INSERT INTO yago2 (subject, predicate, object)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM upsert);
                ''', 
                (object, subject, predicate, subject, predicate, object))
                self.conn.commit()
                _write_to_log(subject,predicate,object,2)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def List(self):
       l.update(l1)
       l1.clear()
        
    def close(self):
        """ Close the database connection. """
        if self.conn:
            self.conn.close()

@app.route('/query/<subject>', methods=['GET'])
def handle_query(subject):
    store = TripleStore()
    results = store.query(subject)
    store.close()
    return jsonify(results)

@app.route('/update', methods=['POST'])
def handle_update():
    data = request.json
    # print(data)
    store = TripleStore()
    file_a="update_log.txt"+str(2)
    with open(file_a,"r") as file_a:
        lines_a = file_a.readlines()
    if (len(lines_a) > 200):
        return jsonify({"message":"Log file limit reached, please do some updates manually"}), 500
    store.update(data['subject'], data['predicate'], data['object'])
    store.List()
    store.close()
    return jsonify({"message": "Triple updated"}), 200

@app.route('/merge', methods=['POST'])
def merge_data():
    data = request.json
    store=TripleStore()
    sid=data['source_id']
    id=data['id']
    file_a="update_log.txt"+str(id)
    file_b="update_log.txt"+str(sid)
    lines_b = l
    with open(file_a, "r") as file_a, open(file_b, "r") as file_b:
        lines_a = file_a.readlines()
        # lines_b = file_b.readlines()
        # lines_b = l
        # print(len(lines_b), " is the current length")
        # updated_lines = []
        for line_a in lines_a:
            # a_timestamp, a_subject, a_predicate, a_object = line_a.strip().split()
            print(len(lines_b), " is the present length")
            a=line_a.split(":")
            # print(a)
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
                store.update(a_subject,a_predicate,a_object)
    store.List()
    # store = TripleStore()
    store.close()
    # store.close()
    return jsonify({"message": "Merge completed"}), 200

def _write_to_log(subject, predicate, object_,c):
        k=str(c)
        
        """Write update information to a log file."""
        with open("update_log.txt"+k, "a") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            l1[subject,predicate]=timestamp
            # l1.append(f"{timestamp}: Subject: {subject}, Predicate: {predicate}, Object: {object_}\n")
            file.write(f"{timestamp}: Subject: {subject}, Predicate: {predicate}, Object: {object_}\n")

if __name__ == '__main__':
    app.run(debug=True,port=5002)