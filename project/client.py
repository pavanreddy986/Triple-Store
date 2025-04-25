import requests
from datetime import datetime
# timestamp=0
class TripleStoreAPI:
    """Client API for interacting with the TripleStore servers."""
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.log_file = "update_log.txt"
    
    def query_subject(self, subject):
        """Query triples by subject."""
        response = requests.get(f"{self.base_url}/query/{subject}")
        return response.json() if response.ok else {"error": "Failed to query data"}

    def update_triple(self, subject, predicate, object_):
        """Update a triple."""
        data = {"subject": subject, "predicate": predicate, "object": object_}
        response = requests.post(f"{self.base_url}/update", json=data)
        if response.ok:
            # self._write_to_log(subject, predicate, object_,c)
            return response.json()
        else:
            return "failed"+str(response.json())

    def merge_data(self, id,c):
        """Merge data from another server."""
        data = {"source_id":c,"id":id}
        
        response = requests.post(f"{self.base_url}/merge", json=data)
        return response.json() if response.ok else {"error": "Failed to merge data"}


    def _write_to_log(self, subject, predicate, object_,c):
        k=str(c)
        
        """Write update information to a log file."""
        with open(self.log_file+k, "a") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"{timestamp}: Subject: {subject}, Predicate: {predicate}, Object: {object_}\n")

class TripleStoreClient:
    """Command line interface to interact with TripleStore servers."""
    
    def __init__(self):
        self.server_urls = {
            1: "http://127.0.0.1:5001",
            2: "http://127.0.0.1:5002",
            3: "http://127.0.0.1:5003",
        }
        self.c=0
    def main_loop(self):
        print("Welcome to the Triple Store Client")
        
        while True:
            print("\nAvailable Servers:")
            print("1: Server 1")
            print("2: Server 2")
            print("3: Server 3")
            print("4: Exit")
            server_choice = input("Choose server to connect (1 or 2 or 3), or exit (4): ")
            self.c=server_choice
            if server_choice == "4":
                break

            if int(server_choice) not in self.server_urls:
                print("Invalid server choice. Please choose again.")
                continue

            api = TripleStoreAPI(self.server_urls[int(server_choice)])
            self.interact_with_server(api)

    def interact_with_server(self, api):
        while True:
            print("\nOptions:")
            print("1. Query")
            print("2. Update")
            print("3. Merge")
            print("4. Return to server selection")
            option = input("Select an option: ")

            if option == "4":
                break

            if option == "1":  # Query
                subject = input("Enter the subject to query: ")
                print("Querying...")
                result = api.query_subject(subject)
            elif option == "2":  # Update
                subject = input("Enter the subject: ")
                predicate = input("Enter predicate: ")
                object_ = input("Enter object: ")
                print("Updating...")
                
                result = api.update_triple(subject, predicate, object_)
            elif option == "3":  # Merge
                source_server_id = int(input("Enter source server ID for merge (1 or 2): "))
                if source_server_id not in self.server_urls or source_server_id == self.c:
                    print("Invalid source server ID or same as target")
                    continue
                print("Merging...")
                result = api.merge_data(source_server_id,self.c)
            else:
                print("Invalid option")
                continue

            print("Result:", result)

if __name__ == "__main__":
    client = TripleStoreClient()
    client.main_loop()
