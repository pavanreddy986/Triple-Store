from flask import Flask, render_template, request, redirect, url_for
from client import TripleStoreAPI


app = Flask(__name__)

# Initialize TripleStoreAPI with default server URL
triple_store_api = None
current_server = 0

# @app.route('/', methods=['GET', 'POST'])
# def server_selection():
#     global triple_store_api
#     if request.method == 'POST':
#         server_choice = request.form['server']
#         triple_store_api = TripleStoreAPI(base_url=f"http://127.0.0.1:500{server_choice}")
#         return redirect(url_for('index'))
#     return render_template('server_selection.html')



@app.route('/', methods=['GET', 'POST'])
def server_selection():
    global triple_store_api, current_server
    if request.method == 'POST':
        server_choice = request.form['server']
        current_server = server_choice
        triple_store_api = TripleStoreAPI(base_url=f"http://127.0.0.1:500{server_choice}")
        return redirect(url_for('index'))
    return render_template('server_selection.html')


@app.route('/index')
def index():
    if triple_store_api is None:
        return redirect(url_for('server_selection'))
    return render_template('index.html')

@app.route('/query_form')
def query_form():
    return render_template('query_form.html')

@app.route('/query/<subject>')
def query_subject(subject):
    if triple_store_api is None:
        return "TripleStoreAPI object not initialized"
    result = triple_store_api.query_subject(subject)
    return result

@app.route('/update_form')
def update_form():
    return render_template('update_form.html')

@app.route('/update', methods=['POST'])
def handle_update():
    subject = request.form['subject']
    predicate = request.form['predicate']
    object_ = request.form['object']
    
    # Perform update operation using triple_store_api
    if triple_store_api is None:
        return "TripleStoreAPI object not initialized"
    
    # Call update_triple method and capture the result
    result = triple_store_api.update_triple(subject, predicate, object_)
    
    # Check if the result contains an 'error' key
    if 'error' in result:
        return f"Update failed: {result['error']}"
    # elif 'file limit' in result:
    #     return "Log file limit reached, please do some updates manually"
    else:
        return "Update successful"
    
@app.route('/merge_form')
def merge_form():
    global current_server
    available_servers = ['1', '2','3']  # Update with your available server IDs
    # current_server = int(current_server)
    # print(current_server)
    available_servers.remove(current_server)  # Remove the current server from the list
    return render_template('merge_form.html', current_server=current_server, available_servers=available_servers)

@app.route('/merge', methods=['POST'])
def handle_merge():
    global current_server
    server_id = request.form['source_id']  # Assuming your form has an input field with name 'server_id'
    server_id = int(server_id)
    
    # Perform merge operation using triple_store_api
    if triple_store_api is None:
        return "TripleStoreAPI object not initialized"
    
    # Call merge_data method and capture the result
    result = triple_store_api.merge_data(server_id, current_server)  # Use current server ID as source_id
    
    # Check if the result contains an 'error' key
    if 'error' in result:
        return f"Merge failed: {result['error']}"
    else:
        return "Merge successful"


if __name__ == '__main__':
    app.run(debug=True, port=5005)
    # app.secret_key = secrets.token_hex(16)

