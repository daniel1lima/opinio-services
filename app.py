from flask import Flask, jsonify, request
# import subprocess
# import csv
# import pandas as pd

# import Modules.category_splitting NOT WORKING BECAUSE OF line 16

# These imports need to be treated to aswell
# import Modules.create_embeddings
# import Modules.generate_insight
# from Modules.scrape_reviews import scrape_reviews_function
# from mongofeeder import push_document


app = Flask(__name__)



@app.route('/status', methods=['GET'])
def status():
    return jsonify({'status': 'Opinio is working!'})



# @app.route('/scrape_reviews', methods=['POST'])
# #need to match above with axios call to /run-script
# def api_scrape_reviews():
#     request_data = request.get_json()
#     # Process the parameters from the request here, for us this would look like
#     query = request_data.get("business_name", None)
#     # idk what we want to do exactly but im guessing since we have to run this every now and then some useful ones will be 
#     pages = request_data.get("pages", None)
#     stop_at = request_data.get("date_to_stop_at", None)
#     # etc..

#     #All the params come here in the format that you want them
#     result = scrape_reviews_function(query, pages, stop_at)

#     # Check scrape reviews for how to setup the other functions, the endpoints are all ready here

#     return result


# # Not sure if we need this endpoint

# @app.route('/category_splitting', methods=['POST'])
# #need to match above with axios call to /run-script
# def api_category_splitting():
#     request_data = request.get_json()

#     return result

# @app.route('/generate_insight', methods=['GET'])
# #need to match above with axios call to /run-script
# def api_generate_insight():
#     request_data = request.get_json()

#     return result

    


# @app.route('/create_embeddings_pipeline', methods=['POST'])
# def api_create_embeddings_pipeline():
#     request_data = request.get_json()

#     query = request_data.get("query", None)

#     pages = request_data.get("pages", None)
#     stop_at = request_data.get("date_to_stop_at", None)
#     # etc..

#     result = create_embeddings(query, pages, stop_at)



#     return result

    

# flask --app flask_script run



# # Template for a GET endpoint
# @app.route('/example_get', methods=['GET'])
# def example_get():
#     # Implement your GET request handling logic here
#     # Example: Fetch data from a database or perform some computation
#     data = {"message": "This is a GET response"}
#     return jsonify(data), 200

# # Template for a POST endpoint
# @app.route('/example_post', methods=['POST'])
# def example_post():
#     # Get JSON data from the request
#     request_data = request.get_json()
#     # Implement your POST request handling logic here
#     # Example: Process the received data and return a response
#     received_value = request_data.get("some_key", "default_value")
#     response = {"received_data": received_value}
#     return jsonify(response), 201

# # Template for an endpoint using a custom function
# @app.route('/custom_endpoint', methods=['POST'])
# def custom_endpoint():
#     request_data = request.get_json()
#     # Example: Use a custom function to process data
#     # result = custom_function(request_data)
#     result = {"message": "This would call a custom function"}
#     return jsonify(result), 200

# # Add more endpoints as needed
# # @app.route('/another_endpoint', methods=['GET', 'POST'])
# # def another_endpoint():
# #     # Your logic here
# #     return jsonify({"message": "Another endpoint response"}), 200
