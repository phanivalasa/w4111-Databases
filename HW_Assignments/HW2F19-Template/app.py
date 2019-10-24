
####################################################################################################
#
# DO NOT WORRY ABOUT ANY OF THE STUFF IN THIS SECTION. THIS HELPS YOU IMPLEMENT.
#
#


# Import functions and objects the microservice needs.
# - Flask is the top-level application. You implement the application by adding methods to it.
# - Response enables creating well-formed HTTP/REST responses.
# - requests enables accessing the elements of an incoming HTTP/REST request.
#
from flask import Flask, Response, request
from datetime import datetime
import json
import src.data_service.data_table_adaptor as dta

import logging



logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# The convention is that a compound primary key in a path has the elements sepatayed by "_"
# For example, /batting/willite01_BOS_1960_1 maps to the primary key for batting
_key_delimiter = "_"
_host = "127.0.0.1"
_port = 5002
_api_base = "/api"

application = Flask(__name__)


def handle_args(args):
    """

    :param args: The dictionary form of request.args.
    :return: The values removed from lists if they are in a list. This is flask weirdness.
        Sometimes x=y gets represented as {'x': ['y']} and this converts to {'x': 'y'}
    """

    result = {}

    if args is not None:
        for k,v in args.items():
            if type(v) == list:
                v = v[0]
            result[k] = v

    return result

# 1. Extract the input information from the requests object.
# 2. Log the information
# 3. Return extracted information.
#
def log_and_extract_input(method, path_params=None):

    path = request.path
    args = dict(request.args)
    data = None
    headers = dict(request.headers)
    method = request.method
    url = request.url
    base_url = request.base_url

    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        data = "You sent something but I could not get JSON out of it."

    log_message = str(datetime.now()) + ": Method " + method

    # Get rid of the weird way that Flask sometimes handles query parameters.
    args = handle_args(args)

    inputs =  {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data,
        "url": url,
        "base_url": base_url
        }

    # Pull out the fields list as a separate element.
    if args and args.get('fields', None):
        fields = args.get('fields')
        fields = fields.split(",")
        inputs['fields_param']=args.get('fields')
        del args['fields']
        inputs['fields'] = fields

    if args and args.get('limit', None):
        limit = args.get('limit')
        del args['limit']
        inputs['limit'] = int(limit)

    if args and args.get('offset', None):
        offset = args.get('offset')
        del args['offset']
        inputs['offset'] = int(offset)

    log_message += " received: \n" + json.dumps(inputs, indent=2)
    logger.debug(log_message)

    return inputs


def log_response(path, rsp):
    """

    :param path: The path parameter received.
    :param rsp: Response object
    :return:
    """
    msg = rsp
    logger.debug(str(datetime.now()) + ": \n" + str(rsp))


def get_field_list(inputs):
    return inputs.get('fields', None)

def generate_error(status_code, msg=None):
    """

    This used to be more complicated in previous semesters, but we simplified for fall 2019.
    Does not do much now.
    :param status_code:
    :param ex:
    :param msg:
    :return:
    """

    rsp = Response("Oops", status=500, content_type="text/plain")

    if status_code == 500:
        if msg is None:
            msg = "INTERNAL SERVER ERROR. Please take COMSE6156 -- Cloud Native Applications."

        rsp = Response(msg, status=status_code, content_type="text/plain")

    return rsp


def custom_formatted_output(output, offset=None, limit=None, new_row=None, inputs=None):
    """

    :output - This is the output from the sql execution from RDB tables. it's in the format of a list of dict when the data is selected.
             Or has the number of records updated/deleted/nserted.
    :offset - offset value used generate next a previous links
    :limit - number of records to be limited; again used for generating pagination links
    :new_row - the values in new row when issued by POST
    :inputs - input dicitionary object with all the elements captured from the request
    :return - Returns a formatted output with data and links for pagination
    """

    if request.method == 'GET':

        format = {}
        if output is None:
            output = []
            format['data'] = output
        else:
            format['data'] = output

        links = []
        links.append({"rel": "current", "href": request.url})

        if limit is not None:

            if len(output) >= limit:
                new_url = get_url(inputs)
                if offset is not None:
                    nextoffset = offset + limit
                else:
                    nextoffset = limit
                if new_url.find("?") > 0:
                    new_url += "&offset=" + str(nextoffset) + "&limit=" + str(limit)
                else:
                    new_url += "?offset=" + str(nextoffset) + "&limit=" + str(limit)
                links.append({"rel": "next", "href": new_url})

            # caluclate previous offset value
            prevoffset = -1
            if offset is not None:
                if (offset - limit) >= 0:
                    prevoffset = offset - limit
                else:
                    prevoffset = 0

            if prevoffset > -1 and offset is not 0:
                new_url = get_url(inputs)
                if new_url.find("?") > 0:
                    new_url += "&offset=" + str(prevoffset) + "&limit=" + str(limit)
                else:
                    new_url += "?offset=" + str(prevoffset) + "&limit=" + str(limit)
                links.append({"rel": "previous", "href": new_url})

        format['links'] = links
        return format

    if request.method == 'PUT':

        format = {}
        format['Rows Updated'] = output
        request_url = request.url

        if request_url.find("?") > 0:
            new_url = request_url[:request_url.find("?")]
        else:
            new_url = request_url

        links = []
        links.append({"rel": "current", "href": new_url})

        format['links'] = links
        return format

    if request.method == 'DELETE':

        format = {}
        format['Rows Deleted'] = output
        request_url = request.url

        if request_url.find("?") > 0:
            new_url = request_url[:request_url.find("?")]
        else:
            new_url = request_url

        links = []
        links.append({"rel": "current", "href": new_url})

        format['links'] = links
        return format

    if request.method == 'POST':

        format = {}
        format['Record Inserted'] = 'Successful'
        request_url = request.url

        if request_url.find("?") > 0:
            post_url = request_url[:request_url.find("?")]
        else:
            post_url = request_url
        s = 0
        for key in new_row:
            if s == 0:
                post_url += "?" + key + "=" + str(new_row[key])
                s = 1
            else:
                post_url += "&" + key + "=" + str(new_row[key])

        return format, post_url


def get_url(inputs):
    request_base_url = inputs['base_url']
    if request_base_url.find("?") > 0:
        return_url = request_base_url[:request_base_url.find("?")]
    else:
        return_url = request_base_url

    start = 0
    for key, value in inputs['query_params'].items():
        if not key in ('offset', 'limit'):
            if start == 0:
                return_url += "?" + key + "=" + str(value)
                start = 1
            else:
                return_url += "&" + key + "=" + str(value)

    fields = inputs.get('fields', None)
    if fields is not None:
        if return_url.find("?") > 0 :
            return_url += "&fields=" + str(inputs['fields_param'])
        else:
            return_url += "?&fields=" + str(inputs['fields_param'])

    return return_url


####################################################################################################
#
# THESE ARE JUST SOME EXAMPLES TO HELP YOU UNDERSTAND WHAT IS GOING ON.
#
#

# This function performs a basic health check. We will flesh this out.
@application.route("/health", methods=["GET"])
def health_check():

    rsp_data = { "status": "healthy", "time": str(datetime.now()) }
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp


@application.route("/demo/<parameter>", methods=["GET", "PUT", "DELETE", "POST"])
def demo(parameter):
    """
    This simple echoes the various elements that you get for handling a REST request.
    Look at https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data

    :param parameter: A list of the path parameters.
    :return: None
    """

    inputs = log_and_extract_input(demo, { "parameter": parameter })

    msg = {
        "/demo received the following inputs" : inputs
    }

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp

####################################################################################################
#
# YOU HAVE TO COMPLETE THE IMPLEMENTATION OF THE FUNCTIONS BELOW.
#
#
@application.route("/api/databases", methods=["GET"])
def dbs():
    """

    :return: A JSON object/list containing the databases at this endpoint.
    """
    try:
        inputs = log_and_extract_input(dbs, None)

        # IMPLEMENTATION
        output = dta.get_databases()
        print('rows selected:', len(output))
        output = custom_formatted_output(output)


        if output:
            # If row exists, return JSON data and 200
            output_data = json.dumps(output, default=str)
            resp = Response(output_data, status=200, mimetype='application/json')
            return resp

        else:
            # No exception and no data means 404
            resp = Response("Not found", status=404, mimetype="text/plain")
            return resp

    except Exception as e:
        print(e)
        return generate_error(500)



@application.route("/api/databases/<dbname>", methods=["GET"])
def tbls(dbname):
    """

    :param dbname: The name of a database/sche,a
    :return: List of tables in the database.
    """
    try:
        inputs = log_and_extract_input(tbls, None)

        # IMPLEMENTATION
        output = dta.get_table_names(dbname)
        if output is None:
            return Response("No valid database with tables found", status=404, mimetype="text/plain")
        else:
            print ('rows selected:', len(output))
        output = custom_formatted_output(output)


        if output:
            # If row exists, return JSON data and 200
            output_data = json.dumps(output, default=str)
            resp = Response(output_data, status=200, mimetype='application/json')
            return resp

        else:
            # No exception and no data means 404
            resp = Response("Not found", status=404, mimetype="text/plain")
            return resp

    except Exception as e:
        print(e)
        return generate_error(500)


@application.route('/api/<dbname>/<resource>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def resource_by_id(dbname, resource, primary_key):
    """

    :param dbname: Schema/database name.
    :param resource: Table name.
    :param primary_key: Primary key in the form "col1_col2_..._coln" with the values of key columns.
    :return: Result of operations.
    """

    result = None

    try:
        # Parse the incoming request into an application specific format.
        context = log_and_extract_input(resource_by_id, (dbname, resource, primary_key))

        # IMPLEMENTATION

        # Its' the design consideration that primary key values are separated by "_" and passed.
        pk_values = primary_key.split("_")
        fields = context.get('fields', None)
        new_values = context.get('body', None)

        if request.method == 'GET':

            # IMPLEMENTATION
            output = dta.get_by_primary_key(resource, dbname, pk_values, fields=fields)
            if output is not None:
                log_message = 'Rows selected: ' + str(len(output))
            else:
                log_message = 'Rows selected: 0'
            logger.debug(log_message)
            output = custom_formatted_output(output)

        elif request.method == 'DELETE':

            # IMPLEMENTATTION
            output = dta.delete_by_primary_key(resource, dbname, pk_values)
            log_message = 'Rows deleted: ' + str(output)
            logger.debug(log_message)
            output = custom_formatted_output(output)

        elif request.method == 'PUT':

            # IMPLEMENTATION
            output = dta.update_by_primary_key(resource, dbname, pk_values, new_values)
            log_message = 'Rows updated: ' + str(output)
            logger.debug(log_message)
            output = custom_formatted_output(output)


        if output:
            # If row exists, return JSON data and 200
            output_data = json.dumps(output, default=str)
            resp = Response(output_data, status=200, mimetype='application/json')
            return resp
        else:
            # No exception and no data means 404
            resp = Response("Not found", status=404, mimetype="text/plain")
            return resp

    except Exception as e:
        print(str(e))
        return generate_error(500, 'Internal Error: ' + str(e))


@application.route('/api/<dbname>/<resource_name>', methods=['GET', 'POST'])
def get_resource(dbname, resource_name):

    result = None

    try:
        context = log_and_extract_input(get_resource, (dbname, resource_name))

        # IMPLEMENTATION

        fields = context.get('fields', None)
        new_record = context.get('body', None)
        query_params = context.get('query_params',None)
        limit = context.get('limit',None)
        offset = context.get('offset', None)


        if request.method == 'GET':

            # IMPLEMENTATION

            output = dta.get_by_template(resource_name, dbname, query_params, fields=fields, limit=limit, offset=offset)
            if output is not None:
                log_message = 'Rows selected: ' + str(len(output))
            else:
                log_message = 'Rows selected: 0'
            logger.debug(log_message)
            output = custom_formatted_output(output,limit=limit,offset=offset, inputs=context)

            if output:
                output_data = json.dumps(output, default=str)
                resp = Response(output_data, status=200, mimetype='application/json')
            else:
                resp = Response("Not found", status=404, mimetype="text/plain")

            return resp

        elif request.method == 'POST':
            # IMPLEMENTATION

            output = dta.insert_record(resource_name, dbname, new_record)

            log_message = 'Rows Inserted: ' + str(output)
            logger.debug(log_message)

            output, post_url = custom_formatted_output(output, new_row=new_record)
            output_data = json.dumps(output, default=str)
            resp = Response(output_data, status=200, mimetype='application/json')
            resp.headers['Location'] = post_url
            return resp

        else:
            result = "Invalid request."
            return result, 400, {'Content-Type': 'text/plain; charset=utf-8'}

    except Exception as e:
        print("Exception e = ", e)
        return generate_error(500, 'Internal Error: '+str(e))


@application.route('/api/<dbname>/<parent_name>/<primary_key>/<target_name>', methods=['GET'])
def get_by_path(dbname, parent_name, primary_key, target_name):

    # Do not implement

    result = " -- THANK ALY AND ARA -- "

    return result, 501, {'Content-Type': 'application/json; charset=utf-8'}




@application.route('/api/<dbname>/<parent_name>/<primary_key>/<target_name>/<target_key>',
           methods=['GET'])
def get_by_path_key(dbname, parent_name, primary_key, target_name, target_key):
    # Do not implement

    result = " -- THANK ALY AND ARA -- "

    return result, 501, {'Content-Type': 'application/json; charset=utf-8'}


# You can ignore this method.
def handle_error(e, result):
    return "Internal error.", 504, {'Content-Type': 'text/plain; charset=utf-8'}

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.


    logger.debug("Starting HW2 time: " + str(datetime.now()))
    application.debug = True
    application.run(host=_host, port=_port)