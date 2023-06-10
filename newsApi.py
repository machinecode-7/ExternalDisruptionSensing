class QueryAPI:
    """
        - This is an abstract class to define the overall structure of API query module. 
        - The routing of queries is implemented as generic function which can send various components of HTTP requests.
    """
    def __init__(self, exceptions_mapping):
        """
            - This is constructor function which defines the essential and common parameters used in routing queries to external APIs.
            - Below are input/output map of the function:
                Input : 
                    exceptions_mapping : [DICTIONARY] -> This dictionary holds the display texts indexed with exception identifiers.
                Output :
                    None
        """
        self.endpoint = ""
        self.API_key = ""
        self.query_string = ""
        self.exceptions_mapping = exceptions_mapping
    def send_request(self, queryString, mode = "GET", headers = None, data = None, params = None, json = None):
        """
            - This function is responsible for sending out HTTP requests using provided HTTP method like GET, POST, HEAD, etc.,
            - This implementation is inherited by all the API classes and called using below paradigm.
                Syntax : super(<CLASS_NAME>, self).send_request(
                    queryString,
                    mode <OPTIONAL>,
                    headers <OPTIONAL>,
                    data <OPTIONAL>,
                    params <OPTIONAL>
                )
            - Below are input/output map of the function:
                Input : 
                    queryString : [STRING] -> The endpoint url with/without parameters.
                    mode : [STRING] -> Identifier for the HTTP method like GET, POST, HEAD, etc.,
                    headers : [DICTIONARY] -> The mapping of HTTP headers with the header values.
                    data : [DICTIONARY] -> The mapping HTTP request body parameters and values.
                    params : [DICTIONARY] -> The mapping of HTTP request parameters which optionally can be supplied in the queryString in case of GET request.   
                Output :
                    response : [HTTPResponseObject] -> The response received from the API endpoint along with metadata.
        """
        if mode not in ["GET", "POST"]:
            raise Exception(f"[Exception-API_QUERY003] {self.exceptions_mapping['Exception-API_QUERY003']['display_text'].replace('$$mode$$',mode).replace('$$API_NAME$$', self.API_NAME)}")
        response = requests.request(mode, queryString, headers=headers, data = data, verify = False, params = params, json = json)
        return response
    

class NewsAPI(QueryAPI):
    """
        - The module is responsible for routing the query to the NewsAPI and get the JSON response.
        - The functionality of cadence is incorporated into the module. 
            ~ The cadence sets the limit of time window span which can be queried in a single HTTP request.
            ~ If the difference between start and end date is more than the cadence then iteratively request is routed with with time window same as specified as cadence of the source.
    """
    def __init__(self, API_config, NewsAPI_key, exceptions_mapping, logger, environment, timestamp):
        """
            - This is constructor method which coallates all the parameters required to route the request to NewsAPI from the configuration.
            - Exception handling is performed for missing necessary/mandatory parameters.
            - Below are input/output map of the function:
                Input : 
                    API_config : [DICTIONARY] -> The consolidated (external client + pipeline) configuration for API invocation.
                    NewsAPI_key : [STRING] -> The API key for authentication.
                    exceptions_mapping : [DICTIONARY] -> This dictionary holds the display texts indexed with exception identifiers.
                    logger : [<LOG>] -> The instance of logging module which will track the status of API invocation.
                Output :
                    None

        """
        super(NewsAPI, self).__init__(exceptions_mapping)
        start = time()
        exceptions_stack = []
        self.logger = logger
        self.API_NAME = "NewsAPI"
        self.endpoint = API_config["NewsAPI"]["endpoint"]
        self.method = API_config["NewsAPI"]["method"]
        self.API_key = NewsAPI_key
        self.query_string = API_config["NewsAPI"]["query_string"]
        self.search_in = API_config["NewsAPI"]["search_field"]
        self.sources = API_config["NewsAPI"]["sources"]
        self.domains = API_config["NewsAPI"]["domains"]
        self.exclude_domains = API_config["NewsAPI"]["exclude_domains"]
        self.from_date_time = API_config["NewsAPI"]["from_date_time"]
        self.to_date_time = API_config["NewsAPI"]["to_date_time"]
        self.language = API_config["NewsAPI"]["language"]
        self.sort_by = API_config["NewsAPI"]["sort_by"]
        mandatory_fields = API_config["NewsAPI"]["mandatory_fields"]
        self.cadence = API_config["NewsAPI"]["cadence"]
        self.date_format = API_config["NewsAPI"]["date_format"]
        self.status_log_table_name = API_config["run_status_table_name"]
        self.process_log_table_name = API_config["process_log_table_name"]
        self.path_to_client_data= API_config["NewsAPI"]["path_to_client_data"].replace("$$environment$$", environment)
        self.bucket_name = API_config["bucket_name"]
        self.bucket_path = API_config["save_directory"].replace("$$environment$$", environment).replace("$$timestamp$$", timestamp).replace("$$source$$", API_config["NewsAPI"]["NAME"]).replace("$$type$$", API_config["NewsAPI"]["TYPE"])
        self.raw_response_file_name = API_config["NewsAPI"]["raw_response_file_name"]
        self.timestamp = timestamp
        for field in mandatory_fields:
            if field not in API_config["NewsAPI"].keys():
                exceptions_stack.append(Exception(f"[Exception-API_QUERY001] {self.exceptions_mapping['Exception-API_QUERY001']['display_text'].replace('$$parameter$$',field).replace('$$API_NAME$$', self.API_NAME)}"))
            elif len(API_config["NewsAPI"][field]) == 0:
                exceptions_stack.append(Exception(f"[Exception-API_QUERY002] {self.exceptions_mapping['Exception-API_QUERY002']['display_text'].replace('$$parameter$$',field).replace('$$API_NAME$$', self.API_NAME)}"))
        if (self.API_key == None):
            exceptions_stack.append(Exception(f"[Exception-API_QUERY001] {self.exceptions_mapping['Exception-API_QUERY001']['display_text'].replace('$$parameter$$','API_key').replace('$$API_NAME$$', self.API_NAME)}"))
        elif len(self.API_key) == 0: 
            exceptions_stack.append(Exception(f"[Exception-API_QUERY002] {self.exceptions_mapping['Exception-API_QUERY002']['display_text'].replace('$$parameter$$','API_key').replace('$$API_NAME$$', self.API_NAME)}"))
        if len(exceptions_stack) != 0:
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] API INITIALIZATION","Initialization of parameters for NewsAPI failed.","FAILURE",f"{time() - start}",datetime.now())])
            raise Exception(exceptions_stack)
        else:
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] API INITIALIZATION","Initialized parameters for NewsAPI.","SUCCESS",f"{time() - start}",datetime.now())])
   
    def generate_query_list(self,client_data):
        """
        This method returns a set of customised queries that needs to be parsed via NewsAPI
        Input:
            Client_data : A dataframe with all the required data from client such as supplier,supplier location, raw material, etc
        Output:
            query_list: A list of templated queries using the input client data and some external sub terms
        """
        supplier_query_list=[]
        port_query_list=[]
        plant_location_query_list=[]
        product_query_list=[]
        for index,row in client_data.iterrows():
            supplier_query = "("+row['Supplier Name']+") OR (("+row["Raw material/Commodity Provided"]+' OR '+row['commodity']+") AND "+row["Supplier Country"]+") OR (("+row["Raw material/Commodity Provided"]+' OR '+row['commodity']+") AND "+row["Supplier City/State"]+")"
            supplier_query_list.append(supplier_query)
            product_query = "("+str(row['Products manufactured that utilize the given raw material'])+") AND (Effects OR good OR bad)"
            product_query_list.append(product_query)
            if str(row['Source Port, in case of import'])!='nan':
                port_query = "("+str(row['Source Port, in case of import'])+") AND (Natural Calamities OR Disasters)"
                port_query_list.append(port_query)
            if str(row['Plant Location'])!='nan':
                plant_location_query = "("+str(row['Plant Location'])+") AND (Natural Calamities OR Disasters)"
                plant_location_query_list.append(plant_location_query)
        query_list=supplier_query_list+product_query_list+port_query_list+plant_location_query_list
        return query_list
    
    def generate_QueryString(self, from_date_time, to_date_time, query_string):
        """
            - This method is responsible for generating the "GET" request parameter.
            - For every passed query string a get request url gets generated.
            - Below are input/output map of the function:
                Input : 
                    from_date_time : [STRING] -> A string representing the lower limit of the time window from which the news needs to be fetched.
                    to_date_time : [STRING] -> A string representing the upper limit of the time window uptill which the news needs to be fetched.
                    query_string : [STRING] -> The string consisting of appropriate filters that will be applied to return news articles. 
                Output :
                    queryString : [STRING] -> Fully qualified url along with HTTP GET request parameters.
        """
        queryString1 = ""
        queryString2 = ""
        queryString1 += self.endpoint
        queryString1 += "?"
        queryString2 += f"q={query_string}"
        queryString2 += f"&apiKey={self.API_key}"
        if len(self.search_in) > 0:
            queryString2 += f"&searchIn={self.search_in}"
        if len(self.sources) > 0:
            queryString2 += f"&sources={self.sources}"
        if len(self.domains) > 0:
            queryString2 += f"&domains={self.domains}"
        if len(self.exclude_domains) > 0:
            queryString2 += f"&excludeDomains={self.exclude_domains}"
        if len(self.from_date_time) > 0:
            queryString2 += f"&from={from_date_time}"
        if len(self.to_date_time) > 0:
            queryString2 += f"&to={to_date_time}"
        if len(self.language) > 0:
            queryString2 += f"&language={self.language}"
        if len(self.sort_by) > 0:
            queryString2 += f"&sortBy={self.sort_by}"
        queryString = queryString1 + queryString2
        # print(f"Query String for News API between ({from_date_time} , {to_date_time}) : {queryString}")
        return queryString
    
    def routeQuery(self):
        """
            - The method is responsible for fetching the appropriate start and end time for the API.
            - With the time window, the url generated with approproiate filters and parameters is routed to News API.
            - The API response is coalated into either a list of disctionary or in a single dictionary based on lookback constraint of the API.
            - Below are input/output map of the function:
                Input :
                    None
                Output :
                    response : [DICTIONARY/LIST(DICTIONARY)] -> The response received for a single window is a dictionary and the same received for 
                        a list of time windows is a list of dictionary which is returned.
        """
        start_ = time()
        try:
            if len(self.from_date_time) > 0 and len(self.to_date_time) > 0:
                range = (datetime.strptime(self.from_date_time, self.date_format), datetime.strptime(self.to_date_time, self.date_format))
            elif len(self.from_date_time) > 0:
                range = datetime.strptime(self.from_date_time, self.date_format)
            else:
                range = None
            latest_date = self.logger.get_latest_run_date(self.status_log_table_name, self.API_NAME, self.date_format, self.timestamp, range)
            # run_id = self.logger.get_latest_run_id(self.status_log_table_name, self.API_NAME, range)
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Latest Date and Run ID Fetch",f"Fetched last run date ({latest_date}) and run identifier ({self.timestamp}).","SUCCESS",f"{time() - start_}",datetime.now())])
        except Exception as e:
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Latest Date and Run ID Fetch",f"Failed to Fetch last run date and run identifier due to {e}.","FAILED",f"{time() - start_}",datetime.now())])            
        start_ = time()
        try:
            client_data=read_excel(self.bucket_name, self.path_to_client_data)
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Client Data Read",f"Client data read completed.","SUCCESS",f"{time() - start_}",datetime.now())])
        except Exception as e:
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Client Data Read",f"Failed to read client data due to {e}.","FAILED",f"{time() - start_}",datetime.now())])
        # if run_id == None:
        #     run_id = 1
        # else:
        #     run_id = int(run_id) + 1
        run_id = self.timestamp
        start_ = time()
        try:
            if latest_date != None:
                if not isinstance(latest_date, str):
                    self.from_date_time = latest_date.strftime(self.date_format)
                else:
                    self.from_date_time = latest_date
                self.to_date_time = datetime.now().strftime(self.date_format)
            elif (latest_date == None) and (len(self.from_date_time) == 0):
                self.to_date_time = datetime.now().strftime(self.date_format)
                if self.cadence[-1] == 'M':
                    self.from_date_time = (datetime.now() - timedelta(days = int(self.cadence[ : len(self.cadence) - 1]) * 30)).strftime(self.date_format)
                elif self.cadence[-1] == 'D':
                    self.from_date_time = (datetime.now() - timedelta(days = int(self.cadence[ : len(self.cadence) - 1]))).strftime(self.date_format) 
                elif self.cadence[-1] == 'Y':
                    self.from_date_time = (datetime.now() - timedelta(days = int(self.cadence[ : len(self.cadence) - 1]) * 365)).strftime(self.date_format)
            start = datetime.strptime(self.from_date_time, self.date_format)
            end = datetime.strptime(self.to_date_time, self.date_format)
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Date range computation",f"Successfully computed start ({start.strftime(self.date_format)}) and end date ({end.strftime(self.date_format)}) for the API query.","SUCCESS",f"{time() - start_}",datetime.now())])
        except Exception as e:
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Date range computation",f"Failed to compute start and end date for the API query.","FAILED",f"{time() - start_}",datetime.now())])
        if self.cadence[-1] == 'M':
            date_delta = (end - start).days // 30
        elif self.cadence[-1] == 'D':
            date_delta = (end - start).days 
        elif self.cadence[-1] == 'Y':
            date_delta = (end - start).days // 365
        if date_delta > int(self.cadence[ : len(self.cadence) - 1]):
            response = []
            rows = []
            exceptions_stack = []
            start_ = time()
            try: 
                while(start <= end):
                    from_ = start.strftime(self.date_format)
                    if self.cadence[-1] == 'M':
                        start += timedelta(days = int(self.cadence[ : len(self.cadence) - 1]) * 30)
                    elif self.cadence[-1] == 'D':
                        start += timedelta(days = int(self.cadence[ : len(self.cadence) - 1]))
                    elif self.cadence[-1] == 'Y':
                        start += timedelta(days = int(self.cadence[ : len(self.cadence) - 1]) * 365)
                    to = start.strftime(self.date_format)
                    start__ = time()
                    try:
                        query_string=self.generate_query_list(client_data)
                        self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Query template creation",f"Successfully created the query template.","SUCCESS",f"{time() - start__}",datetime.now())])
                    except Exception as e:
                        self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Query template creation",f"Failed to create query template due to {e}.","FAILED",f"{time() - start__}",datetime.now())])
                    counter = 1
                    for Qs in query_string:
                        queryString = self.generate_QueryString(from_, to, Qs)
                        response_ = super(NewsAPI, self).send_request(queryString, mode = self.method)
                        if counter % 3 == 0:
                            sleep(60)
                        if response_.status_code != 200:
                            row = (self.API_NAME, from_, to, run_id, 'FAILURE', "NULL", datetime.now())
                            exceptions_stack.append(Exception(f"[Exception-API_QUERY004] {self.exceptions_mapping['Exception-API_QUERY004']['display_text'].replace('$$status_code$$',str(response_.status_code)).replace('$$API_NAME$$', self.API_NAME)}"))
                        response.append(response_.json())
                        counter += 1
                    row = (self.API_NAME, from_, to, run_id, 'SUCCESS', "NULL",datetime.now())
                    rows.append(row)
                self.logger.log_run_status(self.status_log_table_name, rows)
                self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Request Loop",f"Executed request loop due to start and end date delta greater than the cadence.","SUCCESS",f"{time() - start_}",datetime.now())])
            except Exception as e:
                self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Request Loop",f"Failed execution of request loop.","FAILED",f"{time() - start_}",datetime.now())])
            if len(exceptions_stack) > 0:
                raise Exception(exceptions_stack)
        else:
            response = []
            start_ = time()
            try:
                query_string=self.generate_query_list(client_data)
                self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Query template creation",f"Successfully created the query template.","SUCCESS",f"{time() - start_}",datetime.now())])
            except Exception as e:
                self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Query template creation",f"Failed to create query template due to {e}.","FAILED",f"{time() - start_}",datetime.now())])
            start_ = time()
            try:
                counter = 1
                for Qs in query_string:
                    queryString = self.generate_QueryString(self.from_date_time, self.to_date_time, Qs)
                    response_ = super(NewsAPI, self).send_request(queryString, mode = self.method)
                    if counter % 3 == 0:
                        sleep(60)
                    if response_.status_code != 200:
                        row = [(self.API_NAME, self.from_date_time, self.to_date_time, run_id, 'FAILURE', "NULL", datetime.now())]
                        self.logger.log_run_status(self.status_log_table_name, row)
                        raise Exception(f"[Exception-API_QUERY004] {self.exceptions_mapping['Exception-API_QUERY004']['display_text'].replace('$$status_code$$',str(response_.status_code)).replace('$$API_NAME$$', self.API_NAME)}")
                    response.append(response_.json())
                    counter += 1
                row = [(self.API_NAME, self.from_date_time, self.to_date_time, run_id, 'SUCCESS', "NULL", datetime.now())]
                self.logger.log_run_status(self.status_log_table_name, row)
                self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Single Request",f"Request to endpoint completed.","SUCCESS",f"{time() - start_}",datetime.now())])
            except Exception as e:
                self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Single Request",f"Failed to route request due to {e}.","FAILED",f"{time() - start_}",datetime.now())])
        start_ = time()
        try:
            json_to_bucket(response, self.bucket_name, self.bucket_path + '/' +self.raw_response_file_name)
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Save raw resonse",f"Raw response saved at {self.bucket_path + '/' +self.raw_response_file_name}.","SUCCESS",f"{time() - start_}",datetime.now())])
        except Exception as e:
            self.logger.log_process(self.process_log_table_name,[(self.timestamp,"API_QUERY","[NewsAPI] Save raw resonse",f"Failed to save raw response.","FAILED",f"{time() - start_}",datetime.now())])
        return response 