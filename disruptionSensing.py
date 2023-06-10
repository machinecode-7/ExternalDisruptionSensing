from datetime import datetime
import copy
import json
import psycopg2
import newsApi as API

import ddl

environment = "development"


def read_configuration(require_API_config = False, require_environment = False, require_timestamp = False, require_NewsAPI = False , require_KavidaAPI = False, requireETL = False, require_Alerts = False, require_ETL_phase_2 = False, require_LLM_summarization_layer = False, require_con = False, require_logger = False, require_process_table_name = False, require_date_time_format = False):
    global environment
    # global trigger_timestamp
    import logger

    return_objects = []
    with open("client_configuration_external.json") as client_external_config_fd:
        client_external_config = json.load(client_external_config_fd)
    with open("pipeline_configuration_external.json") as pipeline_external_config_fd:
        pipeline_external_config = json.load(pipeline_external_config_fd)
    with open("exceptions.json") as exceptions_config_fd:
        exceptions_mapping = json.load(exceptions_config_fd)
    with open("key_vault.json", 'r') as key_vault_fd:
        key_vault = json.load(key_vault_fd)
    API_config = copy.deepcopy(client_external_config)
    for pipeline_params in pipeline_external_config.keys():
        if pipeline_params not in API_config.keys():
            API_config[pipeline_params] = pipeline_external_config[pipeline_params]
        else:
            for pipeline_params_l2 in pipeline_external_config[pipeline_params].keys():
                if pipeline_params_l2 not in API_config[pipeline_params].keys():
                    API_config[pipeline_params][pipeline_params_l2] = pipeline_external_config[pipeline_params][pipeline_params_l2]
                else:
                    API_config[pipeline_params][pipeline_params_l2].update(pipeline_external_config[pipeline_params][pipeline_params_l2])
    # timestamp = trigger_timestamp.strftime(API_config["directory_timestamp_format"])
    ENDPOINT = key_vault["db_server_credentials"]["ENDPOINT"]
    PORT = key_vault["db_server_credentials"]["PORT"]
    USER = key_vault["db_server_credentials"]["USER"]
    REGION = key_vault["db_server_credentials"]["REGION"]
    DBNAME = key_vault["db_server_credentials"]["DBNAME"]
    token = key_vault["db_server_credentials"]["PASSWORD"]

    con = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=token)

    logger = logger.Log(key_vault["db_server_credentials"], exceptions_mapping, con)
    logger.create_run_status_log_table(API_config["run_status_table_name"])
    logger.create_process_log_table(API_config["process_log_table_name"])
    timestamp = logger.get_latest_process_run_id(API_config["process_log_table_name"])
    if timestamp == None:
        timestamp = datetime.now().strftime(API_config["directory_timestamp_format"])
    NewsAPI_object = API.NewsAPI(API_config, key_vault["NewsAPI"]["api_key"], exceptions_mapping, logger, environment, timestamp)
    ETL_object = ETL_API_data.ETL(exceptions_mapping, API_config, key_vault["openai"]["api_key"],environment, timestamp, logger)
    LLM_summarization_layer = LLM_summarization.LLM(API_config, key_vault, logger, con, timestamp)
    if require_API_config == True:
        return_objects.append(API_config)
    if require_environment == True:
        return_objects.append(environment)
    if require_timestamp == True:
        return_objects.append(timestamp)
    if require_NewsAPI == True:
        return_objects.append(NewsAPI_object)
    if require_KavidaAPI == True:
        return_objects.append(KavidaAPI_object)
    if requireETL == True:
        return_objects.append(ETL_object)
    if require_Alerts == True:
        return_objects.append(alerts_obj)
    if require_ETL_phase_2 == True:
        return_objects.append(ETL_phase_2_object)
    if require_LLM_summarization_layer == True:
        return_objects.append(LLM_summarization_layer)
    if require_con == True:
        return_objects.append(con)
    if require_logger == True:
        return_objects.append(logger)
    if require_process_table_name == True:
        return_objects.append(API_config["process_log_table_name"])
    if require_date_time_format == True:
        return_objects.append(API_config["directory_timestamp_format"])
    return return_objects

def ddl_main():
    con, logger, table_name, datetime_format = read_configuration(require_API_config = False, require_environment = False, require_timestamp = False, require_NewsAPI = False , require_KavidaAPI = False, requireETL = False, require_Alerts = False, require_ETL_phase_2 = False, require_LLM_summarization_layer = False, require_con = True, require_logger = True, require_process_table_name = True, require_date_time_format = True)
    ddl.main(con, logger, table_name, datetime_format)

def ETL_NewsAPI():
    NewsAPI_object , ETL_object = read_configuration(require_API_config = False, require_environment = False, require_timestamp = False, require_NewsAPI = True , require_KavidaAPI = False, requireETL = True, require_Alerts = False)
    try:
        response_NewsAPI = NewsAPI_object.routeQuery()
        ETL_object.parseNewsAPIResponse(response_NewsAPI)
    except Exception as e:
        print("Execption - [NewsAPI]", e)

def Secondry_ETL():
    API_config, environment, timestamp, ETL_phase_2_obj = read_configuration(require_API_config = True, require_environment = True, require_timestamp = True, require_NewsAPI = False , require_KavidaAPI = False, requireETL = False, require_Alerts = False, require_ETL_phase_2 = True)
    ETL_phase_2_obj.GenerateEntityAlertData(
            API_config["save_directory"].replace("$$environment$$", environment).replace("$$timestamp$$", timestamp).replace("$$source$$", API_config["KavidaAPI"]["Entity_Alerts"]["NAME"]).replace("$$type$$", API_config["KavidaAPI"]["Entity_Alerts"]["TYPE"]) + "/" +
            API_config["KavidaAPI"]["Entity_Alerts"]["response_file_name"]
            , API_config["filters"]["EntityAlert"])
    ETL_phase_2_obj.GenerateCommodityPriceAlertData(API_config["save_directory"].replace("$$environment$$", environment).replace("$$timestamp$$", timestamp).replace("$$source$$", API_config["KavidaAPI"]["Commodity_Prices"]["NAME"]).replace("$$type$$", API_config["KavidaAPI"]["Commodity_Prices"]["TYPE"])  + "/" +
            API_config["KavidaAPI"]["Commodity_Prices"]["response_file_name"], API_config["filters"]["CommodityPriceAlert"])  
    ETL_phase_2_obj.GenerateLocationAlertData(API_config["save_directory"].replace("$$environment$$", environment).replace("$$timestamp$$", timestamp).replace("$$source$$", API_config["KavidaAPI"]["Location_Alerts"]["NAME"]).replace("$$type$$", API_config["KavidaAPI"]["Location_Alerts"]["TYPE"])  + "/" +
            API_config["KavidaAPI"]["Location_Alerts"]["response_file_name"]
            , API_config["filters"]["LocationAlert"])

    ETL_phase_2_obj.compute_metrics()
    
def LLM_Summarization():
    LLM_Summarization_layer = read_configuration(require_API_config = False, require_environment = False, require_timestamp = False, require_NewsAPI = False , require_KavidaAPI = False, requireETL = False, require_Alerts = False, require_ETL_phase_2 = False, require_LLM_summarization_layer = True)[0]
    LLM_Summarization_layer.summarize()

def close_db_connection():
    con = read_configuration(require_API_config = False, require_environment = False, require_timestamp = False, require_NewsAPI = False , require_KavidaAPI = False, requireETL = False, require_Alerts = False, require_ETL_phase_2 = False, require_LLM_summarization_layer = False, require_con = True)[0]
    con.close()

dag = DAG(
    'Data_Ingestion', 
    description='DAG for ingesting the data from various API endpoints and transform it into form suitable to be fed into LLM model.',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2023, 5, 4), catchup=False
)

ddl_dag = PythonOperator(task_id='DDL', python_callable=ddl_main, dag=dag)
ETL_NewsAPI_ = PythonOperator(task_id='ETL_NewsAPI', python_callable=ETL_NewsAPI, dag=dag)
ETL_KavidaAPI_Entity_Alerts_ = PythonOperator(task_id='ETL_KavidaAPI_Entity_Alerts', python_callable=ETL_KavidaAPI_Entity_Alerts, dag=dag)
ETL_KavidaAPI_Location_Alerts_ = PythonOperator(task_id='ETL_KavidaAPI_Location_Alerts', python_callable=ETL_KavidaAPI_Location_Alerts, dag=dag)
ETL_KavidaAPI_Commodity_List_Alerts_ = PythonOperator(task_id='ETL_KavidaAPI_Commodity_List_Alerts', python_callable=ETL_KavidaAPI_Commodity_List_Alerts, dag=dag)
ETL_KavidaAPI_Commodity_Prices_Alerts_ = PythonOperator(task_id='ETL_KavidaAPI_Commodity_Prices_Alerts', python_callable=ETL_KavidaAPI_Commodity_Prices_Alerts, dag=dag)
ETL_KavidaAPI_Ports_Alerts_ = PythonOperator(task_id='ETL_KavidaAPI_Ports_Alerts', python_callable=ETL_KavidaAPI_Ports_Alerts, dag=dag)
Secondry_ETL_ = PythonOperator(task_id='Secondry_ETL_Layer', python_callable=Secondry_ETL, dag=dag)
LLM_Summarization_ = PythonOperator(task_id='LLM_Summarization_Layer', python_callable=LLM_Summarization, dag=dag)
close_db_connection_ = PythonOperator(task_id='Close_DB_Connection', python_callable=close_db_connection, dag=dag)

ddl_dag >> [ETL_NewsAPI_, ETL_KavidaAPI_Entity_Alerts_, ETL_KavidaAPI_Location_Alerts_, ETL_KavidaAPI_Commodity_List_Alerts_, ETL_KavidaAPI_Commodity_Prices_Alerts_, ETL_KavidaAPI_Ports_Alerts_]
ETL_NewsAPI_ >> Secondry_ETL_
ETL_KavidaAPI_Entity_Alerts_ >> Secondry_ETL_
ETL_KavidaAPI_Location_Alerts_ >> Secondry_ETL_
ETL_KavidaAPI_Commodity_List_Alerts_ >> Secondry_ETL_
ETL_KavidaAPI_Commodity_Prices_Alerts_ >> Secondry_ETL_
ETL_KavidaAPI_Ports_Alerts_ >> Secondry_ETL_
Secondry_ETL_ >> LLM_Summarization_
LLM_Summarization_ >> close_db_connection_
