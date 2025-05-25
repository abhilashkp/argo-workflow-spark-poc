import os
import json
import logging
import traceback
from typing import List, Dict, Tuple,Optional
from pyspark.sql import SparkSession
from pyspark import SparkConf
from ifsutils.cipher import CipherManager

# Define Log4jHandler to forward Python logs to Log4j via Py4J
class Log4jHandler(logging.Handler):
    def __init__(self, spark):
        super().__init__()
        # Access the JVM gateway that Spark has already initialized
        self.log4j_logger = spark._jvm.org.apache.log4j.LogManager.getLogger("PythonSparkScript")

    def emit(self, record):
        log_entry = self.format(record)
        if record.levelname == "INFO":
            self.log4j_logger.info(log_entry)
        elif record.levelname == "ERROR":
            self.log4j_logger.error(log_entry)
        elif record.levelname == "WARNING":
            self.log4j_logger.warn(log_entry)

# Function to format exception into a single line
def format_exception(e):
    formatted_exception = ' | '.join(traceback.format_exception(type(e), e, e.__traceback__))
    escaped_exception = json.dumps(formatted_exception)  # Escapes special characters
    return escaped_exception[1:-1]  # Strip the extra quotes added by json.dumps()

def configure_log4j(spark):    
    # Set up the Python logging system after Spark session is created
    handler = Log4jHandler(spark)
    formatter = logging.Formatter('%(message)s')  # You can format this if needed
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(handler)
    
    logLevel = os.environ.get('LOG_LEVEL', 'INFO').lower()
    if logLevel == 'debug':
        logger.setLevel(logging.DEBUG)
    elif logLevel == 'trace':
        logger.setLevel(logging.DEBUG)
    elif logLevel == 'info':
        logger.setLevel(logging.INFO)
    elif logLevel == 'notice':
        logger.setLevel(logging.INFO)  
    elif logLevel == 'warn':
        logger.setLevel(logging.WARNING)
    elif logLevel == 'error':
        logger.setLevel(logging.ERROR)
    elif logLevel == 'crit':
        logger.setLevel(logging.CRITICAL)
    elif logLevel == 'alert':
        logger.setLevel(logging.CRITICAL)  
    elif logLevel == 'emerg':
        logger.setLevel(logging.CRITICAL)  
    else:
        logger.setLevel(logging.INFO)  # Default to INFO if logLevel is not recognized 
    
    # Retrieve the Spark application ID
    spark_app_id = spark.sparkContext.applicationId
    os.environ['SPARK_APPLICATION_ID'] = spark_app_id
    logger.info(f"Spark Application ID: {spark_app_id}")

    spark_app_name = os.environ.get('WORKFLOW_NAME')
    spark_app_mount_path = os.environ.get('MOUNT_PATH')

    if spark_app_mount_path:
        logger.info(f"MOUNT_PATH: {spark_app_mount_path}")

        spark_local_dir = f'{spark_app_mount_path}/log-index'
        logger.info(f"spark_local_dir: {spark_local_dir}")

        if not os.path.exists(spark_local_dir):
            os.makedirs(spark_local_dir)
            logger.info(f"Directory {spark_local_dir} created.")

        if spark_app_name:
            logger.info(f"WORKFLOW_NAME: {spark_app_name}")
            index_log_file_name = f'{spark_local_dir}/{spark_app_name}-{spark_app_id}'

            if not os.path.exists(index_log_file_name):
                with open(index_log_file_name, 'w') as file:
                    file.write('')  # Optionally, write something to the file
                logger.info(f'Index log file "{index_log_file_name}" has been created.')
            else:
                logger.info(f'Index log file "{index_log_file_name}" already exists.')
        else:
            logger.info('Environment variable WORKFLOW_NAME is missing. Skipping log index file creation.')
    else:
        logger.info("Environment variable MOUNT_PATH is missing. Skipping directory creation.")

class DataLakeConnectionInfo:    
    storageAccount: str
    containerName: str
    sasToken: str
    preSignedUrl: str
    clientId: str
    clientSecret: str
    tenantId: str
    def __init__(self,
                 storageAccount: str,
                 containerName: str,
                 sasToken: str,
                 preSignedUrl: str,
                 clientId: str,
                 clientSecret: str,
                 tenantId: str):        
        self.storageAccount = storageAccount
        self.containerName = containerName
        self.sasToken = sasToken
        self.preSignedUrl = preSignedUrl
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.tenantId = tenantId

class SessionFileFormat:
    ICEBERG = "ICEBERG"

class DataLakeType:
    UNKNOWN = "UNKNOWN"
    AZURE = "AZURE"
    MINIO = "MINIO"
    S3 = "S3"

class FileFormatConfigurator:
    def configure(self, sparkConf: SparkConf):
        raise NotImplementedError("Subclasses should implement this method")

class IcebergConfigurator(FileFormatConfigurator):
    def configure(self, sparkConf: SparkConf):
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

class HiveSupportConfigurator:
    def configure(self, builder):
        return builder.enableHiveSupport()

class ExtraSparkConfigSettingsConfigurator:
    def __init__(self, settings: Dict[str, str]):
        self.settings = settings

    def configure(self, sparkConf: SparkConf):
        for key, value in self.settings.items():
            sparkConf.set(key, value)

class DataLakeConfigurator:
    def configure(self, sparkConf: SparkConf, connectionInfo: DataLakeConnectionInfo):
        raise NotImplementedError("Subclasses should implement this method")

class AzureConfigurator(DataLakeConfigurator):
    def configure(self, sparkConf: SparkConf, connectionInfo: DataLakeConnectionInfo):
        if connectionInfo.sasToken != "":
            sparkConf.set(f"fs.azure.account.auth.type.{connectionInfo.storageAccount}.dfs.core.windows.net", "SAS")
            sparkConf.set(f"fs.azure.sas.token.provider.type.{connectionInfo.storageAccount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
            sparkConf.set(f"fs.azure.sas.fixed.token.{connectionInfo.storageAccount}.dfs.core.windows.net", connectionInfo.sasToken)

        else:
            sparkConf.set(f"fs.azure.account.auth.type.{connectionInfo.storageAccount}.dfs.core.windows.net", "OAuth")
            sparkConf.set(f"fs.azure.account.oauth.provider.type.{connectionInfo.storageAccount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            sparkConf.set(f"fs.azure.account.oauth2.client.id.{connectionInfo.storageAccount}.dfs.core.windows.net", connectionInfo.clientId)
            sparkConf.set(f"fs.azure.account.oauth2.client.secret.{connectionInfo.storageAccount}.dfs.core.windows.net", connectionInfo.clientSecret)
            sparkConf.set(f"fs.azure.account.oauth2.client.endpoint.{connectionInfo.storageAccount}.dfs.core.windows.net", f"https://login.microsoftonline.com/{connectionInfo.tenantId}/oauth2/token")

class MinioConfigurator(DataLakeConfigurator):
    def configure(self, sparkConf: SparkConf, connectionInfo: DataLakeConnectionInfo):
        print('MINIO specific Spark configurations not set, please set the configurations and retry.')

class S3Configurator(DataLakeConfigurator):
    def configure(self, sparkConf: SparkConf, connectionInfo: DataLakeConnectionInfo):
        print('AWS-S3 specific Spark configurations not set, please set the configurations and retry.')

def parse_connection_info(json_string: str) -> DataLakeConnectionInfo:
    return json.loads(json_string, object_hook=lambda d: DataLakeConnectionInfo(**d))

# This function will return parameters retrieved from the workflow additionally to the create spark session
def create_spark_session_with_extension_data(
        appName: str,        
        hiveSupport: bool = False,
        warehouseLocationFromBase: str = "",
        fileFormats: List[SessionFileFormat] = [SessionFileFormat.ICEBERG],
        extraSparkConfigSettings: Dict[str, str] = None) -> Tuple[SparkSession,str,Optional[dict]]:

    try:
        spark,dataLakeUrl = create_spark_session(appName, hiveSupport, warehouseLocationFromBase, fileFormats, extraSparkConfigSettings)
        workflow_request_parameters = _read_workflow_request_parameters()

        return spark, dataLakeUrl, workflow_request_parameters
    except ValueError as e:
        print(f"Caught an error: {e}")
        
def create_spark_session(
        appName: str,        
        hiveSupport: bool = False,
        warehouseLocationFromBase: str = "",
        fileFormats: List[SessionFileFormat] = [SessionFileFormat.ICEBERG],
        extraSparkConfigSettings: Dict[str, str] = None) -> Tuple[SparkSession,str]:

    try:
        connectionInfo = _read_and_decrypt_data_lake_connection_info()
        spark = _setup_spark_session(appName, connectionInfo, warehouseLocationFromBase, hiveSupport, fileFormats, extraSparkConfigSettings)
        configure_log4j(spark)
        _write_log_to_consumption_map_file(spark)
        return spark, _create_data_lake_url(connectionInfo)

    except ValueError as e:
        print(f"Caught an error: {e}")

# This local function reads the PARAMS environment variable and returns a key value pair json object. 
def _read_workflow_request_parameters() -> dict:
    if os.getenv('PARAMS') is None:
        print("Info: PARAMS environment variable is not set")
        return None
    else:
        json_string: str = os.getenv('PARAMS')
        print(f"Info: PARAMS environment variable is set to {json_string}")
        try:
            # Convert the JSON string to a Python dictionary
            json_object = json.loads(json_string)
            print("Info: Successfully converted JSON string to Python object")
            return json_object
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse JSON string: {e}")
            return None

def _read_and_decrypt_data_lake_connection_info() -> str:
    json_string:str = os.getenv('DATALAKE_CONNECTION_INFO')
    symmetric_key:str = os.getenv('SYMMETRIC_KEY')
    
    decrypter = CipherManager(symmetric_key);
    dataLakeConnectionInfo: DataLakeConnectionInfo = parse_connection_info(json_string)    
    
    if(dataLakeConnectionInfo.clientSecret != "" and dataLakeConnectionInfo.clientSecret != None):
        dataLakeConnectionInfo.clientSecret = decrypter.symmetric_decrypt(dataLakeConnectionInfo.clientSecret)
    if(dataLakeConnectionInfo.sasToken != "" and dataLakeConnectionInfo.sasToken != None):
        dataLakeConnectionInfo.sasToken = decrypter.symmetric_decrypt(dataLakeConnectionInfo.sasToken)
        
    return dataLakeConnectionInfo

def _setup_spark_session(
        appName: str,        
        connectionInfo: DataLakeConnectionInfo,
        warehouseLocationFromBase: str,
        hiveSupport: bool = False,
        fileFormats: List[SessionFileFormat] = [SessionFileFormat.ICEBERG],
        extraSparkConfigSettings: Dict[str, str] = None) -> SparkSession:
    
    sparkConf = SparkConf().setAppName(appName)
    dataLakeType = _get_datalake_type(connectionInfo)
    if not dataLakeType or dataLakeType == DataLakeType.UNKNOWN:
        raise ValueError("Invalid datalake, exiting the program.")

    # Mapping file formats to their respective configurators
    file_format_configurators = {
        SessionFileFormat.ICEBERG: IcebergConfigurator()
    }

    # Apply configurations based on file formats
    for fileFormat in fileFormats:
        configurator = file_format_configurators.get(fileFormat)
        if configurator:
            configurator.configure(sparkConf)
    
    dataLakeUrl = _create_data_lake_url(connectionInfo)
    
    sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    sparkConf.set("spark.sql.catalog.spark_catalog.type", "hadoop")
    sparkConf.set("spark.sql.catalog.spark_catalog.warehouse", dataLakeUrl + warehouseLocationFromBase)

    # Apply extra Spark configuration settings if provided
    if extraSparkConfigSettings:
        ExtraSparkConfigSettingsConfigurator(extraSparkConfigSettings).configure(sparkConf)

    # Mapping data lake types to their respective configurators
    data_lake_configurators = {
        DataLakeType.AZURE: AzureConfigurator(),
        DataLakeType.MINIO: MinioConfigurator(),
        DataLakeType.S3: S3Configurator()
    }

    # Apply data lake specific configurations
    data_lake_configurator = data_lake_configurators.get(dataLakeType)
    if data_lake_configurator:
        data_lake_configurator.configure(sparkConf, connectionInfo)

    # Create SparkSession with or without Hive support
    builder = SparkSession.builder.config(conf=sparkConf)
    if hiveSupport:
        builder = HiveSupportConfigurator().configure(builder)

    return builder.getOrCreate()
        

def _write_log_to_consumption_map_file(spark: SparkSession):
    # Retrieve the Spark application ID
    spark_app_id = spark.sparkContext.applicationId
    os.environ['SPARK_APPLICATION_ID'] = spark_app_id
    print(f"Spark Application ID: {spark_app_id}")

    workflow_name = os.environ.get('WORKFLOW_NAME')
    spark_app_mount_path = os.environ.get('MOUNT_PATH')

    if spark_app_mount_path:
        print(f"MOUNT_PATH: {spark_app_mount_path}")

        spark_local_dir = f'{spark_app_mount_path}/log-index'
        print(f"spark_local_dir: {spark_local_dir}")

        if not os.path.exists(spark_local_dir):
            os.makedirs(spark_local_dir)
            print(f"Directory {spark_local_dir} created.")
        
        if workflow_name:
            print(f"WORKFLOW_NAME: {workflow_name}")
            index_log_file_name = f'{spark_local_dir}/{workflow_name}-{spark_app_id}'

            if not os.path.exists(index_log_file_name):
                with open(index_log_file_name, 'w') as file:
                    file.write('')  # Optionally, write something to the file
                print(f'Index log file "{index_log_file_name}" has been created.')
            else:
                print(f'Index log file "{index_log_file_name}" already exists.')
        else:
            print('Environment variable WORKFLOW_NAME is missing. Skipping log index file creation.')
    else:
        print("Environment variable MOUNT_PATH is missing. Skipping directory creation.")


def _get_datalake_type(connectionInfo: DataLakeConnectionInfo) -> str:
    if connectionInfo.sasToken != "":
        return DataLakeType.AZURE
    elif connectionInfo.clientSecret != "":
        return DataLakeType.AZURE    
    else:
        print("Error: Unknown datalake type. URL: " + connectionInfo)
        return DataLakeType.UNKNOWN
    
def _create_data_lake_url(connectionInfo: DataLakeConnectionInfo) -> str:
    return f"abfss://{connectionInfo.containerName}@{connectionInfo.storageAccount}.dfs.core.windows.net/"