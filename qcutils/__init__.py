import yaml
import logging
from botocore.exceptions import ClientError
import boto3
import io
import s3fs
from IPython.display import Markdown, display
import os
import traceback
import tarfile
import os

##Private functions
def __search_sub_node(node, lst):

    pname = lst.pop(0)
    subnode = node[pname]
    if (len(lst) > 0):
        return (__search_sub_node(subnode, lst))
    else:
        return subnode

def __make_tarfile(source_dir, output_path):
    try:
        if output_path.endswith('.tar.gz'):
            output_filename=output_path.split("/")[-1]
            with tarfile.open(output_path, "w:gz") as tar:
                tar.add(source_dir, arcname=output_filename)
            print("OK")
            return True
        else:
            print("The output_path must contains the name of the output .tar.gz archive")
            return False
    except:
        traceback.print_exc()
        return False

def __upload_file_s3(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

#Generic utils
def read_config_value(key,cf_path = "/home/jovyan/utils/config.yaml"):
    """Read, from a yaml-style config file, the value related to the key

    Parameters
    ----------
    key: str
        The key corresponding to the configuration value to extract
    cf_path: str, optional
        Absolute path of the configuration file  (default is /home/jovyan/utils/config.yaml)
	Returns
    -------
    object
        The value corresponding to the key
    """

    with open(cf_path) as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    pnames = key.split(".")

    return __search_sub_node(cfg, pnames)
        
def compress_folder(path="/home/jovyan/materials"):
    """Compress folder at specified path in a .tar.gz archive

    Parameters
    ----------
    path: str, optional
        Absolute path of the folder to compress  (default is/home/jovyan/materials)
    """
    try:
        path="/home/jovyan/materials/data-track/bootcamp"
        print("Compressing {} folder....".format(path.split("/")[-1]))
        jhub_user=os.environ['JUPYTERHUB_USER']
        output_filename=path.split("/")[-1]+"_"+jhub_user.replace(".", "_")+".tar.gz"
        if(__make_tarfile(path, "/home/jovyan/"+output_filename)):
            print("You can find your {} in your home folder".format(output_filename))
        else:
            raise Exception
    except:
        traceback.print_exc()  

def update_materials():
    """Update the folder /home/jovyan/materials with the new content from github repository the classes (this command try to perform a git merges)
    """
    print("updating materials folder....")
    os.system('gitpuller $GITHUB_REPO $GITHUB_BRANCH /home/jovyan/materials') 
    
def deliver_bootcamp(path="/home/jovyan/materials/bootcamp"):
    """Compress the specified folder and push the resulting archive on the quantia-bootcamp-results S3 bucket

    Parameters
    ----------
    path: str, optional
        Absolute path of the folder to compress and push (default is /home/jovyan/materials/bootcamp)
    """
    push_folder(path)

# Spark utils
def init_spark_session(spark_session, cf_path = "/home/jovyan/utils/config.yaml"):
    """Initialize an already existing SparkSession with the information to read from S3 using the s3a filesystem

    Parameters
    ----------
    spark_session: SparkSession
        The SparkSession object to initialize
    cf_path: str, optional
        Absolute path of the configuration file  (default is /home/jovyan/utils/config.yaml)
    """
    
    aws_key = read_config_value("aws.access.key")
    aws_secret = read_config_value("aws.access.secret")
    aws_key = read_config_value("aws.access.key")
    
    #Set-up the hadoop configuration to enable s3a filesystem
    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    #Enable better dataframe visualiztion in jupyter
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", True)
    
    return

# Kafka utils

def kafka_srv_description(cf_path = "/home/jovyan/utils/config.yaml"):
    """Show a table with all the kafka services available in the environment

    Parameters
    ----------
    cf_path: str, optional
        Absolute path of the configuration file  (default is /home/jovyan/utils/config.yaml)
    """
    from tabulate import tabulate
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError

    try:
        kconf_bkey = 'kafka'

        ksrv = read_config_value(key="{}.server".format(kconf_bkey), cf_path=cf_path)
        ksrvp = str(read_config_value(key="{}.port".format(kconf_bkey), cf_path=cf_path))

        z = read_config_value(key="{}.zookeper".format(kconf_bkey), cf_path=cf_path)

        sr_temp=read_config_value("{}.schema_registry.url".format(kconf_bkey), cf_path=cf_path).split(':')
        sr=sr_temp[0]+':'+sr_temp[1]
        srp=sr_temp[2]

        l = [["Kafka", ksrv, ksrvp], ["Zookeeper", z, "-"], ["Schema Registry", sr, srp]]
        table = tabulate(l, headers=['Service', 'Address', 'Port'], tablefmt='pretty')
        print(table)
    except:
        print("No kafka service available")

def create_kafka_topic(topic, security=False, cf_path = "/home/jovyan/utils/config.yaml", partitions=1,replication=1):
    """Create a new kafka topic on a kafka broker. The information related to kafka is stored in the config file.

    Parameters
    ----------
    topic: str
        The name of the topic
    security: bool, optional
        If True, the server is secured via SASL protocol and you need to specify `sasl.username` and `sasl.password` in the configuration file (default id False)
    cf_path: str, optional
        Absolute path of the configuration file  (default is /home/jovyan/utils/config.yaml)
    partitions: int, optional
        Number of partitions of the new topic (default is 1)
    replication: int, optional
        Number of replica of the new topic (default is 1)
    """
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError
    kconf_bkey = 'kafka'
    servers=read_config_value(key="{}.server".format(kconf_bkey), cf_path=cf_path) + ":" + str(read_config_value(key="{}.port".format(kconf_bkey), cf_path=cf_path))
    
    if security:
        username=read_config_value(key="{}.access.key".format(kconf_bkey), cf_path=cf_path)
        password=read_config_value(key="{}.access.secret".format(kconf_bkey), cf_path=cf_path)

        adminconf = {
                'bootstrap.servers': servers,
                'sasl.mechanisms': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': username,
                'sasl.password': password 
                }
    else:
        adminconf = {
                'bootstrap.servers': servers
                }

    a = AdminClient(adminconf)
    fs = a.create_topics([NewTopic(
         topic,
         num_partitions=partitions,
         replication_factor=replication
    )])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print("{}".format(e))


# S3 utils

def push_folder(path="/home/jovyan/materials"):
    """Compress the specified folder and push the resulting archive on the quantia-bootcamp-results S3 bucket

    Parameters
    ----------
    path: str, optional
        Absolute path of the folder to compress and push (default is /home/jovyan/materials)
    """
    compress_folder(path)
    print("Sending compressed {} to qc repo....".format(path.split("/")[-1]))
    ghb=os.environ['GITHUB_BRANCH']
    jhub_user=os.environ['JUPYTERHUB_USER']
    file_name=path.split("/")[-1]+"_"+jhub_user.replace(".", "_")+".tar.gz"
    res=__upload_file_s3("/home/jovyan/"+file_name, "quantia-bootcamp-results", ghb+"/"+file_name)
    if res:
        print("{} is now on qc remote repo -> {}".format(path.split("/")[-1], ghb+"/"+file_name))
        

def list_s3_bucket_objects(bucket_name='quantia-master', prefix='training', limit=10):
    """List objects in a S3 bucket and folder

    Parameters
    ----------
    bucket_name: str, optional
        Name of the S3 bucket to be listed (default is quantia-master)
    prefix: str, optional
        Prefix of the object to list. It can be as complex as you want. (default is training)
    limit: int, optional
        Max number of object to show (default is 10)
    """
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix =prefix)

    for obj in objects.get('Contents')[:limit]:
        print(obj.get('Key')) 

def print_s3_bucket_object(key, bucket_name='quantia-master', size=1000, decode=True):
    """Print an objects in a S3 bucket

    Parameters
    ----------
    key: str
        Path of the object
    bucket_name: str, optional
        Name of the S3 bucket that contains object (default is quantia-master)
    size: int, optional
        Number of bytes to be printed  (default is 1000)
    decode: bool, optional
        Decode the output streaming object from S3 using UTF-8 (default is True)
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(
        Bucket=bucket_name, 
        Key=key)

    if decode:
        body = obj.get('Body').read(size).decode(encoding="utf-8",errors="ignore")
    else:
        body = obj.get('Body').read(size)
    
    print(body)
