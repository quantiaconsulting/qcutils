import yaml
import requests
import logging
from botocore.exceptions import ClientError
import boto3
from IPython.display import Markdown, display
import os
import traceback
import tarfile
import shutil
import tarfile
import configparser

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

def __download_file_s3(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Download the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.download_file(bucket, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

#Generic utils
def read_config_value(key, github_user="", github_token="", remote_cf_version = "0.6.0", cf_path = "/home/jovyan/utils/config.yaml"):
    """Read, from a yaml-style config file, the value related to the key

    Args:
        key: str
        The key corresponding to the configuration value to extract
        github_user: str, optional: 
        The Github User to access the config file on the private quantia repository. Defaults to "".
        github_token str, optional: 
        The Github Token to access the config file on the private quantia repository. Defaults to "".
        remote_cf_version str, optional: 
        Version of the remote config file to download. Defaults to "0.6.0".
        cf_path: str, optional
        Absolute path of the configuration file. Default to "/home/jovyan/utils/config.yaml"
    """    

    if not os.path.exists(cf_path):
        print("Getting config files from remote")
        if github_user == "":
            github_user=os.environ['GITHUB_USER']
        if github_token == "":
            github_token=os.environ['GITHUB_TOKEN']
        if os.environ['MODE'] == "local":
            with open(cf_path, 'wb') as config_file:
                url = 'https://{}:{}@raw.githubusercontent.com/quantiaconsulting/qc-edu-platform-dp/master/utils/config_files/hare-config-local-{}.yaml'.format(github_user, github_token, remote_cf_version)
                r = requests.get(url, allow_redirects=True)
                config_file.write(r.content)
        else:
            with open(cf_path, 'wb') as config_file:
                url = 'https://{}:{}@raw.githubusercontent.com/quantiaconsulting/qc-edu-platform-dp/master/utils/config_files/hare-config-remote-{}.yaml'.format(github_user, github_token, remote_cf_version)
                r = requests.get(url, allow_redirects=True)
                config_file.write(r.content)

        while not os.path.exists(cf_path):
            print("Waiting for config file to be ready")
            time.sleep(1)

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
    push_to_remote("quantia-bootcamp-results", path)

def persist_user_materials(path="/home/jovyan/materials"):
    """Compress the specified folder and push the resulting archive on the quantia-platform-users S3 bucket

    Parameters
    ----------
    path: str, optional
        Absolute path of the folder to compress and push (default is /home/jovyan/materials)
    """
    push_to_remote("quantia-platform-users", path)

def restore_user_materials(bucket="quantia-platform-users", local_file_path="/home/jovyan/"):
    """Pull the user folder from quantia-platform-users and uncompress it into persistent-materials folder

    Parameters
    ----------
        bucket (str, optional): Bucket containing persisted data. Defaults to "quantia-platform-users".
        local_file_path (str, optional): Base-path to be used to restore persisted materials. Defaults to "/home/jovyan/".
    """    
    
    
    
    """Pull the user folder from quantia-platform-users and uncompress it into persistent-materials folder

    Parameters
    ----------
    path: str, optional
        Absolute path of the folder to compress and push (default is /home/jovyan/materials)
    """

    #!/usr/bin/python
    import os

    # Remove persistent-materials original folder
    folder_path = "/home/jovyan/persistent-materials"
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    
    tar_file=pull_from_remote(bucket, local_file_path)

    my_tar = tarfile.open(tar_file)
    for member in my_tar.getmembers():
        if (".ipynb_checkpoints" not in member.name):
            my_tar.extract(member, path="/home/jovyan/tmp")  
    my_tar.close()

    os.mkdir("/home/jovyan/persistent-materials")
    source_dir = "/home/jovyan/tmp/"+tar_file.split("/")[3]
    target_dir = '/home/jovyan/persistent-materials'
        
    file_names = os.listdir(source_dir)
        
    for file_name in file_names:
        shutil.move(os.path.join(source_dir, file_name), target_dir)

    os.remove(tar_file)
    shutil.rmtree("/home/jovyan/tmp/")

def restore_user_bootcamp(bucket="quantia-bootcamp-results", local_file_path="/home/jovyan/"):
    # Remove persistent-materials original folder
    folder_path = "/home/jovyan/materials/bootcamp"
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    
    tar_file=pull_from_remote(bucket, local_file_path)
    print(tar_file)
    my_tar = tarfile.open(tar_file)
    for member in my_tar.getmembers():
        if (".ipynb_checkpoints" not in member.name):
            my_tar.extract(member, path="/home/jovyan/tmp")  
    my_tar.close()

    os.mkdir("/home/jovyan/materials/bootcamp")
    source_dir = "/home/jovyan/tmp/"+tar_file.split("/")[-1]
    target_dir = '/home/jovyan/materials/bootcamp'
        
    file_names = os.listdir(source_dir)
        
    for file_name in file_names:
        shutil.move(os.path.join(source_dir, file_name), target_dir)

    os.remove(tar_file)
    shutil.rmtree("/home/jovyan/tmp/")

# Spark utils
def init_spark_session(spark_session):
    """Initialize an already existing SparkSession with the information to read from S3 using the s3a filesystem

    Parameters
    ----------
    spark_session: SparkSession
        The SparkSession object to initialize
    """
    
    config = configparser.RawConfigParser()
    path = os.path.expanduser('~/.aws/credentials')
    config.read(path)

    aws_key=config['default']["aws_access_key_id"]
    aws_secret=config['default']["aws_secret_access_key"]
    
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

def push_to_remote(bucket, path="/home/jovyan/materials"):
    """Compress the specified folder and push the resulting archive on the quantia-bootcamp-results S3 bucket

    Parameters
    ----------
    path: str, optional
        Absolute path of the folder to compress and push (default is /home/jovyan/materials)
    bucket: str
        Name of the bucket to be used as destination
    """
    compress_folder(path)
    print("Sending compressed {} to qc repo....".format(path.split("/")[-1]))
    ghb=os.environ['GITHUB_BRANCH']
    jhub_user=os.environ['JUPYTERHUB_USER']
    file_name=path.split("/")[-1]+"_"+jhub_user.replace(".", "_")+".tar.gz"
    res=__upload_file_s3("/home/jovyan/"+file_name, bucket, ghb+"/"+file_name)
    if res:
        print("{} is now on qc remote repo -> {}".format(path.split("/")[-1], ghb+"/"+file_name))
        
def pull_from_remote(bucket, local_file_path):
    """Pull the user folder from the specified bucket

    Parameters
    ----------
    bucket: str
        Name of the bucket to be used as destination
    local_file_path (str): 
        Local path for downloading the file
    """

    if not local_file_path.endswith("/"):
        local_file_path = local_file_path+"/"

    ghb=os.environ['GITHUB_BRANCH']
    jhub_user=os.environ['JUPYTERHUB_USER']
    file_name=jhub_user.replace(".", "_")+".tar.gz"
    print(file_name + ","+ bucket + ","+  ghb+"/*"+file_name)

    s3_rs = boto3.resource('s3')
    s3_client = boto3.client('s3')

    bucket_obj = s3_rs.Bucket(bucket)

    objects = bucket_obj.objects.filter(Prefix=ghb+"/")

    for object in objects:
        if object.key.endswith(file_name):
            s3_client.download_file(bucket, object.key, local_file_path + object.key.split("/")[1])
            return local_file_path + object.key.split("/")[1]

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
