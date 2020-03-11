import yaml

def init_session(spark_session, cf_path = "/home/jovyan/materials/utils/config.yaml"):

    with open(cf_path, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    #Read the AWS key and secret from cofiguration file
    aws_key = cfg['aws']['access']['key']
    aws_secret = cfg['aws']['access']['secret']
    
    #Set-up the hadoop configuration to enable s3a filesystem
    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    #Enable better dataframe visualiztion in jupyter
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", True)
    
    return
