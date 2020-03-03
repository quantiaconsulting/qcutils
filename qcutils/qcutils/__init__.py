def init_session(spark_session):
    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "AKIAXH7JU6RNMFM23HUM")
    hadoop_conf.set("fs.s3a.secret.key", "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")
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return