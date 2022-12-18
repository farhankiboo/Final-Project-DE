# import libraries
# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# build connection with spark
spark = SparkSession \
    .builder \
    .config("spark.jars", "/usr/local/spark/resources/mysql-connector-java-8.0.22.jar") \
    .appName("final_project") \
    .getOrCreate()


# get data for application_train
application_train = spark.read \
                .format("csv") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load('/usr/local/spark/resources/application_train.csv')
# application_train.show()


# get data for application_test
application_test = spark.read \
                .format("csv") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load('/usr/local/spark/resources/application_test.csv')
    

# upload data application_train to mysql
application_train.write.format('jdbc').options(
    url='jdbc:mysql://host.docker.internal:3306/mysql',
    # url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_train',
    user='root',
    password='1234').mode('ignore').save()


# upload data application_test to mysql
application_test.write.format('jdbc').options(
    url='jdbc:mysql://host.docker.internal:3306/mysql',
    # url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_test',
    user='root',
    password='1234').mode('ignore').save()