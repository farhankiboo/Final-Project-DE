# import libraries
# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



# build mysql connection with spark
spark_mysql = SparkSession \
    .builder \
    .config("spark.jars", "/usr/local/spark/resources/mysql-connector-java-8.0.22.jar") \
    .appName("final_project") \
    .getOrCreate()


# get application_train dataframe from mysql 
application_train = spark_mysql.read.format('jdbc').options(
    url='jdbc:mysql://host.docker.internal:3306/mysql',
    # url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_train',
    user='root',
    password='1234').load()    


# get application_test dataframe from mysql 
application_test = spark_mysql.read.format('jdbc').options(
    url='jdbc:mysql://host.docker.internal:3306/mysql',
    # url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_test',
    user='root',
    password='1234').load()    
# application_test.show()

# build postgres connection with spark
spark_postgres = SparkSession \
    .builder \
    .config("spark.jars", "/usr/local/spark/resources/postgresql-42.2.25.jar") \
    .appName("final_project_postgres") \
    .getOrCreate()

# upload application_train dataframe to postgres
application_train.write.format('jdbc').options(
    url='jdbc:postgresql://host.docker.internal:5432/postgres',
    # url='jdbc:postgresql://localhost:5432/postgres',
    driver='org.postgresql.Driver',
    dbtable='home_credit_default_risk_application_train',
    user='postgres',
    password='1234').mode('ignore').save() 

# upload application_test dataframe to postgres
application_test.write.format('jdbc').options(
    url='jdbc:postgresql://host.docker.internal:5432/postgres',
    # url='jdbc:postgresql://localhost:5432/postgres',
    driver='org.postgresql.Driver',
    dbtable='home_credit_default_risk_application_test',
    user='postgres',
    password='1234').mode('ignore').save() 

