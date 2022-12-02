# init docker
## create docker network
``
docker network default_network
``

# airflow
``
docker-compose up -d --build
``

# spark
``
docker-compose up -d
``

# kafka
``
docker-compose up -d
``

# postgresql
``
docker run --name postgres-ds9 --network=default_network -e POSTGRES_PASSWORD=anypassword -d postgres
``

# mysql
``
docker run --name mysql-ds9 --network=default_network -e MYSQL_ROOT_PASSWORD=anypassword -d mysql
``