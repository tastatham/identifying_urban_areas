#!/bin/bash
mkdir data notebooks jupyter

# Define Dockerfile path
DOCKERFILE="./jupyter/Dockerfile"
COMPOSE="docker-compose.yml"
USER="tastatham"
# Create files
touch $DOCKERFILE
touch $COMPOSE

# Populate dockerfile
#USER $NB_USER \n \
echo -e "\
FROM jupyter/minimal-notebook \n \
ARG NB_USER=jovyan \n \
RUN conda install --quiet --yes psycopg2 \n \ 
RUN conda install python-dotenv  \n \
VOLUME /notebooks \n \
USER $NB_USER \n \
WORKDIR /notebooks" > $DOCKERFILE

# Populate docker-compose
echo -e "\
version: '"3"' \n\
services: \n \
 jupyter: \n \
   build: \n \
     context: ./jupyter \n \
   user: root \n \
   working_dir: /home/${USER}/work \n \
   ports: \n \
     - '"8888:8888"' \n \
   links: \n \
     - postgres \n \
   volumes: \n \
     - "notebooks:/home/${USER}/work" \n \
   environment: \n \
     - JUPYTER_ENABLE_LAB: 1 \n \
     - NB_USER: ${USER} \n \ 
     - NB_GID: 1000 \n \
     - CHOWN_HOME: 'yes' \n \
     - CHOWN_HOME_OPTS: '-R' \n \
 postgres: \n \
   image: postgres \n \
   restart: always \n \
   environment: \n \
     POSTGRES_USER: tastatham \n \
     POSTGRES_PASSWORD: mypassword \n \
     POSTGRES_DB: data" > $COMPOSE

# Create container
sudo docker-compose up