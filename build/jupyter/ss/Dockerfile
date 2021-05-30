FROM jupyter/minimal-notebook 
 ARG NB_USER=jovyan 
 RUN conda install --quiet --yes psycopg2 geopandas 
 RUN pip install python-dotenv
 VOLUME /notebooks 
 USER $NB_USER 
 WORKDIR /notebooks
