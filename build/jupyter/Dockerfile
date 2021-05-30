# Get minimial jupyter notebook
FROM jupyter/minimal-notebook:d113a601dbb8

# Define env name and python version
ARG conda_env=comparing_urban
ARG py_ver=3.8.6

# Switch to root to install pkgs
USER root

RUN apt-get update && \ 
    apt-get install -y build-essential apt-utils cmake curl doxygen git graphviz gdal-bin libgdal-dev libgeos-dev unzip wget

# Install exactextract # version: c1831a052600c67577279d0bc02b05fa73c2a1b2
RUN git clone https://github.com/isciences/exactextract && \
    cd exactextract && \
    mkdir cmake-build-release && \
    cd cmake-build-release && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install

# Create conda env using defined conda pkgs
RUN conda create --quiet --yes -p $CONDA_DIR/envs/$conda_env python=$py_ver ipython ipykernel geopandas=0.8.0 descartes palettable=3.3.0 sqlalchemy=1.4.7 geoalchemy2=0.6.3 xarray=0.18.2 rioxarray=0.4.1 xlrd=2.0.1 contextily=1.1.0 matplotlib=3.4.1 affine=2.2.0 seaborn=0.11.1 scikit-learn=0.24.0 psycopg2=2.8.5 lxml dask=2021.2.0 pyarrow=3.0.0  && \
    conda clean --all -f -y

# Make conda env a kernel that can be selected in jupyter
RUN $CONDA_DIR/envs/${conda_env}/bin/python -m ipykernel install --user --name=${conda_env} && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# Prepend conda env to path of base conda env - same as conda init
#e.g. SHELL ["/bin/bash","-c"] /n RUN conda init
ENV PATH $CONDA_DIR/envs/${conda_env}/bin:$PATH
# Activate conda env as default
RUN echo "source activate ${conda_env}" > ~/.bashrc
# Install postgis 3.0 & postgresql 12
RUN conda update conda && conda config --add channels conda-forge && conda install postgis
# Set conda env as default
ENV CONDA_DEFAULT_ENV ${conda_env}

# Create conda env using yml file
# COPY --chown=${NB_UID}:${NB_GID} environment.yml /home/$NB_USER/tmp/
# RUN cd /home/$NB_USER/tmp/ && \
#     conda env create -p $CONDA_DIR/envs/$conda_env -f environment.yml && \
#     conda clean --all -f -y
# Add any additional pkgs using pip
# RUN $CONDA_DIR/envs/${conda_env}/bin/pip install 
