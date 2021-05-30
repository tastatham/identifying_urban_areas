from setuptools import setup, find_packages

setup(
    name="comparing_urban_populations",
    version="0.0.1",
    packages=find_packages(),
    #install_requires=['geopandas','Psycopg2','rasterstats', 'fiona',
    #                  'sklearn'],
    include_package_data=True,
    #tests_require=['pytest'],
    author="Thomas Statham",
    author_email="tastatham@gmail.com",
    maintainer="Thomas Statham", 
    description=("Code repository for two papers:"
                 "'Identifying urban areas by applying clustering techniques and density thresholds to gridded population datasets'"
                 "'Urban structural changes using different urban boundary definitions'"),
    license="BSD",
    keywords=("urban", "boundaries", "clustering", 'gridded population data', 'urban structure')
    #url="",
)