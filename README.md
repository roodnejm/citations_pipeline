Implement a Spark job that generates a json file representing a graph 
of drugs, medical publications, clinical trials and journals. 

module package allows to:
- import files from various sources
- apply various processing to clean and merge data
- generate and save output file as json formatted graph
- run the data pipeline as a Spark job to execute all the above tasks at once

prerequisites:

tested with Python 3.7 and Spark 3.1.1
install dependencies from requirement.txt

add libraries to src before building:
mkdir ./src/libs
pip install -r requirements.txt -t ./src/libs

build the project:
make build

Run full pipeline  

cd dist && spark-submit --master local --py-files jobs.zip,libs.zip,pipeline.zip main.py --job run

Tested only in local mode





