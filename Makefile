build: clean-build
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -x \*libs\* -x \*pipeline\* -r ../dist/jobs.zip .
	cd ./src/libs && zip -r ../../dist/libs.zip .
	cd ./src/pipeline && zip -r ../../dist/pipeline.zip .

clean-build:
	rm -fr dist/
