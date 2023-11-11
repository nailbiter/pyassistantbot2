all: Dockerfile

Dockerfile: Dockerfile.jinja produce-docker.py
	./produce-docker.py -i Dockerfile.jinja -o $@
