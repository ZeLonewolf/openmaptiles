#!/bin/sh

make destroy-db
make clean
make all
make download
make start-db
make import-data
make import-osm
#make import-wikidata

