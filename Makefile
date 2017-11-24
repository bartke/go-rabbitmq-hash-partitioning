
SHELL := /bin/bash

.PHONY: producer consumer
default: all

all: run

run: producer consumer
	echo "starting consumer1"
	./consumer/consumer &
	echo "starting producer"
	./producer/producer &

producer:
	$(MAKE) -C $@

consumer:
	$(MAKE) -C $@

