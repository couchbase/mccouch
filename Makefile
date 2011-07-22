COUCH_SRC=../couchdb/src/couchdb
VERSION=$(shell git describe)

all: ebin compile

ebin:
	mkdir -p ebin

clean:
	rm -rf ebin *.tar.gz

compile:
	erl -pa build -noinput +B -eval 'case make:all([{i, "'${COUCH_SRC}'"}]) of up_to_date -> halt(0); error -> halt(1) end.'

mccouch-$(VERSION).tar.gz:
	git archive --prefix=mccouch-$(VERSION)/ --format tar HEAD | gzip -9vc > $@

dist: mccouch-$(VERSION).tar.gz
