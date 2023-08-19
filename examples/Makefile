CDATA = pypolars_data

conduit: clean build
	../cmd/conduit/conduit -d $(CDATA)

build:
	cd .. && make

clean:
	rm -f $(CDATA)/metadata.json