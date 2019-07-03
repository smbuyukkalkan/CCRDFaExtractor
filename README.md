# Common Crawl RDFa Extractor
Common Crawl RDFa Extractor is a Python program that extracts RDFa data from Common Crawl's archives. The program was developed for senior design in university, as a part of a greater project that has the main idea of estimating meaningful RDFa data contained in the Internet.<br/>

## Running The Project
The program uses several internal and external libraries, which can be downloaded using [Python Package Index](https://pip.pypa.io/en/stable/). To acquire the libraries, open a terminal or command line and do the following:<br/>
```bash
pip install extruct
pip install rdflib
pip install requests
```
Then run `EntryPoint.py` file.</br>

## The Algorithm
The program does the following works:
1. Download web archive (`warc`) files from Common Crawl one by one,
1. Read every file line by line,
1. Extract RDFa data along with the URL of the web site,
1. Convert the RDFa data to `RDF/XML` and save to the file system.

## Notes
* The program is designed to run on a cloud computing environment, like AWS or Google Cloud Compute.
* The program automatically deletes downloaded `warc` file after the analysis of the file.
* The program downloads May 2019 version of Common Crawl archives, but can be changed by changing the `lines = read_gzip...` line.
* Keep in mind that the program will generate tens of thousands of `txt` and `xml` files. `txt` files are generated as a fallback if some RDFa data cannot be converted to `RDF/XML`.
* Due to several reasons, mostly because libraries that does not like some input types, roughly between 50% - 60% of the files are successfully converted to `RDF/XML`.
