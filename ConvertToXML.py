from rdflib import Graph


def convertInstant(data, file_name):
    data = str(data).replace('\'', '\"')
    obj = Graph().parse(data=data, format='json-ld')
    xml = obj.serialize(format='xml', indent=4)

    xmlFile = open('{0}'.format(file_name), "w", encoding="utf-8")
    xmlFile.write(xml.decode("utf-8"))
    xmlFile.close()


if __name__ == "__main__":
    print("This code is meant to be run by the Common Crawl RDFa Extractor Utility, and cannot be run directly. Use convertInstant function instead.")
    exit(1)
