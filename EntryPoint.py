import ConvertToXML
import io
import json
import lxml
import gzip
import urllib.parse
import urllib.request
import os
from extruct.rdfa import RDFaExtractor
import locale
import requests
import shutil

# region Global Variables
debug = True
use_extruct_as_extractor = False
iteration_count = 1
report_at_every_nth_step = 100_000
# endregion


def download_file(url, target_location, chunksize):
    response = requests.get(url, stream=True)
    handle = open(target_location, "wb")
    for chunk in response.iter_content(chunk_size=chunksize):
        if chunk:
            handle.write(chunk)


def extract_from_gzip_and_delete(file):
    with gzip.open(file, 'rb') as f_in:
        with open('WARCFile', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(file)


def read_gzip(url):
    with urllib.request.urlopen(url) as response:
        yield from gzip.open(io.BufferedReader(response))


def get_rdfa_from_warc(warc_file_no, path):
    global iteration_count
    global report_at_every_nth_step

    rdfaFileID = 1
    htmlURL = ''
    data = ''
    append = False

    rdfaExtractor = RDFaExtractor()

    if not os.path.exists('RDFa Files\\WARC_{0}'.format(warc_file_no)):
        os.makedirs('RDFa Files\\WARC_{0}'.format(warc_file_no))

    if not os.path.exists('XML Files\\WARC_{0}'.format(warc_file_no)):
        os.makedirs('XML Files\\WARC_{0}'.format(warc_file_no))

    print('[INFO/PROGRESS] The file being processed: {0}'.format(path))

    with open(path, encoding='utf-8', errors='replace') as file:
        for line in file:
            if debug and iteration_count % report_at_every_nth_step == 0:
                print("[DEBUG/PROGRESS] Processing line #{0:n}".format(iteration_count))

            if 'WARC/1.0' in line and append:
                append = False

                try:
                    rdfaData = rdfaExtractor.extract(data, base_url=htmlURL)

                    if rdfaData != []:
                        with open('RDFa Files\\WARC_{0}\\RDFa_{1}.txt'.format(warc_file_no, rdfaFileID), 'w', encoding='utf-8') as f:
                            f.write('URL: {0}\n\n'.format(htmlURL))
                            f.write(str(rdfaData))
                            f.close()

                            ConvertToXML.convertInstant(str(rdfaData), "XML Files\\WARC_{0}\\RDFa_{1}.xml".format(warc_file_no, rdfaFileID))
                            if debug:
                                print("[DEBUG/PROGRESS] Processed file #{0} at URI {1} successfully".format(rdfaFileID, htmlURL))
                except json.decoder.JSONDecodeError as jde:
                    print('[ERROR] Current file (#{0}) could not be converted to RDF/XML: {1} | This JSON-LD may be invalid.'.format(rdfaFileID, str(jde)))
                except lxml.etree.ParserError as pe:
                    print('[ERROR] Current file (#{0}) could not be converted to RDF/XML: {1} | This file may not have a valid RDFa representation.'.format(rdfaFileID, str(pe)))
                except Exception as exc:
                    if str(exc).startswith('Can\'t split'):
                        print('[ERROR] Current file (#{0}) could not be converted to RDF/XML: {1} | This file may be containing invalid XML namespaces.'.format(rdfaFileID, str(exc)))
                    else:
                        print('[ERROR] An error has occurred while processing current file (#{0}): {1}'.format(rdfaFileID, str(exc)))
                finally:
                    rdfaFileID += 1
                    data = ''
                    htmlURL = ''

            if 'WARC-Target-URI:' in line:
                htmlURL = line.replace('WARC-Target-URI: ', '').replace('\r', '').replace('\n', '')

            if '<!DOCTYPE html' in line or '<!doctype html' in line or '<html' in line:
                append = True

            if append:
                data = data + line + '\n'

            iteration_count = iteration_count + 1

    return iteration_count


def main():
    global iteration_count

    locale.setlocale(locale.LC_ALL, '')

    if not os.path.exists('RDFa Files'):
        os.makedirs('RDFa Files')

    if not os.path.exists('XML Files'):
        os.makedirs('XML Files')

    lines = read_gzip('https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-22/warc.paths.gz')
    paths = ['https://commoncrawl.s3.amazonaws.com/' + x.decode('utf-8').strip() for x in lines]

    i = 1
    for path in paths:
        try:
            print('[INFO/PROGRESS] Downloading {0:n}/{1:n}: {2}...'.format(i, len(paths), path))
            download_file(path, 'WARCFile.gz', 65536)
            extract_from_gzip_and_delete('WARCFile.gz')
        except Exception as exc:
            print('[ERROR] The file at {0} could not be downloaded: {1}'.format(path, str(exc)))
            i += 1
            continue

        processed_line_count = get_rdfa_from_warc(i, 'WARCFile')
        print('[INFO] Processing of {0}. completed. {1:n} lines were scanned.'.format(i, processed_line_count))
        i += 1
        iteration_count = 0
        os.remove('WARCFile')

    print('[INFO] The execution is finished.')


if __name__ == "__main__":
    main()
