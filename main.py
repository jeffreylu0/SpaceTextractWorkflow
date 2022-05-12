from extract import Textract
from upload import S3Uploader
from relation_pipeline import RelationsPipeline
import boto3
import argparse
import re
import sys

def main():

    if args.start and args.stop:
        page_range = (args.start, args.stop)
    else:
        page_range = None

    uploader = S3Uploader(bucket=bucket, path=args.input, s3_client=s3, page_range=page_range)
    extractor = Textract(bucket=bucket, textract_client=textract)
    output_path = args.output + '/' + args.job_name

    if args.mode == 'table':
        if args.png:
            s3_keys = uploader.upload(png=True)
            for key in s3_keys:
                extractor.extract(mode='table', document=key,
                                  output_csv_path=output_path + 'Tables.csv')
        else:
            s3_key = uploader.upload()
            extractor.extract(mode='table', document=s3_key,
                              output_csv_path=output_path + 'Tables.csv')

    if args.mode == 'text':
        s3_key = uploader.upload()
        text_path = extractor.extract(mode='text',document=s3_key,
                                      output_csv_path=output_path + 'Text.csv')
        if args.relationships:
            pipe = RelationsPipeline()
            pipe.export_relations(input_data=text_path,
                                  output_file=output_path + 'Relations.csv')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['table', 'text'])
    parser.add_argument('input')
    parser.add_argument('output')
    parser.add_argument('job_name')
    parser.add_argument('--png', action='store_true', dest='png', default=False)
    parser.add_argument('--labels', action='store_true', dest='labels', default=False)
    parser.add_argument('--relationships', action='store_true', dest='relationships', default=False)
    parser.add_argument('--start', dest='start', type=int)
    parser.add_argument('--stop', dest='stop', type=int)

    args = parser.parse_args()

    #Check if input is PDF
    pdf_check = re.compile(r'(\.pdf)$')
    if pdf_check.search(args.input) is None:
        raise Exception('Input file must be PDF')

    region_name = 'us-east-1'
    bucket = 'textract-bucket-test-1'
    textract = boto3.client('textract',region_name)
    s3 = boto3.resource('s3')

    main()



