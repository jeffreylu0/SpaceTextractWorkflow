"""
Extract tables or text using Amazon Textract into CSV files.
Tables on separate pages are treated as different tables. Multipage tables will be split up within the csv.

Table extraction workflow:
    1. Return all detected table cells on a single page as a string
    2. Write each string to CSV along with table ID and page number

Text extraction workflow:
    1. Return raw text split by page
    2. Normalize text using Textacy preprocessing pipeline
    3. Segment each page's text into sentences
    4. Pass each sentence into a trained 'sentence relevance' classification model for postprocessing.
        a. Relevance of a sentence defined as:
            - Sentence is grammatically correct
            - Sentence has self-contained information (no reference to outside documents/figures/tables/visuals)
    5. Write relevant sentences into CSV file
"""
import time
import spacy
from textacy.preprocessing import pipeline, normalize, remove
import csv

class Textract:

    def __init__(self, bucket, textract_client):
        self.bucket = bucket
        self.textract = textract_client

    #Start job for table extraction
    def DocumentAnalysis(self):
        """
        Start Textract job for table extraction
        :return: none
        """
        response = self.textract.start_document_analysis(DocumentLocation={'S3Object': {'Bucket': self.bucket, 'Name': self.document}},
                                                             FeatureTypes=["TABLES"])
        self.jobId = str(response['JobId'])

    #Start job for text extraction
    def DocumentTextDetection(self):
        """
        Start Textract job for text extraction
        :return: none
        """
        response = self.textract.start_document_text_detection(DocumentLocation={'S3Object': {'Bucket': self.bucket, 'Name': self.document}})
        self.jobId = str(response['JobId'])

    def WaitForJob(self):
        """
        Wait for job to finish
        :return: none
        """

        if self.mode == 'table':
            time.sleep(5)
            response = self.textract.get_document_analysis(JobId=self.jobId)
            status = response['JobStatus']
            while status == 'IN_PROGRESS':
                time.sleep(5)
                response = self.textract.get_document_analysis(JobId=self.jobId)
                status = response['JobStatus']

        if self.mode == 'text':
            time.sleep(5)
            response = self.textract.get_document_text_detection(JobId=self.jobId)
            status = response['JobStatus']
            while status == 'IN_PROGRESS':
                time.sleep(5)
                response = self.textract.get_document_text_detection(JobId=self.jobId)
                status = response['JobStatus']

    def get_rows_columns_map(self, table_result, blocks_map):
        rows = {}
        for relationship in table_result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    try:
                        cell = blocks_map[child_id]
                        if cell['BlockType'] == 'CELL':
                            row_index = cell['RowIndex']
                            col_index = cell['ColumnIndex']
                            if row_index not in rows:
                                # create new row
                                rows[row_index] = {}

                            # get the text value
                            rows[row_index][col_index] = self.get_cell_text(cell, blocks_map)
                    except KeyError:
                        print("Error extracting Table data - {}:".format(KeyError))
                        pass
        return rows

    def get_cell_text(self, result, blocks_map):
        text = ''
        if 'Relationships' in result:
            for relationship in result['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        try:
                            word = blocks_map[child_id]
                            if word['BlockType'] == 'WORD':
                                text += word['Text'] + ' '
                            if word['BlockType'] == 'SELECTION_ELEMENT':
                                if word['SelectionStatus'] == 'SELECTED':
                                    text += 'X '
                        except KeyError:
                            print("Error extracting Table data - {}:".format(KeyError))

        return text

    def get_table_csv_results(self, blocks):

        blocks_map = {}
        table_blocks = []
        for block in blocks:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                table_blocks.append(block)

        if len(table_blocks) <= 0:
            return "<b> NO Table FOUND </b>"

        csv = ''
        for table in table_blocks:
            csv += self.generate_table_csv(table, blocks_map, table['Page'])
            csv += '\n\n'

        return csv

    def generate_table_csv(self, table_result, blocks_map, table_page):
        rows = self.get_rows_columns_map(table_result, blocks_map)

        csv = f'Page:{table_page}\n\n'

        for row_index, cols in rows.items():

            for col_index, text in cols.items():
                text_no_comma = text.replace(',',';') #replace commas with semicolons so text cell is not split in csv
                csv += f'{text_no_comma}' + ","
            csv += '\n'

        csv += '\n\n\n'
        return csv

    #Return all tables detected by Textract in a CSV
    def GetTablesCSV(self):
        """
        Return all tables detected by Textract in a CSV
        :return:
        """
        self.WaitForJob()

        paginationToken = None
        finished = False

        while finished == False:

            response = None
            tables = []

            if paginationToken == None:
                response = self.textract.get_document_analysis(JobId=self.jobId)
            else:
                response = self.textract.get_document_analysis(JobId=self.jobId, NextToken=paginationToken)

            blocks = response['Blocks']
            table_csv = self.get_table_csv_results(blocks)
            # replace content
            with open(self.output_csv_path, "at") as fout:
                fout.write(table_csv)

            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True
        return tables

    def GetTextLines(self):
        """
        Return list of text lines detected from Textract
        :return: none
        """
        self.WaitForJob()

        lines = []
        paginationToken = None
        finished = False

        while finished == False:

            response = None

            if paginationToken == None:
                response = self.textract.get_document_text_detection(JobId=self.jobId)
            else:
                response = self.textract.get_document_text_detection(JobId=self.jobId, NextToken=paginationToken)

            blocks = response['Blocks']

            for block in blocks:
                if block['BlockType'] == 'LINE':
                    lines.append((block['Text'],block['Page'])) #Tuple of detected text line with associated page number

            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True

            return lines

    #Convert detected Textract lines to sentences in CSV
    def GetSentencesCSV(self):
        """
        Convert detected Textract lines to sentences in CSV
        :return: none
        """

        lines = self.GetTextLines() #return list of tuples (text, page)
        raw_text = []

        last_page = max(lines,key=lambda x: x[1])[1]

        #Merge text lines that are from the same page
        for page in range(1,last_page+1):
            page_text = [line[0] for line in filter(lambda x:x[1]==page, lines)]
            raw_text.append(' '.join(page_text))

        #Setup textacy preprocessing pipeline
        preprocessor = pipeline.make_pipeline(normalize.unicode,
                                              normalize.whitespace,
                                              normalize.bullet_points,
                                              normalize.hyphenated_words,
                                              normalize.quotation_marks,
                                              remove.accents)

        #Sentence segmentation - preprocess raw text and split into sentences
        nlp = spacy.load('en_core_web_lg', exclude=['ner','lemmatizer'])
        preprocessed_text = [preprocessor(page) for page in raw_text]
        docs = nlp.pipe(preprocessed_text)

        sentences = []
        for doc in docs:
            for sentence in doc.sents:
                sentences.append(sentence.text)

        #Use trained sentence relevance model to filter out irrelevant/non-grammatical spans of text
        sent_relevance_model = spacy.load('./Models/sentence-relevance-model-tok2vec')
        sent_docs = sent_relevance_model.pipe(sentences)
        with open(self.output_csv_path, 'at', newline='', encoding='utf-8') as fout:
            fieldnames = ['inputs']
            writer = csv.DictWriter(fout,fieldnames=fieldnames)
            writer.writeheader()
            for doc in sent_docs:
                if doc.cats['relevant'] >= 0.95: #Keep texts with over 95% relevance
                    #Write to CSV
                    writer.writerow({'inputs':doc.text})

    def extract(self, mode, document, output_csv_path):
        """
        Main function for extraction on a document based on mode
        :return: path for output csv file
        """
        self.mode = mode
        self.document = document
        self.output_csv_path = output_csv_path

        if self.mode == 'table':
            self.DocumentAnalysis()
            self.GetTablesCSV()

        if self.mode == 'text':
            self.DocumentTextDetection()
            self.GetSentencesCSV()

        return self.output_csv_path



