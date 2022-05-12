from PyPDF2 import PdfFileReader, PdfFileWriter
from pdf2image import convert_from_path
import re
import io

class S3Uploader:

  def __init__(self, bucket, path, s3_client, page_range=None):
    self.bucket = bucket
    self.path = path
    self.s3 = s3_client
    self.page_range = page_range

  def subsetPDF(self):
    """
    Create new PDF file with the specified page range
    :return: none
    """
    pages = [page_num-1 for page_num in list(range(self.page_range[0],self.page_range[1]+1))]

    pdf = PdfFileReader(self.path)
    name = self.path.replace('pdf','')
    writer = PdfFileWriter()
    for page_num in pages:
      writer.addPage(pdf.getPage(page_num))

    subset_pdf_path = f'{name}_Page_{self.page_range[0]}_to_{self.page_range[1]}.pdf'
    with open(subset_pdf_path,'wb') as f:
      writer.write(f)

    return subset_pdf_path

  def get_document_name(self):
    """
    Return path of the PDF document and its name
    :return: none
    """

    pattern = re.compile(r'[^\\/]+(?=\.pdf$)')  #Regex pattern for matching file name

    #Return original path if no page range was given
    if self.page_range == None:
      doc_name = pattern.search(self.path).group()
      return self.path, doc_name
    #Return path of the subset PDF if page range was given
    else:
      subset_path = self.subsetPDF()
      doc_name = pattern.search(subset_path).group()
      return subset_path, doc_name

  def convert_to_png(self, path):
    """
    Convert file in path to PNG images returned as list of bytes (to be compatible for upload in S3)
    :param path: path of the PDF file to convert
    :return: png_byte_list: list of each PNG image as bytes
    """
    images = convert_from_path(path, fmt='png')
    png_byte_list = []

    for img in images:
      img_bytes = io.BytesIO()
      img.save(img_bytes, format=img.format)
      img_bytes = img_bytes.getvalue()
      png_byte_list.append(img_bytes)

    return png_byte_list

  def upload(self, png=False):
    """
    Upload to S3 bucket
    :return:
    """
    path, doc_name = self.get_document_name()

    #Uploads all PNG images into one folder in S3
    if png:
      png_byte_list = self.convert_to_png(path)
      s3_keys = [] #keys for every image
      for index,bytes in enumerate(png_byte_list):

        s3_key = doc_name + r'/' + str(index) + '.png'
        s3_keys.append(s3_key)
        self.s3.meta.client.put_object(Body=bytes, Bucket=self.bucket, Key=s3_key)
      return s3_keys

    #Upload PDF to bucket
    else:
      s3_key = doc_name + '.pdf'
      self.s3.meta.client.upload_file(Filename=path, Bucket=self.bucket, Key=s3_key)

      return s3_key

