from PyPDF2 import PdfFileMerger, PdfFileReader, PdfFileWriter
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from glob import glob
import csv
from tqdm import tqdm

merger = PdfFileMerger()
#reader = csv.reader(open('../derived_data/likekind.csv','r'))
reader = csv.reader(open('./likekind.csv','r'))
header = reader.__next__()

rcvd_idx = header.index('received')
rcvd_amt_idx = header.index('received_amount')
rlqd_idx = header.index('relinquished')
rlqd_amt_idx = header.index('relinquished_amount')
swap_idx = header.index('swap_date')
origin_idx = header.index('origin_date')
start = 0
i = start


for row in tqdm(reader):
    rlqd_desc = '%s %s' % (row[rlqd_amt_idx], row[rlqd_idx])
    rcvd_desc = '%s %s' % (row[rcvd_amt_idx], row[rcvd_idx])

    swap_date = row[swap_idx]
    year, month, day = swap_date.split('/')
    swap_date = '%s/%s/%s' % (month, day, year)

    origin_date = row[origin_idx]
    year, month, day = origin_date.split('/')
    origin_date = '%s/%s/%s' % (month, day, year)


    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)
    can.drawString(100, 626, rlqd_desc)
    can.drawString(100, 590, rcvd_desc)
    # MM/DD/YYYY
    can.drawString(478, 567, origin_date)
    can.drawString(478, 547, swap_date)
    can.drawString(478, 517, swap_date)
    can.drawString(478, 487, swap_date)

    can.save()

    #move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    output = PdfFileWriter()
    # add the "watermark" (which is the new pdf) on the existing page
    # read your existing PDF
    template = PdfFileReader(open('./8824-blank.pdf', 'rb'))
    page = template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    outputStream = open('./intermediate/%d.pdf' % i, "wb")
    i+=1
    output.write(outputStream)
    outputStream.close()

g = glob('./intermediate/*.pdf')
for path in tqdm(g):
    merger.append(PdfFileReader(open(path, 'rb')))

print('Merging all pdfs into one')
merger.write("output.pdf")
