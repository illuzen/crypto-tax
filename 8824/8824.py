from PyPDF2 import PdfFileMerger, PdfFileReader, PdfFileWriter
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from glob import glob
import csv
from tqdm import tqdm
from config import *
from multiprocessing.pool import Pool

merger = PdfFileMerger()
reader = csv.reader(open('../%s/likekind.csv' % derived_folder,'r'))
header = reader.__next__()

rcvd_idx = header.index('received')
rcvd_amt_idx = header.index('received_amount')
rcvd_price_idx = header.index('received_price')
rlqd_idx = header.index('relinquished')
rlqd_amt_idx = header.index('relinquished_amount')
rlqd_price_idx = header.index('relinquished_price')
swap_idx = header.index('swap_date')
origin_idx = header.index('origin_date')
cost_basis_idx = header.index('cost_basis')
max_digits = 2


#################
#################
#################
#################
### FILL OUT ####
#################
#################
#################
#################

name = ''
ssn = ''


def get_first_page_text(row):
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
    can.drawString(50, 688, name)
    can.drawString(500, 688, ssn)
    can.drawString(100, 626, rlqd_desc)
    can.drawString(100, 590, rcvd_desc)
    # MM/DD/YYYY
    can.drawString(478, 567, origin_date)
    can.drawString(478, 547, swap_date)
    can.drawString(478, 517, swap_date)
    can.drawString(478, 487, swap_date)
    can.drawString(542, 447, 'X')

    can.save()
    #move to the beginning of the StringIO buffer
    packet.seek(0)
    return PdfFileReader(packet)


def get_second_page_text(row):
    rlqd_fmv = float(row[rlqd_price_idx]) * float(row[rlqd_amt_idx])
    rcvd_fmv = float(row[rcvd_price_idx]) * float(row[rcvd_amt_idx])
    cost_basis = float(row[cost_basis_idx])
    rlqd_fmv = round(rlqd_fmv, max_digits)
    rcvd_fmv = round(rcvd_fmv, max_digits)
    cost_basis = round(cost_basis, max_digits)
    rlqd_gain = round(rlqd_fmv - cost_basis, max_digits)
    rlqd_gain = '(%s)' % (-1 * rlqd_gain) if rlqd_gain < 0 else str(rlqd_gain)
    rcvd_gain = round(rcvd_fmv - cost_basis, max_digits)
    rcvd_gain = '(%s)' % (-1 * rcvd_gain) if rcvd_gain < 0 else str(rcvd_gain)
    rlqd_fmv = str(rlqd_fmv)
    rcvd_fmv = str(rcvd_fmv)
    cost_basis = str(cost_basis)

    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)
    can.drawString(50, 724, name)
    can.drawString(500, 724, ssn)
    can.drawString(369, 664, rlqd_fmv)
    can.drawString(369, 650, cost_basis)
    can.drawString(484, 627, rlqd_gain)
    can.drawString(484, 580, '0')
    can.drawString(484, 566, rcvd_fmv)
    can.drawString(484, 554, rcvd_fmv)
    can.drawString(484, 535, cost_basis)
    can.drawString(484, 518, rcvd_gain)
    can.drawString(484, 506, '0')
    can.drawString(484, 494, '0')
    can.drawString(484, 477, '0')
    can.drawString(484, 458, '0')
    can.drawString(484, 446, rcvd_gain)
    can.drawString(484, 434, cost_basis)
    can.save()
    #move to the beginning of the StringIO buffer
    packet.seek(0)
    return PdfFileReader(packet)


def make_8824(row):
    if float(row[rcvd_amt_idx]) < 1e-2 or float(row[rlqd_amt_idx]) < 1e-2: return

    output = PdfFileWriter()
    template = PdfFileReader(open('./8824-blank.pdf', 'rb'))

    text0 = get_first_page_text(row)
    # add the "watermark" (which is the new pdf) on the existing page
    # read your existing PDF
    page = template.getPage(0)
    page.mergePage(text0.getPage(0))
    output.addPage(page)

    text1 = get_second_page_text(row)
    page = template.getPage(1)
    page.mergePage(text1.getPage(0))
    output.addPage(page)

    filename = './intermediate/%d.pdf' % datetime.datetime.now().timestamp()
    outputStream = open(filename, "wb")
    output.write(outputStream)
    outputStream.close()
    maybe_print('Wrote file %s' % filename)


if parallel:
    with Pool(num_processes) as p:
        _ = p.map(make_8824, reader)
else:
    for row in tqdm(reader):
        make_8824(row)


g = glob('./intermediate/*.pdf')
for path in tqdm(g):
    merger.append(PdfFileReader(open(path, 'rb')))

maybe_print('Merging all %d pdfs into one' % len(g))
merger.write("output.pdf")
