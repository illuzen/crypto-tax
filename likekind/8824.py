from PyPDF2 import PdfFileMerger, PdfFileReader, PdfFileWriter
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from glob import glob
import csv
from tqdm import tqdm
from config import *
from multiprocessing.pool import Pool
from credentials.credentials import name, ssn
from math import modf

merger = PdfFileMerger()
reader = csv.reader(open('%s/likekind.csv' % derived_folder,'r'))
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
cents_delta = 74


def dollars_delta(num):
    return 62 - (5 * len(num))


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


def dollars_cents(num):
    cents, dollars = modf(num)
    return stringify(int(dollars), int(cents * 100))


def stringify(dollars, cents):
    if dollars < 0:
        dollar_str = '(%s' % (-1 * dollars)
        if abs(cents) < 10:
            cents_str = '0%s)' % (-1 * cents)
        else:
            cents_str = '%s)' % (-1 * cents)
    else:
        dollar_str = str(dollars)
        if abs(cents) < 10:
            cents_str = '0%s' % str(cents)
        else:
            cents_str = str(cents)
    return dollar_str, cents_str


def get_second_page_text(row):
    rlqd_fmv = float(row[rlqd_price_idx]) * float(row[rlqd_amt_idx])
    rcvd_fmv = float(row[rcvd_price_idx]) * float(row[rcvd_amt_idx])
    cost_basis = float(row[cost_basis_idx])
    rlqd_gain = rlqd_fmv - cost_basis
    rcvd_gain = rcvd_fmv - cost_basis
    rlqd_fmv_dollars, rlqd_fmv_cents = dollars_cents(rlqd_fmv)
    rcvd_fmv_dollars, rcvd_fmv_cents = dollars_cents(rcvd_fmv)
    rlqd_gain_dollars, rlqd_gain_cents = dollars_cents(rlqd_gain)
    rcvd_gain_dollars, rcvd_gain_cents = dollars_cents(rcvd_gain)
    cost_basis_dollars, cost_basis_cents = dollars_cents(cost_basis)

    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)
    can.drawString(50, 724, name)
    can.drawString(500, 724, ssn)
    can.drawString(369 + dollars_delta(rlqd_fmv_dollars), 664, rlqd_fmv_dollars)
    can.drawString(369 + cents_delta, 664, rlqd_fmv_cents)
    can.drawString(369 + dollars_delta(cost_basis_dollars), 650, cost_basis_dollars)
    can.drawString(369 + cents_delta, 650, cost_basis_cents)
    can.drawString(484 + dollars_delta(rlqd_gain_dollars), 627, rlqd_gain_dollars)
    can.drawString(484 + cents_delta, 627, rlqd_gain_cents)
    can.drawString(484 + dollars_delta('0'), 580, '0')
    can.drawString(484 + dollars_delta(rcvd_fmv_dollars), 566, rcvd_fmv_dollars)
    can.drawString(484 + cents_delta, 566, rcvd_fmv_cents)
    can.drawString(484 + dollars_delta(rcvd_fmv_dollars), 554, rcvd_fmv_dollars)
    can.drawString(484 + cents_delta, 554, rcvd_fmv_cents)
    can.drawString(484 + dollars_delta(cost_basis_dollars), 535, cost_basis_dollars)
    can.drawString(484 + cents_delta, 535, cost_basis_cents)
    can.drawString(484 + dollars_delta(rcvd_gain_dollars), 518, rcvd_gain_dollars)
    can.drawString(484 + cents_delta, 518, rcvd_gain_cents)
    can.drawString(484 + dollars_delta('0'), 506, '0')
    can.drawString(484 + dollars_delta('0'), 494, '0')
    can.drawString(484 + dollars_delta('0'), 477, '0')
    can.drawString(484 + dollars_delta('0'), 458, '0')
    can.drawString(484 + dollars_delta(rcvd_gain_dollars), 446, rcvd_gain_dollars)
    can.drawString(484 + cents_delta, 446, rcvd_gain_cents)
    can.drawString(484 + dollars_delta(cost_basis_dollars), 434, cost_basis_dollars)
    can.drawString(484 + cents_delta, 434, cost_basis_cents)
    can.save()
    #move to the beginning of the StringIO buffer
    packet.seek(0)
    return PdfFileReader(packet)


def make_8824(row):
    if float(row[rcvd_amt_idx]) < 1e-2 or float(row[rlqd_amt_idx]) < 1e-2: return
    if int(row[swap_idx].split('/')[0]) != target_year: return
    if float(row[rcvd_price_idx]) * float(row[rcvd_amt_idx]) < likekind_threshold: return

    output = PdfFileWriter()
    template = PdfFileReader(open('./likekind/8824-blank.pdf', 'rb'))

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

    filename = '%s/intermediate-8824/%s-%s-%s-%s.pdf' % (derived_folder, row[swap_idx].replace('/', '-'),row[rcvd_idx], row[rlqd_idx], datetime.datetime.now().timestamp())
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




g = glob('%s/intermediate-8824/*.pdf' % derived_folder)
for path in tqdm(g):
    merger.append(PdfFileReader(open(path, 'rb')))

maybe_print('Merging all %d pdfs into one' % len(g))
merger.write("%s/8824-complete-%s.pdf" % (derived_folder,datetime.datetime.now().timestamp()))
