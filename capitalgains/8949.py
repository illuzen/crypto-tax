from PyPDF2 import PdfFileMerger, PdfFileReader, PdfFileWriter
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from glob import glob
import csv
from tqdm import tqdm
from config import *
from credentials.credentials import name, ssn

merger = PdfFileMerger()
reader = csv.reader(open('%s/incomespend.csv' % derived_folder,'r'))
header = reader.__next__()

# id,currency,amount,cost_basis,price,timestamp,direction,origin_date,category

amount_idx = header.index('amount')
sold_date_idx = header.index('timestamp')
currency_idx = header.index('currency')
category_idx = header.index('category')
price_idx = header.index('price')
origin_idx = header.index('origin_date')
cost_basis_idx = header.index('cost_basis')
start = 0
i = start
max_digits = 2
rows_per_page = 14
time_fmt = '%Y/%m/%d'
second_page_offset = 36


def extract_row(row):
    d = {}
    d['prop_sold'] = '%s %s' % (row[amount_idx], row[currency_idx])
    d['origin_date'] = datetime.datetime.strptime(row[origin_idx],time_fmt).strftime('%m/%d/%y')
    d['sold_date'] = datetime.datetime.strptime(row[sold_date_idx],time_fmt).strftime('%m/%d/%y')
    usd_rcvd = round(float(row[amount_idx]) * float(row[price_idx]))
    cost_basis = round(float(row[cost_basis_idx]))
    diff = usd_rcvd - cost_basis
    if diff < 0:
        d['diff'] = '(%s)' % str(-diff)
    else:
        d['diff'] = str(diff)
    d['usd_rcvd'] = str(usd_rcvd)
    d['cost_basis'] = str(cost_basis)
    return d


def is_long_term(row):
    spend_date = datetime.datetime.strptime(row[sold_date_idx], time_fmt)
    origin_date = datetime.datetime.strptime(row[origin_idx], time_fmt)
    return (spend_date - origin_date).days > 364


def draw_pages(rows, short_term):
    rowNum = 0
    pages = []
    offset = 0 if short_term else second_page_offset
    totals = {
        'cost_basis': 0,
        'usd_rcvd': 0
    }
    for row in tqdm(rows):
        if row[category_idx] != 'spend': continue
        if is_long_term(row) is short_term: continue
        if int(row[sold_date_idx].split('/')[0]) != target_year: continue
        d = extract_row(row)
        if d['diff'] == '0': continue
        #if int(d['usd_rcvd']) == 0: continue
        #if int(d['cost_basis']) == 0: continue

        #if float(d['usd_rcvd']) < 1e-2: continue
        #if float(d['cost_basis']) < 1e-2: continue

        # if we filled up a page, append it and start over
        if rowNum % rows_per_page == 0:
            packet = io.BytesIO()
            # create a new PDF with Reportlab
            can = canvas.Canvas(packet, pagesize=letter)
            can.setFontSize(9)
            can.drawString(50, 688 + offset, name)
            can.drawString(500, 688 + offset, ssn)
            can.drawString(50, 518 + offset, 'X')

        yCoord = 430 - ((rowNum % 14) * 24) + offset
        rowNum += 1
        totals['cost_basis'] += float(d['cost_basis'])
        totals['usd_rcvd'] += float(d['usd_rcvd'])

        can.drawString(50, yCoord, d['prop_sold'])
        can.drawString(175, yCoord, d['origin_date'])
        can.drawString(225, yCoord, d['sold_date'])
        can.drawString(285, yCoord, d['usd_rcvd'])
        can.drawString(355, yCoord, d['cost_basis'])
        can.drawString(540, yCoord, d['diff'])

        # if we filled up a page, append it and start over
        if rowNum % rows_per_page == 0:
            can.save()
            #move to the beginning of the StringIO buffer
            packet.seek(0)
            pages.append(PdfFileReader(packet))
        #if rowNum % (3 * rows_per_page) == 0: break
    return pages, totals


# first page has 14 rows
def draw_first_pages(rows):
    return draw_pages(rows, short_term=True)


# second page has 14 rows
def draw_second_pages(rows):
    return draw_pages(rows, short_term=False)


def make_8949(reader):

    reader = csv.reader(open('%s/incomespend.csv' % derived_folder,'r'))
    reader.__next__()

    first_pages, totals = draw_first_pages(reader)
    print('short term totals', totals)

    reader = csv.reader(open('%s/incomespend.csv' % derived_folder,'r'))
    reader.__next__()

    second_pages,totals = draw_second_pages(reader)
    print('long term totals', totals)

    # add the "watermark" (which is the new pdf) on the existing page
    # read your existing PDF
    for pageNum, first_page in enumerate(tqdm(first_pages)):
        template = PdfFileReader(open('./capitalgains/8949-blank.pdf', 'rb'))
        output = PdfFileWriter()
        outputStream = open('%s/intermediate-8949/first-%d.pdf' % (derived_folder, pageNum), "wb")
        page = template.getPage(0)
        page.mergePage(first_page.getPage(0))
        output.addPage(page)
        output.write(outputStream)
        outputStream.close()

    for pageNum, second_page in enumerate(tqdm(second_pages)):
        template = PdfFileReader(open('./capitalgains/8949-blank.pdf', 'rb'))
        output = PdfFileWriter()
        outputStream = open('%s/intermediate-8949/second-%d.pdf' % (derived_folder, pageNum), "wb")
        page = template.getPage(1)
        page.mergePage(second_page.getPage(0))
        output.addPage(page)
        output.write(outputStream)
        outputStream.close()



make_8949(reader)


g = glob('%s/intermediate-8949/*.pdf' % derived_folder)
for path in tqdm(g):
    merger.append(PdfFileReader(open(path, 'rb')))

print('Merging all pdfs into one')
merger.write("%s/8949-complete-%s.pdf" % (derived_folder, datetime.datetime.now().timestamp()))
