from argparse import ArgumentParser
from os.path import join

from PyrepBUFR.tables import write_xml
from PyrepBUFR.utility import convert_ncep_table

parser = ArgumentParser(description='Process NCEP BUFR tables into XML format')
parser.add_argument('-d', '--dir', action='store', dest='output_dir', type=str, default='', help='Directory where XML table will be written, default is current directory')
parser.add_argument('filename', metavar='FILENAME', type=str, help='File to convert')

args = parser.parse_args()

table = convert_ncep_table(args.filename)

table_name = '_'.join(['table'] + [str(getattr(table, prop)) for prop in table.id._fields if getattr(table, prop) is not None]) + '.xml'

write_xml(table, join(args.output_dir, table_name))
