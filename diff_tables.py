from argparse import ArgumentParser
from os.path import join

from PyrepBUFR.tables import read_xml, write_xml

parser = ArgumentParser(description='Compare table versions and output a table with the differences.')
parser.add_argument('-m', '--master-table', metavar='MASTERTABLE', action='store', dest='master_table', type=int, default=0, help='Master table ID to use')
parser.add_argument('-o', '--originating-center', metavar='CENTER', action='store', dest='originating_center', type=int, default=None, help='Originating center ID to use')
parser.add_argument('-p', '--prefix', metavar="PREFIX", action='store', dest='prefix', default='diff_table', help='Prefix for output table')
parser.add_argument('-d', '--dir', action='store', dest='output_dir', type=str, default='', help='Directory where XML table will be written, default is current directory')
parser.add_argument('table_type', metavar='TYPE', type=str, choices=['A', 'a', 'B', 'b', 'D', 'd', 'F', 'f'], help='Table type to compare')
parser.add_argument('a_version', metavar='VERSION', type=int, help='Version number to compare against')
parser.add_argument('b_version', metavar='VERSION', type=int, help='Version number to compare, differences will be output')

args = parser.parse_args()

versions = [args.a_version, args.b_version]

a_table_name = '_'.join(['table'] + [str(prop) for prop in [args.table_type.upper(), args.master_table, args.originating_center, max(versions)] if prop is not None]) + '.xml'
b_table_name = '_'.join(['table'] + [str(prop) for prop in [args.table_type.upper(), args.master_table, args.originating_center, min(versions)] if prop is not None]) + '.xml'
diff_table_name = '_'.join([args.prefix] + [str(prop) for prop in [args.table_type.upper(), args.master_table, args.originating_center, min(versions)] if prop is not None]) + '.xml'

base_table = read_xml(join(args.output_dir, a_table_name))
other_table = read_xml(join(args.output_dir, b_table_name))

write_xml(base_table.diff(other_table), join(args.output_dir, diff_table_name))
