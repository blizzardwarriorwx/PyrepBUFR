from argparse import ArgumentParser

from PyrepBUFR.tables import read_xml
from PyrepBUFR.tables.default import default_table

parser = ArgumentParser(description='Look up BUFR element definition information')
parser.add_argument('-m', '--master-table', metavar='MASTERTABLE', action='store', dest='master_table', type=int, default=0, help='Master table ID to use')
parser.add_argument('-o', '--originating-center', metavar='CENTER', action='store', dest='originating_center', type=int, default=None, help='Originating center ID to use')
parser.add_argument('-t', '--tables', metavar="PATH", action='store', dest='tables', type=str, default=None, help='XML file path containing tables')
parser.add_argument('-v', '--table-version', metavar='VERSION', dest='table_version', type=int, default=None, help='BUFR Table version to search')
parser.add_argument('-n', '--mnemonic', dest='mode', action='store_const', const='mnemonic', default='mnemonic', help='Use mnemonic to search for element')
parser.add_argument('-i', '--FXY', dest='mode', action='store_const', const='fxy', default='mnemonic', help='Use FXY to search for element')
parser.add_argument('field', metavar='FIELD', type=str, help='Field mnemonic or FXY to lookup')

args = parser.parse_args()

if args.tables is None:
    tables = default_table
else:
    tables = read_xml(args.tables)

if args.table_version is None:
    args.table_version = max([id.table_version for id in tables.keys() if id.originating_center==args.originating_center and id.master_table==args.master_table])
b_table = tables.construct_table_version('B', args.table_version, master_table=args.master_table, originating_center=args.originating_center)
f_table = tables.construct_table_version('F', args.table_version, master_table=args.master_table, originating_center=args.originating_center)
element = None

if args.mode == 'mnemonic':
    for el in b_table.values():
        if el.mnemonic == args.field:
            element = el
            break
else:
    element = b_table.find(lambda id: id == tuple([int(x) for x in args.field.split('-')]))
    if len(element) == 0:
        element = None
    else:
        element = element.iloc(0)

if element is not None:
    print('')
    print(repr(element).replace('ElementDefinition(', '').replace(')', ''))
    if element.unit.lower() in ['flag table', 'code table']:
        entries = f_table.find(lambda id: id[0:3] == element.id)
        if len(entries) == 1:
            for entry in entries.iloc(0).values():
                print('    {0:5d} = {1:s}'.format(entry.code, entry.meaning))
        elif len(tables) > 1:
            for part in entries.values():
                print('    f={0:d}, x={1:d}, y={2:d}, value={3:d}'.format(part.condition_f, part.condition_x, part.condition_y, part.condition_value))
                for entry in part.values():
                    print('        {0:5d} = {1:s}'.format(entry.code, entry.meaning))
    print('')