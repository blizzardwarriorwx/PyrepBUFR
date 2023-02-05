from sys import argv

from PyrepBUFR import BUFRFile
from PyrepBUFR.tables import read_xml
from PyrepBUFR.tables.default import default_table

tables = None
if len(argv) > 2:
    tables = read_xml(argv[2])
else:
    tables = default_table

bf = BUFRFile(argv[1], table_source=tables)

print(str(bf))

bf.close()