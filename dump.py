from sys import argv

from PyrepBUFR import BUFRFile
from PyrepBUFR.tables import read_xml

tables = None
if len(argv) > 2:
    tables = read_xml(argv[2])

bf = BUFRFile(argv[1], table_source=tables)

print(str(bf))

bf.close()