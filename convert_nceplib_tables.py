from sys import argv

from PyrepBUFR.tables import write_xml
from PyrepBUFR.utility import convert_ncep_table

write_xml(convert_ncep_table(argv[1]), argv[2])