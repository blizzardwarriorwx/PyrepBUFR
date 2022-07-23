from sys import argv

from PyrepBUFR.tables import write_xml
from PyrepBUFR.utility import convert_wmo_table

write_xml(convert_wmo_table(argv[1]), argv[2])