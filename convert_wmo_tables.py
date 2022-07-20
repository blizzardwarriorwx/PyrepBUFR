from sys import argv

from PyrepBUFR.tables import write_xml
from PyrepBUFR.utility import convert_wmo_defnition_table

write_xml(convert_wmo_defnition_table(argv[1]), argv[2])