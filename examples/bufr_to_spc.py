# Example script to print out an observed sounding in WMO BUFR standard format 309052 in SPC text fromat

from sys import argv

from PyrepBUFR import BUFRFile
from PyrepBUFR.tables import read_xml
from PyrepBUFR.tables.default import default_table
from PyrepBUFR.values import units
from PyrepBUFR.utility import transpose

tables = None
if len(argv) > 2:
    tables = read_xml(argv[2])
else:
    tables = default_table

bf = BUFRFile(argv[1], table_source=tables)

header_template = """%TITLE%
 {fileName:s}   {year:02d}{month:02d}{day:02d}/{hour:02d}{minute:02d} 

   LEVEL       HGHT       TEMP       DWPT       WDIR       WSPD
-------------------------------------------------------------------
%RAW%"""

data = transpose(bf.data.to_dict(filter_keys=['SMID', 'YEAR', 'MNTH', 'DAYS', 'HOUR', 'MINU', 'SECO', 'CLATH', 'CLONH', 'LTDS', 'PRLC', 'GPH10', 'LATDH', 'LONDH', 'TMDB', 'TMDP', 'WDIR', 'WSPD'],
                    convert_units={'PRLC': units('hPa'), 'TMDB': units('degC'), 'TMDP': units('degC'), 'WSPD': units('knots')}))

print(header_template.format(fileName=argv[1], year=data['YEAR'][0] % 100, month=data['MNTH'][0], day=data['DAYS'][0], hour=data['HOUR'][0], minute=data['MINU'][0]))#.format(fileName=argv[1], year=data['YEAR'][0]%100, month=data["MNTH"][0], day=data['DAYS'][0], hour=data["HOUR"], munute=data['MINU'][0]))

for i in range(len(data['TMDB'])):
    print('{level:8.2f},{height:10.2f},{temperature:10.2f},{dew_point:10.2f},{wind_direction:10.2f},{wind_speed:10.2f}'.format(level=data['PRLC'][i] if data['PRLC'][i] is not None else -9999.0,
                                                                                                                               height=data['GPH10'][i] if data['GPH10'][i] is not None else -9999.0,
                                                                                                                               temperature=data['TMDB'][i] if data['TMDB'][i] is not None else -9999.0,
                                                                                                                               dew_point=data['TMDP'][i] if data['TMDP'][i] is not None else -9999.0,
                                                                                                                               wind_direction=data['WDIR'][i] if data['WDIR'][i] is not None else -9999.0,
                                                                                                                               wind_speed=data['WSPD'][i] if data['WSPD'][i] is not None else -9999.0))
print("%END%")
bf.close()