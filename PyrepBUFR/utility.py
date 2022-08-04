from re import split
from xml.etree import ElementTree as ET

from PyrepBUFR.tables import BUFRDataType, CodeFlagDefinition, CodeFlagElement, ElementDefinition, SequenceDefinition, SequenceElement, Table

try:
    from numpy import frombuffer, log, ceil, uint8, arange
    def read_integer(byte_string, big_endian=True, unsigned=True):
        padding = b''
        byte_length = len(byte_string)
        byte_width = uint8(2 ** ceil(log(byte_length) / log(2)))
        if byte_length < byte_width:
            padding += 'b\x00' * (byte_width - byte_length)
        return frombuffer((padding + byte_string) if big_endian else (byte_string + padding),
                          ('>' if big_endian else '<') + ('u' if unsigned else 'i') + str(byte_width))[0]
    def read_integers(byte_string, byte_width, big_endian=True, unsigned=True):
        padding = b''
        byte_length = len(byte_string)
        for i in arange(byte_length % byte_width):
            padding += b'\x00'
        return frombuffer(byte_string + padding,
                          ('>' if big_endian else '<') + ('u' if unsigned else 'i') + str(byte_width))
except ModuleNotFoundError:
    def read_integer(byte_string, big_endian=True, unsigned=True):
        return int.from_bytes(byte_string, 'big' if big_endian else 'little', signed=(not unsigned))
    def read_integers(byte_string, byte_width, big_endian=True, unsigned=True):
        return [int.from_bytes(byte_string[i:i+byte_width], 'big' if big_endian else 'little', signed=(not unsigned)) for i in range(0, len(byte_string), byte_width)]

wmo_field_names = {
    'CodeFigure': 'code',
    'Meaning_en': 'meaning'
}

def convert_wmo_table(filename):
    tree = ET.parse(filename)
    root = tree.getroot()
    table = Table('A', 0, None, 0)
    for child in root:
        if child.tag == 'BUFR_TableA_en':
            kwargs = dict([(wmo_field_names[field.tag], field.text) for field in child if field.tag in wmo_field_names])
            if kwargs['code'].find('-') < 0:
                kwargs['code'] = [int(kwargs['code']), int(kwargs['code'])]
            else:
                kwargs['code'] = [int(x) for x in kwargs['code'].split('-')]
            for i in range(int(kwargs['code'][0]), int(kwargs['code'][1])+1):
                kwargs['code'] = i
                table.append(BUFRDataType(
                    kwargs['code'],
                    kwargs['meaning']
                ))
    return table

def convert_ncep_table(filename):
    table = None
    with open(filename, 'r') as in_file:
        line = in_file.readline().strip()
        while line != 'END':
            if line != '' and line[0] != '#':
                if len(line) > 5 and line[0:5] == 'Table':
                    table_type = line[6]
                    id = [int(x) for x in  split(r'\s*\|\s*', line[line.find('|')+1:].strip())]
                    if len(id) == 2:
                        master_table, table_version = id
                        originating_center = None
                    elif len(id) == 3:
                        master_table, originating_center, table_version = id
                    table = Table(table_type=table_type, master_table=master_table, originating_center=originating_center, table_version=table_version)
                    if table.table_type == 'B':
                        convert_ncep_B_table(in_file, table)
                        break
                    elif table.table_type == 'D':
                        convert_ncep_D_table(in_file, table)
                        break
                    elif table.table_type == 'F':
                        convert_ncep_F_table(in_file, table)
                        break
            line = in_file.readline().strip()
    return table

def convert_ncep_B_table(in_file, table):
    line = in_file.readline().strip()
    while line != 'END':
        if line != '' and line[0] != '#':
            parts = split(r'\s*\|\s*', line.strip())
            parts[0] = [int(x) for x in parts[0].strip().split('-')]
            parts[1:4] = [int(x.strip()) for x in parts[1:4]]
            parts[4] = parts[4].strip()
            parts[5] = [x.strip() for x in parts[5].strip().split(';')]
            table.append(ElementDefinition(
                parts[0][0],
                parts[0][1],
                parts[0][2],
                parts[1],
                parts[2],
                parts[3],
                parts[4],
                parts[5][0],
                parts[5][1],
                parts[5][2]
            ))
        line = in_file.readline().strip()

def convert_ncep_D_table(in_file, table):
    index = 0
    line = in_file.readline().strip()
    sequence = None
    while line != 'END':
        if line != '' and line[0] != '#':
            parts = split(r'\s*\|\s*', line.strip())
            if len(parts[0]) > 0:
                parts[0] = [int(x) for x in parts[0].strip().split('-')]
                parts[1] = [x.strip() for x in parts[1].strip().split(';')]
                if sequence is not None:
                    table.append(sequence)
                sequence = SequenceDefinition(
                    parts[0][0],
                    parts[0][1],
                    parts[0][2],
                    parts[1][0],
                    parts[1][1],
                    parts[1][2],
                )
                index = 0
            else:
                parts[1] = [int(x) for x in parts[1].replace('>','').strip().split('-')]
                index += 1
                sequence.append(SequenceElement(index,
                                                parts[1][0],
                                                parts[1][1],
                                                parts[1][2],
                                                parts[2].strip()))
        line = in_file.readline().strip()
    table.append(sequence)

def convert_ncep_F_table(in_file, table):
    line = in_file.readline().strip()
    f = x = y =  None
    conditional_ids = []
    conditional_values = []
    codes = []
    while line != 'END':
        if line != '' and line[0] != '#':
            parts = split(r'\s*\|\s*', line.strip())
            if len(parts[0]) > 0:
                f, x, y = [int(x) for x in parts[0].strip().split('-')]
                mnemonic, code_type = [x.strip() for x in parts[1].strip().split(';')]
            elif len(parts) == 2:
                if len(codes) > 0:
                    if len(conditional_ids) > 0:
                        for conditional_id in conditional_ids:
                            for conditional_value in conditional_values:
                                table.append(CodeFlagDefinition(
                                    f, x, y, code_type == 'FLAG',
                                    conditional_id[0], conditional_id[1], conditional_id[2],
                                    conditional_value, mnemonic, codes
                                ))
                    else:
                        table.append(CodeFlagDefinition(
                            f, x, y, code_type == 'FLAG',
                            None, None, None,
                            None, mnemonic, codes
                        ))
                conditional_ids = []
                conditional_values = []
                codes = []
                parts = parts[1].split('=')
                conditional_ids = [[int(x) for x in y.strip().split('-')] for y in parts[0].split(',')]
                conditional_values = [int(x) for x in parts[1].split(',')]
            else:                    
                code = CodeFlagElement(parts[1].replace('>', '').strip(), parts[2].strip())
                codes.append((code.id, code))
                if parts[1].find('>') == -1:
                    if len(codes) > 0:
                        if len(conditional_ids) > 0:
                            for conditional_id in conditional_ids:
                                for conditional_value in conditional_values:
                                    table.append(CodeFlagDefinition(
                                        f, x, y, code_type == 'FLAG',
                                        conditional_id[0], conditional_id[1], conditional_id[2],
                                        conditional_value, mnemonic, codes
                                    ))
                        else:
                            table.append(CodeFlagDefinition(
                                f, x, y, code_type == 'FLAG',
                                None, None, None,
                                None, mnemonic, codes
                            ))
                    conditional_ids = []
                    conditional_values = []
                    codes = []
        line = in_file.readline().strip()