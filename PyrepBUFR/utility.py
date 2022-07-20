

from re import split, compile, search
from xml.etree import cElementTree as ET

from PyrepBUFR.tables import CodeEntry, CodeTableDefinition, ElementDefinition, FlagTableDefinition, SequenceDefinition, SequenceElement, Table

wmo_field_names = {
    'CodeFigure': 'code',
    'Meaning_en': 'meaning'
}

def convert_wmo_defnition_table(filename):
    tree = ET.parse(filename)
    root = tree.getroot()
    table = Table(table_type='A')
    for child in root:
        if child.tag == 'BUFR_TableA_en':
            kwargs = dict([(wmo_field_names[field.tag], field.text) for field in child if field.tag in wmo_field_names])
            if kwargs['code'].find('-') < 0:
                kwargs['code'] = [int(kwargs['code']), int(kwargs['code'])]
            else:
                kwargs['code'] = [int(x) for x in kwargs['code'].split('-')]
            for i in range(int(kwargs['code'][0]), int(kwargs['code'][1])+1):
                kwargs['code'] = i
                table.append(CodeEntry(**kwargs))
    return table

def convert_ncep_definition_table(filename):
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
                elif table is not None:
                    if table.table_type == 'D':
                        parts = split(r'\s*\|\s*', line.strip())
                        if len(parts[0]) > 0:
                            parts[0] = [int(x) for x in parts[0].strip().split('-')]
                            parts[1] = [x.strip() for x in parts[1].strip().split(';')]
                            table.append(SequenceDefinition(
                                f=parts[0][0],
                                x=parts[0][1],
                                y=parts[0][2],
                                mnemonic=parts[1][0],
                                dcod=parts[1][1],
                                name=parts[1][2],
                            ))
                        else:
                            parts[1] = [int(x) for x in parts[1].replace('>','').strip().split('-')]
                            table.entries[-1].append(SequenceElement(f=parts[1][0],
                                                                     x=parts[1][1],
                                                                     y=parts[1][2],
                                                                     name=parts[2].strip()))
                    elif table.table_type == 'B':
                        parts = split(r'\s*\|\s*', line.strip())
                        parts[0] = [int(x) for x in parts[0].strip().split('-')]
                        parts[1:4] = [int(x.strip()) for x in parts[1:4]]
                        parts[4] = parts[4].strip()
                        parts[5] = [x.strip() for x in parts[5].strip().split(';')]
                        table.append(ElementDefinition(
                            f=parts[0][0],
                            x=parts[0][1],
                            y=parts[0][2],
                            scale=parts[1],
                            reference_value=parts[2],
                            bit_width=parts[3],
                            unit=parts[4],
                            mnemonic=parts[5][0],
                            desc_code=parts[5][1],
                            name=parts[5][2]
                        ))
            line = in_file.readline().strip()
    return table

def convert_ncep_code_table(filename):
    match_regex = compile(r'\s+\d-\d{2}-\d{3}\s+\|\s+\w+\s+;\s+(?:CODE|FLAG)\s+(?:\|\s+(?:\d-\d{2}-\d{3},?)+=\d+)?\s*(?:\|\s+\d+\s+>\s+\|\s+[^\n]+\s+)+(?:\|\s+\d+\s+\|\s+[^\n]+)')

    table = None
    chunk = ''
    condition_fields = [None]
    condition_values  = [None]
    codes = []
    with open(filename, 'r') as in_file:
        line = in_file.readline()
        while line.strip() != 'END':
            if line.strip() != '' and line[0] != '#':
                if len(line) > 5 and line[0:5] == 'Table':
                    table_type = line[6]
                    id = [int(x) for x in  split(r'\s*\|\s*', line[line.find('|')+1:].strip())]
                    if len(id) == 2:
                        master_table, table_version = id
                        originating_center = None
                    elif len(id) == 3:
                        master_table, originating_center, table_version = id
                    table = Table(table_type=table_type, master_table=master_table, originating_center=originating_center, table_version=table_version)
                elif table is not None:
                    chunk += line
                    match = search(match_regex, chunk)
                    if match is not None:
                        for table_line in chunk.strip().split('\n'):
                            parts = split(r'\s*\|\s*', table_line.strip())
                            if len(parts[0]) > 0:
                                f, x, y = [int(p) for p in parts[0].strip().split('-')]
                                mnemonic, flag_type = [p.strip() for p in parts[1].strip().split(';')]
                            elif search(r'^\d-\d\d-\d\d\d', parts[1].strip()) is not None:
                                if len(codes) > 0:
                                    for condition_field in condition_fields:
                                        for condition_value in condition_values:
                                            kwargs = dict(f=f, x=x, y=y, mnemonic=mnemonic)
                                            if condition_field is not None:
                                                kwargs['condition_f'] = condition_field[0]
                                                kwargs['condition_x'] = condition_field[1]
                                                kwargs['condition_y'] = condition_field[2]
                                                kwargs['condition_value'] = condition_value
                                            if flag_type == 'CODE':
                                                definition = CodeTableDefinition(**kwargs)
                                            elif flag_type == 'FLAG':
                                                definition = FlagTableDefinition(**kwargs)
                                            for code in codes:
                                                definition.append(CodeEntry(code=code[0], meaning=code[1]))
                                            table.append(definition)
                                            del definition
                                    codes = []
                                condition_fields, condition_values = parts[1].split('=')
                                condition_values = [int(cv.strip()) for cv in condition_values.split(',')] 
                                condition_fields = [[int(cf) for cf in cfs.split('-')] for cfs in condition_fields.split(',')]
                            else:
                                codes.append((int(parts[1].replace('>','').strip()), parts[2].strip()))
                                    
                        if len(codes) > 0:
                            for condition_field in condition_fields:
                                for condition_value in condition_values:
                                    kwargs = dict(f=f, x=x, y=y, mnemonic=mnemonic)
                                    if condition_field is not None:
                                        kwargs['condition_f'] = condition_field[0]
                                        kwargs['condition_x'] = condition_field[1]
                                        kwargs['condition_y'] = condition_field[2]
                                        kwargs['condition_value'] = condition_value
                                    if flag_type == 'CODE':
                                        definition = CodeTableDefinition(**kwargs)
                                    elif flag_type == 'FLAG':
                                        definition = FlagTableDefinition(**kwargs)
                                    for code in codes:
                                        definition.append(CodeEntry(code=code[0], meaning=code[1]))
                                    table.append(definition)
                                    del definition
                        condition_fields = [None]
                        condition_values  = [None]
                        codes = []
                        chunk = ''
            line = in_file.readline()
    return table