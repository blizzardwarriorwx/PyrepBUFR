
from numpy import array, ceil, frombuffer, uint8, uint16, zeros
from textwrap import wrap

from .tables import ElementDefinition, read_xml, Table
from .values import BUFRList

DEBUG_LEVEL = 0

class InvalidBUFRMessage(Exception):
    pass

class ClosedBUFRFile(Exception):
    pass

class BitMap(object):
    def __init__(self, byte_array):
        self.__byte_array__ = byte_array
        self.cursor = 0
    def seek(self, position):
        self.cursor = position
    def __repr__(self):
        return ''.join(['%02x ' % b for b in self.__byte_array__])
    def read(self, length):
        start_byte = self.cursor // 8
        
        byte_length = ((length + self.cursor) + 8 - ((length + self.cursor) % 8)) // 8
        value_length = ceil(length / 8.0)

        bit_mask = sum([2**i for i in range(8 * (byte_length + start_byte) - self.cursor - 1, 8 * (byte_length + start_byte) - (self.cursor + length) - 1, -1)])

        value = ((int.from_bytes(self.__byte_array__[start_byte:start_byte+byte_length], 'big') & bit_mask) >> (byte_length * 8 - length - (self.cursor - start_byte * 8))).to_bytes(value_length, 'big')
        self.cursor += length
        return value
    @staticmethod
    def is_missing_value(value, length):
        return sum([2**i for i in range(length)]).to_bytes(len(value), 'big') == value

class DelayedReplication(ElementDefinition):
    def __init__(self, f=None, x=None, y=None, replication_element=None, data_elements=None):
        self.f = f
        self.x = x
        self.y = y
        self.mnemonic = "DREPL"
        self.name = 'Delayed Replication'
        self.replication_element = replication_element
        self.data_elements = data_elements
    def __repr__(self):
        return super().__repr__()[:-1] + ', \n    replication_element={0}, \n    data_elements=[\n        {1}\n    ]\n)'.format(self.replication_element, ',\n        '.join([str(x) for x in self.data_elements]))
    def __str__(self):
        return super().__str__() + '\n ' + '\n '.join([str(x) for x in self.data_elements])
    def read_value(self, bit_map):
        n = self.replication_element.read_value(bit_map)
        output = BUFRList()
        for i in range(n.data):
            values = []
            for element in self.data_elements:
                values.append(element.read_value(bit_map))
            output.append(values)
        return output

class Replication(ElementDefinition):
    def __init__(self, f=None, x=None, y=None, data_elements=None):
        self.f = f
        self.x = x
        self.y = y
        self.mnemonic = "REPL"
        self.name = 'Replication'
        self.data_elements = data_elements
    def __repr__(self):
        return super().__repr__()[:-1] + ', \n    replication_count={0}, \n    data_elements=[\n        {1}\n    ]\n)'.format(self.x, ',\n        '.join([str(x) for x in self.data_elements]))
    def __str__(self):
        return super().__str__() + '\n ' + '\n '.join([str(x) for x in self.data_elements])
    def read_value(self, bit_map):
        output = BUFRList()
        for i in range(self.x):
            values = []
            for element in self.data_elements:
                values.append(element.read_value(bit_map))
            output.append(values)
        return output

class BUFRFile(object):
    def __init__(self, filename, table_source=None):
        if table_source is None:
            self.__table_source__ = read_xml('tables.xml')
        else:
            self.__table_source__ = table_source
        if type(filename) == str:
            self.__fobj__ = open(filename, 'rb')
        else:
            self.__fobj__ = filename
        self.messages = []
        message_offset = 0
        continue_reading = True
        while continue_reading:
            try:
                message = BUFRMessage(self.__fobj__, table_source=self.__table_source__, file_offset=message_offset)
                message_offset = message.__section_start__[0] + message.__section_start__[6]
                self.messages.append(message)
            except InvalidBUFRMessage:
                continue_reading = False
        if len(self.messages) == 0:
            raise InvalidBUFRMessage('File contains no valid BUFR messages')
    def __str__(self):
        output = ''
        for i, message in enumerate(self.messages):
            output +=  '\n\n' + '*'* 50 + '\n*' + ' ' * 48 + '*\n*' + '{0: ^48s}'.format('Message {0:d}'.format(i)) + '*\n*' + ' ' * 48 + '*\n' + '*' * 50 + '\n\n' + str(message)
        return output
    def close(self):
        self.__fobj__.close()

class BUFRMessage(object):
    def __init__(self, filename, table_source=None, file_offset=0):
        if table_source is None:
            self.__table_source__ = read_xml('tables.xml')
        else:
            self.__table_source__ = table_source
        if type(filename) == str:
            self.__fobj__ = open(filename, 'rb')
        else:
            self.__fobj__ = filename
        self.__table_a__ = Table(table_type='A')
        self.__table_b__ = Table(table_type='B')
        self.__table_d__ = Table(table_type='D')
        self.__table_f__ = Table(table_version='F')
        self.__section_start__ = zeros(7, dtype='uint32')
        self.__section_start__[0] = file_offset
        start_word = self.__fobj__.read(4)
        file_start = start_word == b'BUFR'
        while not file_start:
            if DEBUG_LEVEL > 1:
                print('Seeking ahead')
            self.__section_start__[0] += 1
            self.__fobj__.seek(self.__section_start__[0])
            start_word = self.__fobj__.read(4)
            file_start = start_word == b'BUFR'
            if start_word == b'':
                break

        if not file_start:
            raise InvalidBUFRMessage('File contains no valid BUFR messages')
    
        if DEBUG_LEVEL > 0:
            print('Found start')
        self.__fobj__.seek(self.__section_start__[0] + 4)
        self.__section_start__[6] = frombuffer(b'\x00' + self.__fobj__.read(3), '>u4')[0]
        self.bufr_edition = frombuffer(self.__fobj__.read(1), '>u1')[0]
        self.__section_start__[1] = self.__section_start__[0] + 8
        self.__fobj__.seek(self.__section_start__[1] + (9 if self.bufr_edition == 4 else 7))
        self.section_2_present = frombuffer(self.__fobj__.read(1), '>?')[0]
        if self.section_2_present:
            self.__fobj__.seek(self.__section_start__[1])
            self.__section_start__[2] = self.__section_start__[1] + frombuffer(b'\x00' + self.__fobj__.read(3), '>u4')[0]
            self.__fobj__.seek(self.__section_start__[2])
            self.__section_start__[3] = self.__section_start__[2] + frombuffer(b'\x00' + self.__fobj__.read(3), '>u4')[0]
        else:
            self.__fobj__.seek(self.__section_start__[1])
            self.__section_start__[3] = self.__section_start__[1] + frombuffer(b'\x00' + self.__fobj__.read(3), '>u4')[0]
        self.__fobj__.seek(self.__section_start__[3])
        self.__section_start__[4] = self.__section_start__[3] + frombuffer(b'\x00' + self.__fobj__.read(3), '>u4')[0]
        self.__fobj__.seek(self.__section_start__[4])
        self.__section_start__[5] = self.__section_start__[4] + frombuffer(b'\x00' + self.__fobj__.read(3), '>u4')[0]
        if DEBUG_LEVEL > 0:
           print('Initializing Table A')
        for table in (self.__table_source__.find(table_type='A', originating_center=None)
                    + self.__table_source__.find(table_type='A', originating_center=self.originating_center)
                    + self.__table_source__.find(table_type='AX')):
            self.__table_a__.append(table)
        if DEBUG_LEVEL > 0:
           print('Initializing Table B')
        for table in (self.__table_source__.find(table_type='B', master_table=self.bufr_master_table, originating_center=None, table_version=self.master_table_version)
                    + self.__table_source__.find(table_type='B', master_table=self.bufr_master_table, originating_center=self.originating_center, table_version=self.local_table_version)
                    + self.__table_source__.find(table_type='BX')):
            self.__table_b__.append(table)
        if DEBUG_LEVEL > 0:
           print('Initializing Table D')
        for table in (self.__table_source__.find(table_type='D', master_table=self.bufr_master_table, originating_center=None, table_version=self.master_table_version)
                    + self.__table_source__.find(table_type='D', master_table=self.bufr_master_table, originating_center=self.originating_center, table_version=self.local_table_version)
                    + self.__table_source__.find(table_type='DX')):
            self.__table_d__.append(table)
        if DEBUG_LEVEL > 0:
           print('Initializing Table F')
        for table in (self.__table_source__.find(table_type='F', master_table=self.bufr_master_table, originating_center=None, table_version=self.master_table_version)
                    + self.__table_source__.find(table_type='F', master_table=self.bufr_master_table, originating_center=self.originating_center, table_version=self.local_table_version)
                    + self.__table_source__.find(table_type='FX')):
            self.__table_f__.append(table)
        
    def close(self):
        self.__fobj__.close()

    # Section 1 - Identification
    
    @property
    def bufr_master_table(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        self.__fobj__.seek(self.__section_start__[1] + 3)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def originating_center(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        if self.bufr_edition == 3:
            offset = 5
        elif self.bufr_edition == 4:
            offset = 4
        self.__fobj__.seek(self.__section_start__[1] + offset)
        value = 0
        if self.bufr_edition == 3:
            value = frombuffer(self.__fobj__.read(1), '>u1')[0]
        elif self.bufr_edition == 4:
            value = frombuffer(self.__fobj__.read(2), '>u2')[0]
        return value
    @property
    def originating_subcenter(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        if self.bufr_edition == 3:
            offset = 4
        elif self.bufr_edition == 4:
            offset = 6
        self.__fobj__.seek(self.__section_start__[1] + offset)
        value = 0
        if self.bufr_edition == 3:
            value = frombuffer(self.__fobj__.read(1), '>u1')[0]
        elif self.bufr_edition == 4:
            value = frombuffer(self.__fobj__.read(2), '>u2')[0]
        return value
    @property
    def update_sequence_number(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 6
        if self.bufr_edition == 4:
            offset += 2
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(2), '>u2')[0]
    @property
    def data_category(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 8
        if self.bufr_edition == 4:
            offset += 2
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def data_category_description(self):
        description = ''
        codes = self.__table_a__.find(code=self.data_category)
        if codes is not None:
            description = codes[0].meaning
        return description
    @property
    def international_data_sub_category(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        value = None
        if self.bufr_edition == 4:
            offset = 11
            self.__fobj__.seek(self.__section_start__[1] + offset)
            value = frombuffer(self.__fobj__.read(1), '>u1')[0]
        return value
    @property
    def local_sub_category(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        if self.bufr_edition == 3:
            offset = 9
        if self.bufr_edition == 4:
            offset = 12
        
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def master_table_version(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 10
        if self.bufr_edition == 4:
            offset += 3
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def local_table_version(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 11
        if self.bufr_edition == 4:
            offset += 3
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def file_date(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 12
        if self.bufr_edition == 4:
            offset += 3
        self.__fobj__.seek(self.__section_start__[1] + offset)
        if self.bufr_edition == 3:
            year = frombuffer(self.__fobj__.read(1), '>u1')[0]
            month, day, hour, minute = frombuffer(self.__fobj__.read(4), '>u1')
            second = 0
        elif self.bufr_edition == 4:
            year = frombuffer(self.__fobj__.read(2), '>u2')[0]
            month, day, hour, minute, second = frombuffer(self.__fobj__.read(5), '>u1')
        return (year, month, day, hour, minute, second)
    
    @property
    def file_year(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 12
        if self.bufr_edition == 4:
            offset += 3
        self.__fobj__.seek(self.__section_start__[1] + offset)
        year = uint16(0)
        if self.bufr_edition == 3:
            year = frombuffer(self.__fobj__.read(1), '>u1')[0]
        elif self.bufr_edition == 4:
            year = frombuffer(self.__fobj__.read(2), '>u2')[0]
        return year
    @property
    def file_month(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 13
        if self.bufr_edition == 4:
            offset += 4
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def file_day(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 14
        if self.bufr_edition == 4:
            offset += 4
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def file_hour(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 15
        if self.bufr_edition == 4:
            offset += 4
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def file_minute(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 16
        if self.bufr_edition == 4:
            offset += 4
        self.__fobj__.seek(self.__section_start__[1] + offset)
        return frombuffer(self.__fobj__.read(1), '>u1')[0]
    @property
    def file_second(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        value = uint8(0)
        if self.bufr_edition == 4:
            offset = 21
            self.__fobj__.seek(self.__section_start__[1] + offset)
            value = frombuffer(self.__fobj__.read(1), '>u1')[0]
        return value
    @property
    def section_1_local_data(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        offset = 17
        if self.bufr_edition == 4:
            offset += 5
        self.__fobj__.seek(self.__section_start__[1] + offset)
        if self.section_2_present:
            end = self.__section_start__[2] - self.__section_start__[1] - offset
        else:
            end = self.__section_start__[3] - self.__section_start__[1] - offset
        return self.__fobj__.read(end)

    # Section 2 - Optional

    @property
    def section_2_local_data(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        data = b''
        if self.section_2_present:
            self.__fobj__.seek(self.__section_start__[2])
            end = self.__section_start__[3] - self.__section_start__[2] - 4
            data = self.__fobj__.read(end)
        return data
    
    # Section 3 - Data Description

    @property
    def number_of_subsets(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        self.__fobj__.seek(self.__section_start__[3] + 4)
        return frombuffer(self.__fobj__.read(2), '>u2')[0]
    @property
    def observed_data(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        self.__fobj__.seek(self.__section_start__[3] + 6)
        return frombuffer(self.__fobj__.read(1), '>u1')[0] & 128 > 0
    @property
    def compressed(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        self.__fobj__.seek(self.__section_start__[3] + 6)
        return frombuffer(self.__fobj__.read(1), '>u1')[0] & 64 > 0
    @property
    def data_descriptors(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        self.__fobj__.seek(self.__section_start__[3] + 7)
        end = self.__section_start__[4] - self.__section_start__[3] - 7
        end -= end % 2
        data = frombuffer(self.__fobj__.read(end), '>u2')
        return array([array([(x & ( 3 << 14)) >> 14,
                             (x & (63 <<  8)) >>  8,
                             (x &        255)      ]) for x in data])
    @property
    def section_4_data_bytes(self):
        if self.__fobj__.closed:
            raise ClosedBUFRFile('File already closed.')
        self.__fobj__.seek(self.__section_start__[4] + 4)
        end = self.__section_start__[5] - self.__section_start__[4] - 4
        return BitMap(self.__fobj__.read(end))

    def expand_descriptors(self, file_descriptors):
        expanded_descriptors = []
        number_of_descriptors = len(file_descriptors)
        i = 0
        
        while i < number_of_descriptors:
            file_descriptor = file_descriptors[i]
            if file_descriptor[0] == 0:
                element = self.__table_b__.find(f=file_descriptor[0], x=file_descriptor[1], y=file_descriptor[2])
                if not element.is_empty:
                    element = element[0]
                    element.set_table_f(self.__table_f__)
                    expanded_descriptors.append(element)
            elif file_descriptor[0] == 1:
                if file_descriptor[2] == 0:
                    expanded_descriptors.append(DelayedReplication(f=file_descriptor[0], x=file_descriptor[1], y=file_descriptor[2], replication_element=self.expand_descriptors(file_descriptors[i+1:i+2])[0], data_elements=self.expand_descriptors(file_descriptors[i+2:i+file_descriptor[1]+2])))
                    i += file_descriptor[1]+1
                else:
                    expanded_descriptors.append(Replication(f=file_descriptor[0], x=file_descriptor[1], y=file_descriptor[2], data_elements=self.expand_descriptors(file_descriptors[i+1:i+file_descriptor[1]+1])))
                    i += file_descriptor[1]
            elif file_descriptor[0] == 2:
                print('Operator Found')
            elif file_descriptor[0] == 3:
                sequence = self.__table_d__.find(f=file_descriptor[0], x=file_descriptor[1], y=file_descriptor[2])
                if not sequence.is_empty:
                    sequence = self.expand_descriptors(sequence[0].get_descriptors())
                    expanded_descriptors.extend(sequence)
            i += 1
        return expanded_descriptors

    def __str__(self):
        description = [
            {
                'Length of Section 0 (bytes)': lambda : '8',
                'Total Length of BUFR Message (bytes)': lambda : str(self.__section_start__[6]),
                'BUFR Edition Number': 'bufr_edition'
            },
            {
                'Length of Section 1 (bytes)': lambda : str(self.__section_start__[2] - self.__section_start__[1] if self.section_2_present else self.__section_start__[3] - self.__section_start__[1]),
                'BUFR Master Table': 'bufr_master_table',
                'Originating Center': 'originating_center',
                'Originating Sub-Center': 'originating_subcenter',
                'Update Sequence Number': 'update_sequence_number',
                'Flag (Presence of Section 2)': 'section_2_present',
                'BUFR Data Category': 'data_category',
                'International Sub-Category': 'international_data_sub_category',
                'Local Sub-Category': 'local_sub_category',
                'Version Number of Master Table': 'master_table_version',
                'Version Number of Local Table': 'local_table_version',
                'Year': 'file_year',
                'Month': 'file_month',
                'Day': 'file_day',
                'Hour': 'file_hour',
                'Minute': 'file_minute',
                'Second': 'file_second',
                '': lambda :  '' if len(self.section_1_local_data) == 0 else '\n{0:=^50s}\n'.format(' Begin Additional Section 1 Data ') + '\n'.join(wrap(''.join(['%02x ' % b for b in self.section_1_local_data]), width=48)) + '\n' + '{0:=^50s}\n'.format(' End Additional Section 1 Data ')
            },
            {
                'Length of Section 1 (bytes)': lambda : str(self.__section_start__[3] - self.__section_start__[2] if self.section_2_present else 0),
                '': lambda :  '' if not self.section_2_present else '\n{0:=^50s}\n'.format(' Begin Optional Section 2 Data ') + '\n'.join(wrap(''.join(['%02x ' % b for b in self.section_2_local_data]), width=48)) + '\n' + '{0:=^50s}\n'.format(' End Optional Section 2 Data ')
            },
            {
                'Length of Section 3 (bytes)': lambda : str(self.__section_start__[4] - self.__section_start__[3]),
                'Number of Data Subsets': 'number_of_subsets',
                'Flag (Observed Data)': 'observed_data',
                'Flag (Compressed Data)': 'compressed',
                'Data Descriptors': lambda : '\n\n' + '\n'.join(wrap('  '.join(['{0:01d}-{1:02d}-{2:03d}'.format(*x) for x in self.data_descriptors]), width=50)) + '\n',
                'Expanded Descriptors': lambda : '\n\n' + '\n'.join(wrap(', '.join([y.strip() for y in ('\n'.join([str(x) for x in self.expand_descriptors(self.data_descriptors)]).split('\n') )]), width=50))
            },
            {
                'Length of Section 4 (bytes)': lambda : str(self.__section_start__[5] - self.__section_start__[4]),
                '': lambda :  '\n{0:=^50s}\n'.format(' Begin Section 4 Data ') + '\n'.join(wrap(str(self.section_4_data_bytes), width=48)) + '\n' + '{0:=^50s}\n'.format(' End Section 4 Data ')
            }
        ]



        output = ''

        for section in range(len(description)):
            output += '\n{0:^50s}\n\n'.format('BUFR Section {0:1d}'.format(section))
            for label, property_name in description[section].items():
                if type(property_name) == str:
                    output += '{0:<36s}{1:>14s}\n'.format(label, str(getattr(self, property_name)))
                else:
                    output += '{0:<36s}{1:>14s}\n'.format(label, property_name())
        return output