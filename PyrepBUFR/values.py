from .utility import byte_integer, ceil, get_min_type

class BUFRValue(object):
    __slots__ = ('element', '__bytes__')
    @classmethod
    def create(cls, element):
        return cls(element, None)
    @classmethod
    def create_from_table(cls, table, f, x, y):
        element = table.find(lambda id: id == (f, x, y))
        if element.is_empty:
            raise IndexError('Element f={0}, x={1}, y={2} not found in table.'.format(f,x,y))
        return cls(element.iloc(0), None)
    def __init__(self, element, byte_string):
        self.element = element
        if byte_string is None:
            self.set_missing()
        else:
            self.__bytes__ = byte_string
    @property
    def f(self):
        return self.element.f
    @property
    def x(self):
        return self.element.x
    @property
    def y(self):
        return self.element.y
    @property
    def mnemonic(self):
        return self.element.mnemonic
    @property
    def is_missing(self):
        return sum([2**i for i in range(self.element.bit_width)]).to_bytes(len(self.__bytes__), 'big') == self.__bytes__
    def set_missing(self):
        self.__bytes__ = sum([2**i for i in range(self.element.bit_width)]).to_bytes( int(ceil(self.element.bit_width / 8)) , 'big')
    def __repr__(self):
        return '{0:s} {1:s} {2}'.format(self.__class__.__name__, self.mnemonic, str(self.data))

class BUFRNumeric(BUFRValue):
    @property
    def data(self):
        return_value = None
        if not self.is_missing:
            if self.element.scale == 0:
                return_value = get_min_type(self.element.reference_value + int.from_bytes(self.__bytes__, 'big'))
            else:
                return_value = get_min_type((self.element.reference_value + int.from_bytes(self.__bytes__, 'big')) * 10.0**(-1 * self.element.scale))
        return return_value
    @data.setter
    def data(self, value):
        if self.element.scale == 0:
            value = value - self.element.reference_value
        else:
            value = int(round(value * 10.0**self.element.scale - self.element.reference_value))
        self.__bytes__ = byte_integer(get_min_type(value), self.element.bit_width)

class BUFRString(BUFRValue):
    @property
    def data(self):
        return_value = None
        if not self.is_missing:
            return_value = self.__bytes__.decode('utf-8').split('\x00')[0]
        return return_value
    @data.setter
    def data(self, value):
        value = value.encode('ascii') + b'\x00'
        value += ((self.element.bit_width // 8) - len(value)) * b' '
        self.__bytes__ = value

class BUFRLookupTable(BUFRValue):
    __slots__ = '__lookup_table__'
    def __init__(self, element, byte_string):
        super().__init__(element, byte_string)
        self.__lookup_table__ = None
    def set_lookup_table(self, codes):
        self.__lookup_table__ = dict([(x.code, x.meaning) for x in codes.values()])
    @property
    def data_raw(self):
        return_value = None
        if not self.is_missing:
            return_value = get_min_type(self.element.reference_value + int.from_bytes(self.__bytes__, 'big'))
        return return_value
    @data_raw.setter
    def data_raw(self, value):
        self.__bytes__ = byte_integer(get_min_type(value - self.element.reference_value), self.element.bit_width)

class BUFRCodeTable(BUFRLookupTable):
    @property
    def data(self):
        meaning = None
        if not self.is_missing and self.__lookup_table__ is not None:
            meaning = self.__lookup_table__.get(self.data_raw, meaning)
        return meaning
    @data.setter
    def data(self, value):
        code_switch = dict([(v, k) for k, v in self.__lookup_table__.items()])
        self.data_raw = code_switch.get(value, sum([2**i for i in range(self.element.bit_width)]))


class BUFRFlagTable(BUFRLookupTable):
    def set_lookup_table(self, codes):
        self.__lookup_table__ = dict([(1 << (self.element.bit_width - x.code), x.meaning) for x in codes.values()])
    @property
    def data(self):
        return_value = None
        if not self.is_missing and self.__lookup_table__ is not None:
            value = self.data_raw
            return_value = [v for k, v in self.__lookup_table__.items() if k & value > 0]
        return return_value
    @data.setter
    def data(self, value):
        flag_switch = dict([(v, k) for k, v in self.__lookup_table__.items()])
        self.data_raw = sum([flag_switch.get(x, 0) for x in value])

class BUFRList(list):
    pass