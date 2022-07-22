try:
    from numpy import min_scalar_type
    def get_min_type(value):
        return min_scalar_type(value).type(value)
except ModuleNotFoundError:
    def get_min_type(value):
        return value

class BUFRValue(object):
    def __init__(self, element, bytes):
        self.element = element
        self.__bytes__ = bytes
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
    def __repr__(self):
        return '{0:s} {1:s} {2}'.format(self.__class__.__name__, self.mnemonic, str(self.data))

class BUFRNumeric(BUFRValue):
    @property
    def data(self):
        if self.element.scale == 0:
            return_value = (self.element.reference_value + int.from_bytes(self.__bytes__, 'big'))
        else:
            return_value = (self.element.reference_value + int.from_bytes(self.__bytes__, 'big')) * 10.0**(-1 * self.element.scale)
        return get_min_type(return_value)

class BUFRString(BUFRValue):
    @property
    def data(self):
        return self.__bytes__.decode('utf-8').split('\x00')[0]

class BUFRCodeTable(BUFRValue):
    def __init__(self, element, bytes, table):
        super().__init__(element, bytes)
        self.__table__ = table
    def get_meaning(self):
        meaning = ''
        codes = self.__table__.find(f=self.f, x=self.x, y=self.y)
        if not codes.is_empty:
            if len(codes) == 1:
                codes = codes[0].find(code=self.data)
                if not codes.is_empty:
                    meaning = codes[0].meaning
        return meaning
    @property
    def data(self):
        return (self.element.reference_value + int.from_bytes(self.__bytes__, 'big')) * 10.0**(-1 * self.element.scale)

class BUFRFlagTable(BUFRValue):
    @property
    def data(self):
        return (self.element.reference_value + int.from_bytes(self.__bytes__, 'big')) * 10.0**(-1 * self.element.scale)

class BUFRMissingValue(BUFRValue):
    def __init__(self, element):
        super().__init__(element, None)
    @property
    def data(self):
        return None

class BUFRList(list):
    pass