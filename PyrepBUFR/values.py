class BUFRValue(object):
    def __init__(self, f, x, y, mnemonic, data):
        self.f = f
        self.x = x
        self.y = y
        self.mnemonic = mnemonic
        self.data = data
    def __repr__(self):
        return '{0:s} {1:s} {2}'.format(self.__class__.__name__, self.mnemonic, str(self.data))

class BUFRNumeric(BUFRValue):
    pass

class BUFRString(BUFRValue):
    pass

class BUFRCodeTable(BUFRValue):
    def __init__(self, f, x, y, mnemonic, data, table):
        super().__init__(f, x, y, mnemonic, data)
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
class BUFRFlagTable(BUFRValue):
    pass

class BUFRMissingValue(BUFRValue):
    def __init__(self, f, x, y, mnemonic):
        self.f = f
        self.x = x
        self.y = y
        self.mnemonic = mnemonic
        self.data = None

class BUFRList(list):
    pass