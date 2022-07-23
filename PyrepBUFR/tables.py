# from os.path import dirname, abspath
from xml.dom.minidom import parseString
from xml.etree import cElementTree as ET, ElementInclude
from sys import modules

from .values import *

try:
    from numpy import dtype
    def parse_int(value, byte_width, big_endian=True, unsigned=True):
        return dtype(('u' if unsigned else 'i') + str(byte_width)).type(value) if value is not None else None
except ModuleNotFoundError:
    def parse_int(value, byte_width, big_endian=True, unsigned=True):
        return int(value) if value is not None else None

def xml2class(node):
    cls = getattr(modules[__name__], node.tag)
    return cls.from_xml(cls, node)

def read_xml(filename):
    tree = ET.parse(filename)
    root = tree.getroot()
    ElementInclude.include(root)
    return xml2class(root)

def write_xml(xml_tree, filename):
    with open(filename, 'w') as out_file:
        out_file.write(xml_tree.to_string())

class BufrObjectIterator(object):
    def __init__(self, object):
        self.object = object
        self.current_index = -1
    def __iter__(self):
        return self
    def __next__(self):
        if hasattr(self.object, '__child_attribute__'):
            child_attribute = getattr(self.object, self.object.__child_attribute__)
            self.current_index += 1
            if self.current_index < len(child_attribute):
                return child_attribute[self.current_index]
            else:
                raise StopIteration
        else:
            raise StopIteration

class BufrTableObject(object):
    __replace_duplicate_child__ = False
    @staticmethod
    def from_xml(cls, node):
        kwargs = dict([(k.replace('-', '_'), v) for k, v in node.attrib.items()])
        cls = cls(**kwargs)
        for child in node:
            cls.append(xml2class(child))
        return cls
    def __init__(self):
        pass
    def __iter__(self):
        return BufrObjectIterator(self)
    def __has__(self, *args, **kwargs):
        if len(kwargs.keys()) == 0 and len(args) > 0:
            if hasattr(args[0], '__identity_attributes__'):
                kwargs = dict([(key, getattr(args[0], key)) for key in args[0].__identity_attributes__])
        child_subset = {}#dict([(k, set()) for k in kwargs])
        if hasattr(self, '__child_attribute__'):
            for i, child in enumerate(getattr(self, self.__child_attribute__)):
                for key in kwargs:
                    if hasattr(child, key):
                        if key not in child_subset:
                            child_subset[key] = set()
                        if getattr(child, key, None) == kwargs[key]:
                            child_subset[key].update([i])
        child_subset = child_subset.values()
        if len(child_subset) > 0:
            child_subset = set.intersection(*child_subset)
        return child_subset
    def index(self, *args, **kwargs):
        indicies = self.__has__(*args, **kwargs)
        if len(indicies) == 0:
            indicies.update([-1])
        return sorted(indicies)[0]
    def __contains__(self, item):
        has_item = False
        if hasattr(item, '__identity_attributes__'):
            kwargs = dict([(key, getattr(item, key)) for key in item.__identity_attributes__])
            has_item = len(self.__has__(**kwargs)) > 0
        return has_item
    def find(self, **kwargs):
        indicies = self.__has__(**kwargs)
        result_kwargs = {}
        if hasattr(self, '__identity_attributes__'):
            result_kwargs = dict([(key, getattr(self, key)) for key in self.__identity_attributes__])
        result = type(self)(**result_kwargs)
        if hasattr(self, '__child_attribute__') and len(indicies) > 0:
            child_attribute = getattr(self, self.__child_attribute__)
            setattr(result, result.__child_attribute__,[child_attribute[i] for i in indicies])
        return result
    def append(self, child):
        if child is not None and hasattr(self, '__child_attribute__'):
            if type(child) == type(self):
                child_attribute = getattr(child, self.__child_attribute__)
                for sub_child in child_attribute:
                    self.append(sub_child)
            else:
                child_attribute = getattr(self, self.__child_attribute__)
                child_exists = child in self
                if child_exists and self.__replace_duplicate_child__:
                    index = self.index(child)
                    child_attribute[index] = child
                elif not child_exists:
                    child_attribute.append(child)
                if hasattr(self, '__child_sort_key__'):
                    child_attribute.sort(key=self.__child_sort_key__)
    def to_xml(self):
        elm = ET.Element(self.__class__.__name__)
        for att in self.__dict__:
            att_value = getattr(self, att)
            if att_value is not None and type(att_value) != list:
                elm.set(att.replace('_', '-'), str(getattr(self, att)))
        return elm
    def to_string(self):
        return parseString(ET.tostring(self.to_xml())).toprettyxml()
    def __getitem__(self, item):
        if not hasattr(self, '__child_attribute__'):
            raise TypeError("'{0:s}' object is not subscriptable")
        else:
            child_attribute = getattr(self, self.__child_attribute__)
            return child_attribute[item]
    @property
    def is_empty(self):
        value = True
        if hasattr(self, '__child_attribute__'):
            value = len(getattr(self, self.__child_attribute__)) == 0
        return value
    def __add__(self, b):
        result_kwargs = {}
        if hasattr(self, '__identity_attributes__'):
            result_kwargs = dict([(key, getattr(self, key)) for key in self.__identity_attributes__])
        result = type(self)(**result_kwargs)
        result.append(self)
        result.append(b)
        return result

class TableCollection(BufrTableObject):
    __child_attribute__ = 'tables'
    __replace_duplicate_child__ = True
    @staticmethod
    def __child_sort_key__(a):
        return (a.table_type, 
                a.master_table if a.master_table is not None else -1 * 2**64, 
                a.originating_center if a.originating_center is not None else -1 * 2**64, 
                a.table_version if a.table_version is not None else -1 * 2**64)
    def __init__(self):
        self.tables = []
    def to_xml(self):
        elm = super().to_xml()
        for table in self.tables:
            elm.append(table.to_xml())
        return elm

class Table(BufrTableObject):
    __child_attribute__ = 'entries'
    __identity_attributes__ = ['table_type', 'master_table', 'originating_center', 'table_version']
    def __init__(self, table_type=None, master_table=None, originating_center=None, table_version=None):
        self.table_type = table_type
        self.master_table = parse_int(master_table, 1)
        self.originating_center = parse_int(originating_center, 2)
        self.table_version = parse_int(table_version, 1)
        self.entries = []
    def to_xml(self):
        elm = super().to_xml()
        for entry in self.entries:
            elm.append(entry.to_xml())
        return elm
    def __len__(self):
        return len(self.entries)
    def to_json(self):
        output = []
        for entry in self.entries:
            if type(entry) == ElementDefinition:
                output.append({
                    'f': entry.f,
                    'x': entry.x,
                    'y': entry.y,
                    'scale': entry.scale,
                    'reference_value': entry.reference_value,
                    'bit_width': entry.bit_width,
                    'unit': entry.unit,
                    'mnemonic': entry.mnemonic,
                    'desc_code': entry.desc_code,
                    'name': entry.name
                })
        return output

class SequenceDefinition(BufrTableObject):
    __child_attribute__ = 'elements'
    __identity_attributes__ = ['f', 'x', 'y']
    def __init__(self, f=None, x=None, y=None, mnemonic=None, dcod=None, name=None):
        self.f = parse_int(f, 1)
        self.x = parse_int(x, 1)
        self.y = parse_int(y, 1)
        self.mnemonic = mnemonic
        self.dcod = dcod
        self.name = name
        self.elements = []
    def to_xml(self):
        elm = super().to_xml()
        for element in self.elements:
            elm.append(element.to_xml())
        return elm
    def get_descriptors(self):
        descriptors = []
        for element in self.elements:
            descriptors.append([element.f, element.x, element.y])
        return descriptors

class SequenceElement(BufrTableObject):
    def __init__(self, f=None, x=None, y=None, name=None):
        self.f = parse_int(f, 1)
        self.x = parse_int(x, 1)
        self.y = parse_int(y, 1)
        self.name = name

class ElementDefinition(BufrTableObject):
    __identity_attributes__ = ['f', 'x', 'y']
    def __init__(self, f=None, x=None, y=None, scale=None, reference_value=None, bit_width=None, unit=None, mnemonic=None, desc_code=None, name=None):
        self.f = parse_int(f, 1)
        self.x = parse_int(x, 1)
        self.y = parse_int(y, 1)
        self.scale = parse_int(scale, 1, unsigned=False)
        self.reference_value = parse_int(reference_value, 4, unsigned=False)
        self.bit_width = parse_int(bit_width, 2)
        self.unit = unit
        self.mnemonic = mnemonic
        self.desc_code = desc_code
        self.name = name
        self.__table_f__ = None
    def set_table_f(self, table_f):
        self.__table_f__ = table_f
    def __repr__(self):
        return ('{0:s}(f={1:01d}, x={2:02d}, y={3:03d}, mnemonic="{4:s}", name="{5:s}")'.format(
            self.__class__.__name__,
            self.f,
            self.x,
            self.y,
            self.mnemonic,
            self.name
        ))
    def __str__(self):
        return '{0:01d}-{1:02d}-{2:03d}'.format(self.f, self.x, self.y)
    def read_value(self, bit_map):
        data_value = BUFRMissingValue(self)
        data_bytes = bit_map.read(self.bit_width)
        if not bit_map.is_missing_value(data_bytes, self.bit_width):
            if self.unit == "CCITT IA5":
                data_value = BUFRString(self, data_bytes)
            else:
                if self.unit == "Code table":
                    data_value = BUFRCodeTable(self, data_bytes, self.__table_f__)
                elif self.unit == "Flag table":
                    data_value = BUFRFlagTable(self, data_bytes)
                else:
                    data_value = BUFRNumeric(self, data_bytes)
        return data_value

class CodeEntry(BufrTableObject):
    __identity_attributes__ = ['code']
    def __init__(self, code=None, meaning=None):
        self.code = parse_int(code, 4)
        self.meaning = meaning

class CodeTableDefinition(BufrTableObject):
    __child_attribute__ = 'codes'
    __replace_duplicate_child__ = True
    __identity_attributes__ = ['f', 'x', 'y', 'condition_f', 'condition_x', 'condition_y', 'condition_value']
    def __init__(self, f=None, x=None, y=None, mnemonic=None, condition_f=None, condition_x=None, condition_y=None, condition_value=None):
        self.f = parse_int(f, 1)
        self.x = parse_int(x, 1)
        self.y = parse_int(y, 1)
        self.mnemonic = mnemonic
        self.condition_f = parse_int(condition_f, 1)
        self.condition_x = parse_int(condition_x, 1)
        self.condition_y = parse_int(condition_y, 1)
        self.condition_value = condition_value
        self.codes = []
    def to_xml(self):
        elm = super().to_xml()
        for code in self.codes:
            elm.append(code.to_xml())
        return elm

class FlagTableDefinition(BufrTableObject):
    __child_attribute__ = 'codes'
    __replace_duplicate_child__ = True
    __identity_attributes__ = ['f', 'x', 'y', 'condition_f', 'condition_x', 'condition_y', 'condition_value']
    def __init__(self, f=None, x=None, y=None, mnemonic=None, condition_f=None, condition_x=None, condition_y=None, condition_value=None):
        self.f = parse_int(f, 1)
        self.x = parse_int(x, 1)
        self.y = parse_int(y, 1)
        self.mnemonic = mnemonic
        self.condition_f = parse_int(condition_f, 1)
        self.condition_x = parse_int(condition_x, 1)
        self.condition_y = parse_int(condition_y, 1)
        self.condition_value = parse_int(condition_value, 2)
        self.codes = []
    def to_xml(self):
        elm = super().to_xml()
        for code in self.codes:
            elm.append(code.to_xml())
        return elm

