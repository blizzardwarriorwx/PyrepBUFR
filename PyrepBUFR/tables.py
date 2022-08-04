from collections import namedtuple
from copy import deepcopy
from sys import modules
from typing import Any
from xml.dom.minidom import parseString
from xml.etree import ElementTree as ET, ElementInclude

try:
    from numpy import dtype
    def parse_int(value, byte_width, big_endian=True, unsigned=True):
        return dtype(('u' if unsigned else 'i') + str(byte_width)).type(value) if value is not None else None
except ModuleNotFoundError:
    def parse_int(value, byte_width, big_endian=True, unsigned=True):
        return int(value) if value is not None else None

def obj2dict(obj):
    return (obj.id, obj)

def xml2class(node: ET.Element) -> Any:
    cls = getattr(modules[__name__], node.tag)
    return cls.from_xml(node)

def read_xml(filename: str) -> Any:
    tree = ET.parse(filename)
    root = tree.getroot()
    ElementInclude.include(root)
    return xml2class(root)

def write_xml(xml_tree: Any, filename: str) -> None:
    with open(filename, 'w') as out_file:
        out_file.write(xml_tree.to_xml())

class BUFRTableObjectBase(object):
    __slots__ = ()
    __id_class__ = None
    def __getattribute__(self, __name: str) -> Any:
        try:
            return super().__getattribute__(__name)
        except AttributeError:
            if 'id' not in self.__slots__ or __name not in self.id._fields:
                raise
            return self.id.__getattribute__(__name)
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == 'id':
            raise AttributeError('Cannot set attribute {0:s}'.format(__name))
        return super().__setattr__(__name, __value)
    def __initialize_id__(self, id):
        if type(id) != self.__id_class__:
            raise ValueError('ID must be class {0}'.format(self.__id_class__))
        super().__setattr__('id', id)
    def __repr__(self):
        value_format = lambda a: ('"{0}"' if type(a) == str else '{0}').format(a)
        return super().__repr__() if self.is_container else '{0:s}({1:s})'.format(self.__class__.__name__, ', '.join(['{0:s}={1:s}'.format(x, value_format(getattr(self, x, None))) for x in (list(self.id._fields) + list(self.__slots__[1:]))]))
    @property
    def is_container(self):
        return issubclass(self.__class__, dict)
    @staticmethod
    def __clone_class__(obj, *args, **kwargs):
        class_args = []
        slot_offset = 0
        if 'id' in obj.__slots__:
            class_args.extend(obj.id)
            slot_offset = 1
        if len(obj.__slots__) - slot_offset > 0:
            class_args.extend([getattr(obj, x, None) for x in  obj.__slots__[slot_offset:]])
        class_args.extend(args)
        return obj.__class__(*class_args, **kwargs)
    def to_element(self):
        elm = ET.Element(self.__class__.__name__)
        slot_offset = 0
        if 'id' in self.__slots__:
            for attribute, value in self.id._asdict().items():
                if value is not None:
                    elm.set(attribute.replace('_', '-'), str(value))
            slot_offset = 1
        if len(self.__slots__) - slot_offset > 0:
            for attribute in self.__slots__[slot_offset:]:
                value = getattr(self, attribute, None)
                if value is not None:
                    elm.set(attribute.replace('_', '-'), str(value))
        if self.is_container:
            for child in sorted(self):
                elm.append(self[child].to_element())
        return elm
    def to_xml(self):
        return parseString(ET.tostring(self.to_element())).toprettyxml()
    def diff(self, other):
        if self.__class__ != other.__class__:
            raise TypeError('Types must match for diff')
        if self.is_container:
            self_set = frozenset(self.keys())
            other_set = frozenset(other.keys())
            missing_set = other_set.difference(self_set)
            non_match_set = frozenset([key for key in other_set.intersection(self_set) if other[key] != self[key]])
            content = [(key, other[key]) for key in missing_set]
            content.extend([(key, other[key]) if not other[key].is_container else (key, self[key].diff(other[key])) for key in non_match_set])
            return BUFRTableObjectBase.__clone_class__(other, content)
        else:
            return self == other
    def __eq__(self, other):
        return  self.__class__ == other.__class__
    def __ne__(self, other):
        return not self == other
    def __deepcopy__(self, memo):
        class_args = list(self.id) + [getattr(self, x, None) for x in  self.__slots__[1:]] 
        if self.is_container:
            class_args += [[obj2dict(deepcopy(value)) for value in self.values()]]
        return self.__class__(*class_args)

class BUFRTableContainerBase(dict):
    __slots__ = ()
    def __hash__(self):
        return hash(self.id)
    def __add__(self, b):
        self.extend(b)
        return self
    def extend(self, item):
        if self.__class__ == item.__class__:
            self.update(item)
        else:
            self.append(item)
    def append(self, item):
        self[item.id] = item
    def iloc(self, index):
        return self[sorted(self.keys())[index]]
    def find(self, search_func):
        return BUFRTableObjectBase.__clone_class__(self, [(key, self[key]) for key in self if search_func(key)])
    @property
    def is_empty(self):
        return len(self) == 0

class TableCollection(BUFRTableObjectBase, BUFRTableContainerBase):
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            self_keys = set(self.keys())
            other_keys = set(other.keys())
            match = self_keys == other_keys
            if match:
                match = min([self[key] == other[key] for key in self_keys])
        return match
    @staticmethod
    def from_xml(elm):
        return TableCollection(
            [obj2dict(xml2class(child)) for child in elm]
        )

class Table(BUFRTableObjectBase, BUFRTableContainerBase):
    __slots__ = ('id',)
    __id_class__ = namedtuple('TableID', 
                              ('table_type', 'master_table', 'originating_center', 'table_version'),
                              defaults=(None, None, None, None))
    def __init__(self, table_type, master_table, originating_center, table_version, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super().__initialize_id__(self.__id_class__(
            table_type=table_type,
            master_table=parse_int(master_table, 1),
            originating_center=parse_int(originating_center, 2),
            table_version=parse_int(table_version, 1)
        ))
    @staticmethod
    def from_xml(elm):
        return Table(
            elm.attrib.get('table-type', None),
            elm.attrib.get('master-table', None),
            elm.attrib.get('originating-center', None),
            elm.attrib.get('table-version', None),
            [obj2dict(xml2class(child)) for child in elm]
        )
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            self_keys = set(self.keys())
            other_keys = set(other.keys())
            match = self_keys == other_keys
            if match:
                match = min([self[key] == other[key] for key in self_keys]) and other.id == self.id
        return match

class ElementDefinition(BUFRTableObjectBase):
    __slots__ = 'id', 'scale', 'reference_value', 'bit_width', 'unit', 'mnemonic', 'desc_code', 'name'
    __id_class__ = namedtuple('ElementDefinitionID', ('f', 'x', 'y'))

    def __init__(self, f, x, y, scale, reference_value, bit_width, unit, mnemonic, desc_code, name):
        super().__initialize_id__(self.__id_class__(
            f=parse_int(f, 1), 
            x=parse_int(x, 1), 
            y=parse_int(y, 1)
        ))
        self.scale = parse_int(scale, 1, unsigned=False)
        self.reference_value = parse_int(reference_value, 4, unsigned=False)
        self.bit_width = parse_int(bit_width, 2)
        self.unit = unit
        self.mnemonic = mnemonic
        self.desc_code = desc_code
        self.name = name
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            match = (self.id == other.id
                 and self.scale == other.scale
                 and self.reference_value == other.reference_value
                 and self.bit_width == other.bit_width
                 and self.unit == other.unit
                 and self.mnemonic == other.mnemonic)
        return match
    @staticmethod
    def from_xml(elm):
        return ElementDefinition(
            elm.attrib.get('f', None),
            elm.attrib.get('x', None),
            elm.attrib.get('y', None),
            elm.attrib.get('scale', None),
            elm.attrib.get('reference-value', None),
            elm.attrib.get('bit-width', None),
            elm.attrib.get('unit', None),
            elm.attrib.get('mnemonic', None),
            elm.attrib.get('desc-code', None),
            elm.attrib.get('name', None)
        )

class SequenceDefinition(BUFRTableObjectBase, BUFRTableContainerBase):
    __slots__ = ('id', 'mnemonic', 'dcod', 'name')
    __id_class__ = namedtuple('SequenceDefinitionID', ('f', 'x', 'y'))
    def __init__(self, f, x, y, mnemonic, dcod, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super().__initialize_id__(self.__id_class__(
            f=parse_int(f, 1),
            x=parse_int(y, 1),
            y=parse_int(y, 1)
        ))
        self.mnemonic = mnemonic
        self.dcod = dcod
        self.name = name
    @staticmethod
    def from_xml(elm):
        return SequenceDefinition(
            elm.attrib.get('f', None),
            elm.attrib.get('x', None),
            elm.attrib.get('y', None),
            elm.attrib.get('mnemonic', None),
            elm.attrib.get('dcode', None),
            elm.attrib.get('name', None),
            [obj2dict(xml2class(child)) for child in elm]
        )
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            self_keys = set(self.keys())
            other_keys = set(other.keys())
            match = self_keys == other_keys
            if match:
                match = min([self[key] == other[key] for key in self_keys]) and other.id == self.id
        return match

class SequenceElement(BUFRTableObjectBase):
    __slots__ = ('id', 'f', 'x', 'y', 'name')
    __id_class__ = namedtuple('SequenceElementID', ('index'))
    def __init__(self, index, f, x, y, name):
        super().__initialize_id__(self.__id_class__(parse_int(index, 2)))
        self.f = parse_int(f, 1)
        self.x = parse_int(x, 1)
        self.y = parse_int(y, 1)
        self.name = name
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            match = (self.id == other.id
                 and self.f == other.f
                 and self.x == other.x
                 and self.y == other.y)
        return match
    @staticmethod
    def from_xml(elm):
        return SequenceElement(
            elm.attrib.get('index', None),
            elm.attrib.get('f', None),
            elm.attrib.get('x', None),
            elm.attrib.get('y', None),
            elm.attrib.get('name', None)
        )

class CodeFlagDefinition(BUFRTableObjectBase, BUFRTableContainerBase):
    __slots__ = ('id','mnemonic')
    __id_class__ = namedtuple('CodeFlagDefinitionID', ('f', 'x', 'y', 'is_flag', 'condition_f', 'condition_x', 'condition_y', 'condition_value'))
    def __init__(self, f, x, y, is_flag, condition_f, condition_x, condition_y, condition_value, mnemonic, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super().__initialize_id__(self.__id_class__(
            f=parse_int(f, 1),
            x=parse_int(x, 1),
            y=parse_int(y, 1),
            is_flag=bool(is_flag) if is_flag is not None else None,
            condition_f=parse_int(condition_f, 1),
            condition_x=parse_int(condition_x, 1),
            condition_y=parse_int(condition_y, 1),
            condition_value=parse_int(condition_value, 4)
        ))
        self.mnemonic = mnemonic
    @staticmethod
    def from_xml(elm):
        return CodeFlagDefinition(
            elm.attrib.get('f', None),
            elm.attrib.get('x', None),
            elm.attrib.get('y', None),
            elm.attrib.get('is-flag', None),
            elm.attrib.get('condition-f', None),
            elm.attrib.get('condition-x', None),
            elm.attrib.get('condition-y', None),
            elm.attrib.get('condition-value', None),
            elm.attrib.get('mnemonic', None),
            [obj2dict(xml2class(child)) for child in elm]
        )
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            self_keys = set(self.keys())
            other_keys = set(other.keys())
            match = self_keys <= other_keys and other.id == self.id
            if match:
                match = min([self[key] == other[key] for key in self_keys]) 
        return match

class CodeFlagElement(BUFRTableObjectBase):
    __slots__ = ('id', 'meaning')
    __id_class__ = namedtuple('CodeFlagElementID', ('code'))
    def __init__(self, code, meaning):
        super().__initialize_id__(self.__id_class__(
            parse_int(code, 4)
        ))
        self.meaning = meaning
    def __eq__(self, other):
        match = super().__eq__(other)
        if match:
            match = (self.id == other.id
                 and self.meaning.lower().strip() == other.meaning.lower().strip())
        return match
    @staticmethod
    def from_xml(elm):
        return CodeFlagElement(
            elm.attrib.get('code', None),
            elm.attrib.get('meaning', None)
        )