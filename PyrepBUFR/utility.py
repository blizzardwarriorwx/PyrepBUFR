from copy import deepcopy

from .external import arange, ceil, frombuffer, log, min_scalar_type, uint8

def read_integer(byte_string, big_endian=True, unsigned=True):
    padding = b''
    byte_length = len(byte_string)
    byte_width = uint8(2 ** ceil(log(byte_length) / log(2)))
    if byte_length < byte_width:
        padding += 'b\x00' * (byte_width - byte_length)
    return frombuffer((padding + byte_string) if big_endian else (byte_string + padding),
                        ('>' if big_endian else '<') + ('u' if unsigned else 'i') + str(byte_width))[0]

def byte_integer(value, bit_length, big_endian=True, unsigned=True):
    byte_width = uint8(2**ceil(log(ceil(bit_length/8)*8)/log(2))//8)
    output_dtype = ('>' if big_endian else '<') + ('u' if unsigned else 'i') + str(byte_width)
    value = value.astype(('>' if big_endian else '<') + ('u' if unsigned else 'i') + str(byte_width))
    if value.dtype.descr[0][1] != output_dtype:
        value = value.byteswap()
    return value.tobytes()

def read_integers(byte_string, byte_width, big_endian=True, unsigned=True):
    padding = b''
    byte_length = len(byte_string)
    for i in arange(byte_length % byte_width):
        padding += b'\x00'
    return frombuffer(byte_string + padding,
                        ('>' if big_endian else '<') + ('u' if unsigned else 'i') + str(byte_width))

def get_min_type(value):
    return min_scalar_type(value).type(value)

def dict_merge(initial_values, new_values):
    output = deepcopy(initial_values)
    output.update(new_values)
    return output