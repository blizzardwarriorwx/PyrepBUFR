from textwrap import wrap

def dump_bin(byte_string, bytes_per_line):
    byte_length = len(byte_string)
    byte_string = int.from_bytes(byte_string, 'big')
    return '\n'.join(wrap(' '.join(wrap('{{0:0{0:d}b}}'.format(byte_length*8).format(byte_string), 8)), 9*bytes_per_line))