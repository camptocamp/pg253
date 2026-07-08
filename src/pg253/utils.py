""" Module containing utility functions for common processing tasks. """

def sizeof_fmt(num):
    """ Returns a formatted size in bytes for a given raw value. """

    num = int(num)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    return "{num:.1f}TB"
