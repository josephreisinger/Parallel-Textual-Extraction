class _Version(tuple):
    """internal version object, subclass of C{tuple},
    but implements a fancier __str__ representation
    """
    def __str__(self):
        return '.'.join([str(x) for x in self])

version = _Version((0,9,13))
del _Version
