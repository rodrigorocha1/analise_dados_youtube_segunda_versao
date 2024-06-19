try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass

print(*sys.path, sep='\n')
