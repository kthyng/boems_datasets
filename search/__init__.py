try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

# import search.search
# import search.ErddapReader
from .Data import (Data)
# import Data
from .ErddapReader import (ErddapReader)
from .axdsReader import (axdsReader)