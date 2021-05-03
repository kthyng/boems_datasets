try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

# import search.search
import search.ErddapReader
import search.axdsReader
import search.localReader

from .Data import (Data)
# import Data
# from .ErddapReader import (ErddapReader, region)
# from .axdsReader import (axdsReader)
# from .localReader import (localReader)