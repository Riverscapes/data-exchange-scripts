# File for handling conditional imports
from termcolor import colored

# First a function to import sqlite3


def import_sqlite3():
    """ Import sqlite3 module

    Returns:
        sqlite3: The sqlite3 module if successfully imported
    """
    try:
        import sqlite3
        return sqlite3
    except ImportError:
        print(colored(
            'sqlite3 module not found. This is a standard library module, so ensure your Python installation is complete.',
            'red'
        ))
        exit(1)


def import_gdal_ogr():
    """ Import gdal module

    Returns:
        gdal: The gdal module if successfully imported
    """
    try:
        from osgeo import gdal, ogr, osr
        return gdal, ogr, osr
    except ImportError:
        print(colored(
            'GDAL module not found. Please install GDAL by following these steps:\n'
            '1. Install GDAL on your system (e.g., `brew install gdal` on macOS or `apt-get install gdal-bin` on Linux).\n'
            '2. Install the Python bindings: `pip install gdal`.\n'
            'For more details, visit: https://gdal.org/',
            'red'
        ))
        exit(1)


def import_rs_commons():
    """ Import rs_commons module

    Returns:
        rs_commons: The rs_commons module if successfully imported
    """
    try:
        import rscommons
        return rscommons
    except ImportError:
        print(colored(
            'rscommons module not found. You will need to pip add it to the environment before you can use it.`.',
            'red'
        ))
        exit(1)
