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


def import_geo():
    """ Import gdal module

    Returns:
        gdal: The gdal module if successfully imported
    """
    success = True
    try:
        import numpy as np
    except ImportError:
        success = False
        print(colored(
            'numpy module not found. Please install numpy by running `pip install numpy`.',
            'red'
        ))
    try:
        import shapely
    except ImportError:
        success = False
        print(colored(
            'shapely module not found. Please install shapely by running `pip install shapely`.',
            'red'
        ))
    try:
        from osgeo import gdal, ogr, osr
    except ImportError:
        success = False
        print(colored(
            'GDAL module not found. Please install GDAL by following these steps:\n'
            '1. Install GDAL on your system (e.g., `brew install gdal` on macOS or `apt-get install gdal-bin` on Linux).\n'
            '2. Install the Python bindings: `pip install gdal`.\n'
            'For more details, visit: https://gdal.org/',
            'red'
        ))
    if not success:
        exit(1)

    return gdal, ogr, osr, shapely, np
