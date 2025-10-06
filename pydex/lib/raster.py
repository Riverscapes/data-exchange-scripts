from __future__ import annotations
import math
from typing import Dict
from os import path
import json
from time import time
from rsxml import Logger

from pydex.imports import import_geo


gdal, ogr, osr, shapely, np = import_geo()


class Raster:
    """ A class to handle raster data

    NOTE: This class was moved from RSCommons and stripped down to only include the most essential functions
    """

    def __init__(self, sfilename):
        self.filename = sfilename
        self.log = Logger("Raster")
        self.errs = ""
        try:
            if path.isfile(self.filename):
                src_ds = gdal.Open(self.filename)
            else:
                self.log.error(f'Missing file: {self.filename}')
                raise Exception(f'Could not find raster file: {path.basename(self.filename)}')
        except RuntimeError as e:
            print(f'Could not open file: {self.filename}', e)
            raise e

        try:
            # Read Raster Properties
            srcband = src_ds.GetRasterBand(1)
            self.bands = src_ds.RasterCount
            self.driver = src_ds.GetDriver().LongName
            self.gt = src_ds.GetGeoTransform()
            self.nodata = srcband.GetNoDataValue()
            """ Turn a Raster with a single band into a 2D [x,y] = v array """
            self.array = srcband.ReadAsArray()

            # Now mask out any NAN or nodata values (we do both for consistency)
            if self.nodata is not None:
                # To get over the issue where self.nodata may be imprecisely set we may need to use the array's
                # true nodata, taken directly from the array
                workingNodata = self.nodata
                self.min = np.nanmin(self.array)
                if isclose(self.min, self.nodata, rel_tol=1e-03):
                    workingNodata = self.min
                self.array = np.ma.array(self.array, mask=(np.isnan(self.array) | (self.array == workingNodata)))

            self.dataType = srcband.DataType
            self.min = np.nanmin(self.array)
            self.max = np.nanmax(self.array)
            self.proj = src_ds.GetProjection()

            # Remember:
            # [0]/* top left x */
            # [1]/* w-e pixel resolution */
            # [2]/* rotation, 0 if image is "north up" */
            # [3]/* top left y */
            # [4]/* rotation, 0 if image is "north up" */
            # [5]/* n-s pixel resolution */
            self.left = self.gt[0]
            self.cellWidth = self.gt[1]
            self.top = self.gt[3]
            self.cellHeight = self.gt[5]
            self.cols = src_ds.RasterXSize
            self.rows = src_ds.RasterYSize
            # Important to throw away the srcband
            srcband.FlushCache()
            srcband = None

        except RuntimeError as e:
            print(f'Could not retrieve meta Data for {self.filename}', e)
            raise e

    def __enter__(self) -> Raster:
        """Behaviour on open when using the "with VectorBase():" Syntax
        """
        return self

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with VectorBase():" Syntax
        """
        print('hi')

    def getBottom(self):
        """ Get the bottom of the raster

        Returns:
            _type_: _description_
        """
        return self.top + (self.cellHeight * self.rows)

    def getRight(self):
        """ Get the right of the raster

        Returns:
            _type_: _description_
        """
        return self.left + (self.cellWidth * self.cols)

    def getWidth(self):
        """ Get the width of the raster

        Returns:
            _type_: _description_
        """
        return self.getRight() - self.left

    def getHeight(self):
        """ Get the height of the raster

        Returns:
            _type_: _description_
        """
        return self.top - self.getBottom()

    def getBoundaryShape(self):
        """ Get the boundary shape of the raster

        Returns:
            _type_: _description_
        """
        return shapely.geometry.Polygon([
            (self.left, self.top),
            (self.getRight(), self.top),
            (self.getRight(), self.getBottom()),
            (self.left, self.getBottom()),
        ])

    def boundsContains(self, bounds, pt):
        """ Check if the bounds contain a point

        Args:
            bounds (_type_): _description_
            pt (_type_): _description_

        Returns:
            _type_: _description_
        """
        return (bounds[0] < pt.coords[0][0] and bounds[1] < pt.coords[0][1] and bounds[2] > pt.coords[0][0] and bounds[3] > pt.coords[0][1])

    def rasterMaskLayer(self, shapefile, fieldname=None):
        """
        return a masked array that corresponds to the input polygon
        :param polygon:
        :return:
        """
        # Create a memory raster to rasterize into.
        target_ds = gdal.GetDriverByName('MEM').Create('', self.cols, self.rows, 1, gdal.GDT_Byte)
        target_ds.SetGeoTransform(self.gt)

        assert len(shapefile) > 0, "The ShapeFile path is empty"

        # Create a memory layer to rasterize from.
        driver = ogr.GetDriverByName("ESRI Shapefile")
        src_ds = driver.Open(shapefile, 0)
        src_lyr = src_ds.GetLayer()

        # Run the algorithm.
        options = ['ALL_TOUCHED=TRUE']
        if fieldname and len(fieldname) > 0:
            options.append('ATTRIBUTE=' + fieldname)

        err = gdal.RasterizeLayer(target_ds, [1], src_lyr, options=options)
        if err:
            print(err)

        # Get the array:
        band = target_ds.GetRasterBand(1)
        return band.ReadAsArray()

    def getPixelVal(self, pt):
        """ Get the pixel value at a point

        Args:
            pt (_type_): _description_

        Returns:
            _type_: _description_
        """
        # Convert from map to pixel coordinates.
        # Only works for geotransforms with no rotation.
        px = int((pt[0] - self.left) / self.cellWidth)  # x pixel
        py = int((pt[1] - self.top) / self.cellHeight)  # y pixel
        val = self.array[py, px]
        if isclose(val, self.nodata, rel_tol=1e-07) or val is np.ma.masked:
            return np.nan

        return val

    def lookupRasterValues(self, points):
        """
        Given an array of points with real-world coordinates, lookup values in raster
        then mask out any nan/nodata values
        :param points:
        :param raster:
        :return:
        """
        pointsdict = {"points": points, "values": []}

        for pt in pointsdict['points']:
            pointsdict['values'].append(self.getPixelVal(pt.coords[0]))

        # Mask out the np.nan values
        pointsdict['values'] = np.ma.masked_invalid(pointsdict['values'])

        return pointsdict

    def write(self, outputRaster):
        """
        Write this raster object to a file. The Raster is closed after this so keep that in mind
        You won't be able to access the raster data after you run this.
        :param outputRaster:
        :return:
        """
        if path.isfile(outputRaster):
            deleteRaster(outputRaster)

        driver = gdal.GetDriverByName('GTiff')
        outRaster = driver.Create(outputRaster, self.cols, self.rows, 1, self.dataType, ['COMPRESS=DEFLATE'])

        # Remember:
        # [0]/* top left x */
        # [1]/* w-e pixel resolution */
        # [2]/* rotation, 0 if image is "north up" */
        # [3]/* top left y */
        # [4]/* rotation, 0 if image is "north up" */
        # [5]/* n-s pixel resolution */
        outRaster.SetGeoTransform([self.left, self.cellWidth, 0, self.top, 0, self.cellHeight])
        outband = outRaster.GetRasterBand(1)

        # Set nans to the original No Data Value
        outband.SetNoDataValue(self.nodata)
        self.array.data[np.isnan(self.array)] = self.nodata
        # Any mask that gets passed in here should have masked out elements set to
        # Nodata Value
        if isinstance(self.array, np.ma.MaskedArray):
            np.ma.set_fill_value(self.array, self.nodata)
            outband.WriteArray(self.array.filled())
        else:
            outband.WriteArray(self.array)

        spatialRef = osr.SpatialReference()
        spatialRef.ImportFromWkt(self.proj)

        outRaster.SetProjection(spatialRef.ExportToWkt())
        outband.FlushCache()
        # Important to throw away the srcband
        outband = None
        self.log.debug("Finished Writing Raster: {outputRaster}")

    def setArray(self, incomingArray, copy=False):
        """
        You can use the self.array directly but if you want to copy from one array
        into a raster we suggest you do it this way
        :param incomingArray:
        :return:
        """
        masked = isinstance(self.array, np.ma.MaskedArray)
        if copy:
            if masked:
                self.array = np.ma.copy(incomingArray)
            else:
                self.array = np.ma.masked_invalid(incomingArray, copy=True)
        else:
            if masked:
                self.array = incomingArray
            else:
                self.array = np.ma.masked_invalid(incomingArray)

        self.rows = self.array.shape[0]
        self.cols = self.array.shape[1]
        self.min = np.nanmin(self.array)
        self.max = np.nanmax(self.array)

    def bin_raster_categorical(self, window_size: int = 256) -> Dict[str, int]:
        """ Bin raster values into categories based on unique values in the raster.

        Args:
            window_size (int, optional): The size of the window to use for binning. Defaults to 256.

        Returns:
            Dict[str, int]: A dictionary mapping category names to their counts.
        """
        self.log.info(f"Binning categorical raster {self.filename} with window size {window_size}")

        ds = gdal.Open(self.filename)
        band = ds.GetRasterBand(1)
        cols, rows = ds.RasterXSize, ds.RasterYSize

        category_counts: Dict[str, int] = {}
        retval = {
            'min': float(self.min),
            'max': float(self.max),
            'nodata': float(self.nodata) if self.nodata is not None else None,
            'geotransform': ds.GetGeoTransform(),
            'proj': ds.GetProjection(),
            'value_count': 0,
            'hist_type': 'categorical',
            'bins': []
        }

        nodata = band.GetNoDataValue()

        self.log.info("Binning raster values...")
        start_time = time()
        for yoff in range(0, rows, window_size):
            for xoff in range(0, cols, window_size):
                xsize = min(window_size, cols - xoff)
                ysize = min(window_size, rows - yoff)
                arr = band.ReadAsArray(xoff, yoff, xsize, ysize).astype(np.float32)
                if nodata is not None:
                    arr = arr[arr != nodata]
                if arr.size == 0:
                    continue
                unique, counts = np.unique(arr, return_counts=True)
                gt = ds.GetGeoTransform()
                x0 = gt[0] + xoff * gt[1]
                y0 = gt[3] + yoff * gt[5]
                window_counts = dict(zip((str(int(u)) for u in unique), map(int, counts)))
                print(f"Window ({xoff},{yoff}) @ ({x0:.1f},{y0:.1f}): counts={window_counts}")
                retval['value_count'] += arr.size
                for category, count in window_counts.items():
                    # Category should be the string representation of an integer
                    category_counts[category] = category_counts.get(category, 0) + count
        end_time = time()

        self.log.info(f"Completed binning in {end_time - start_time:.2f} seconds")
        self.log.debug(f"Category Counts: \n\n{json.dumps(category_counts, indent=2)}\n")

        # Final shape needs to be : [{category: '1', count: 100}, ...]
        retval['bins'] = [{'category': k, 'cell_count': v} for k, v in category_counts.items()]

        return retval

    def bin_raster(self, bin_size: int = 100, window_size: int = 256) -> Dict[int, int]:
        """
        Bin raster values into elevation bins of size `bin_size`.
        The min and max elevation are determined from the raster itself.
        """

        self.log.info(f"Binning raster {self.filename} with bin size {bin_size} and window size {window_size}")

        ds = gdal.Open(self.filename)
        band = ds.GetRasterBand(1)
        cols, rows = ds.RasterXSize, ds.RasterYSize

        # Scan the raster to find min and max (ignoring nodata)
        nodata = band.GetNoDataValue()
        min_elev, max_elev = None, None

        # This should be very fast since we're only reading small windows at a time
        self.log.info("Scanning raster to determine min and max elevation...")
        start_time = time()
        for yoff in range(0, rows, window_size):
            for xoff in range(0, cols, window_size):
                xsize = min(window_size, cols - xoff)
                ysize = min(window_size, rows - yoff)
                arr = band.ReadAsArray(xoff, yoff, xsize, ysize).astype(np.float32)
                if nodata is not None:
                    arr = arr[arr != nodata]
                if arr.size == 0:
                    continue
                arr_min = arr.min()
                arr_max = arr.max()
                if min_elev is None or arr_min < min_elev:
                    min_elev = arr_min
                if max_elev is None or arr_max > max_elev:
                    max_elev = arr_max
        end_time = time()
        self.log.info(f"Determined min elevation: {min_elev}, max elevation: {max_elev} in {end_time - start_time:.2f} seconds")

        if min_elev is None or max_elev is None:
            print("No valid data found in raster.")
            return

        # Define bins based on discovered min/max
        # Round the min down and max up to the nearest bin_size
        min_bin_elev = math.floor(min_elev / bin_size) * bin_size
        max_bin_elev = math.ceil(max_elev / bin_size) * bin_size
        bins = np.arange(min_bin_elev, max_bin_elev + bin_size, bin_size)
        self.log.info(f"Defined {len(bins)-1} bins from {min_bin_elev} to {max_bin_elev}")

        retval = {
            'min': float(min_elev),
            'max': float(max_elev),
            'geotransform': ds.GetGeoTransform(),
            'proj': ds.GetProjection(),
            'nodata': float(nodata) if nodata is not None else None,
            'value_count': 0,
            'hist_type': 'continuous',
            'bin_size': bin_size,
            'bins': {}
        }

        # Now do the actual binning
        self.log.info("Binning raster values...")
        start_time = time()
        total_hist = np.zeros(len(bins) - 1, dtype=int)
        for yoff in range(0, rows, window_size):
            for xoff in range(0, cols, window_size):
                xsize = min(window_size, cols - xoff)
                ysize = min(window_size, rows - yoff)
                arr = band.ReadAsArray(xoff, yoff, xsize, ysize).astype(np.float32)
                if nodata is not None:
                    arr = arr[arr != nodata]
                if arr.size == 0:
                    continue
                hist, edges = np.histogram(arr, bins=bins)
                gt = ds.GetGeoTransform()
                x0 = gt[0] + xoff * gt[1]
                y0 = gt[3] + yoff * gt[5]
                print(f"Window ({xoff},{yoff}) @ ({x0:.1f},{y0:.1f}): counts={hist}")
                # Here you would accumulate hist into a total histogram if desired
                retval['value_count'] += arr.size
                total_hist += hist
        end_time = time()

        # Convert bins to a dictionary for easier use later
        self.log.info(f"Completed binning in {end_time - start_time:.2f} seconds")

        bin_dict = {min_bin_elev + i * bin_size: int(count) for i, count in enumerate(total_hist)}
        self.log.debug(f"Bins: \n\n{json.dumps(bin_dict, indent=2)}\n")
        retval['bins'] = [{'bin': k, 'cell_count': v} for k, v in bin_dict.items()]
        return retval


def isclose(a, b, rel_tol=1e-09, abs_tol=0):
    """ Compare two numbers for closeness

    Args:
        a (_type_): _description_
        b (_type_): _description_
        rel_tol (_type_, optional): _description_. Defaults to 1e-09.
        abs_tol (int, optional): _description_. Defaults to 0.

    Returns:
        _type_: _description_
    """
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def deleteRaster(sFullPath):
    """

    :param path:
    :return:
    """

    log = Logger("Delete Raster")

    if path.isfile(sFullPath):
        try:
            # Delete the raster properly
            driver = gdal.GetDriverByName('GTiff')
            gdal.Driver.Delete(driver, sFullPath)
            log.debug(f"Raster Successfully Deleted: {sFullPath}")
        except Exception as err:
            log.error(f"Failed to remove existing raster at {sFullPath}")
            raise err
    else:
        log.debug(f"No raster file to delete at {sFullPath}")
