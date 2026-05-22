#!/usr/bin/env python3
"""
Convert a GeoTIFF to a Cloud Optimized GeoTIFF (COG).

May 22, 2026 - The Rasters in our National Project are all over the place in terms of structure and metadata.
This script is a best-effort attempt to convert an arbitrary GeoTIFF into a well-formed COG, while preserving
important metadata like the Raster Attribute Table (RAT) and color table. It is not guaranteed to work on every possible input,
but should handle a wide range of common cases.

- Builds overviews on a temp file (source is never modified)
- Internalizes the RAT from a .vat.dbf sidecar if present
- Internalizes the color table if present
- Removes any .aux.xml sidecar created alongside the output

Usage:
    python make_cog.py input.tif output_cog.tif
    python make_cog.py input.tif output_cog.tif --compress DEFLATE --blocksize 512
"""

import argparse
import os

from osgeo import gdal, ogr

gdal.UseExceptions()


# ---------------------------------------------------------------------------
# RAT helpers
# ---------------------------------------------------------------------------


def rat_from_vat_dbf(vat_dbf_path: str) -> gdal.RasterAttributeTable | None:
    """
    Read a .vat.dbf file and return a populated GDAL RasterAttributeTable,
    or None if the file cannot be read.
    """
    ds = ogr.Open(vat_dbf_path)
    if ds is None:
        print(f"  WARNING: Could not open {vat_dbf_path} with OGR")
        return None

    layer = ds.GetLayer(0)
    if layer is None:
        return None

    layer_defn = layer.GetLayerDefn()
    field_count = layer_defn.GetFieldCount()

    rat = gdal.RasterAttributeTable()

    # Build columns — map common field names to proper GFU usages
    for i in range(field_count):
        fld = layer_defn.GetFieldDefn(i)
        name = fld.GetNameRef()
        otype = fld.GetType()

        if otype in (ogr.OFTInteger, ogr.OFTInteger64):
            rat_type = gdal.GFT_Integer
        elif otype == ogr.OFTReal:
            rat_type = gdal.GFT_Real
        else:
            rat_type = gdal.GFT_String

        name_up = name.upper()
        if name_up == 'VALUE':
            usage = gdal.GFU_MinMax
        elif name_up == 'COUNT':
            usage = gdal.GFU_PixelCount
        elif name_up in ('RED', 'R'):
            usage = gdal.GFU_Red
        elif name_up in ('GREEN', 'G'):
            usage = gdal.GFU_Green
        elif name_up in ('BLUE', 'B'):
            usage = gdal.GFU_Blue
        elif name_up == 'ALPHA':
            usage = gdal.GFU_Alpha
        else:
            usage = gdal.GFU_Generic

        rat.CreateColumn(name, rat_type, usage)

    # Populate rows
    features = list(layer)
    rat.SetRowCount(len(features))

    for row_idx, feat in enumerate(features):
        for col_idx in range(field_count):
            fld = layer_defn.GetFieldDefn(col_idx)
            otype = fld.GetType()
            if otype in (ogr.OFTInteger, ogr.OFTInteger64):
                rat.SetValueAsInt(row_idx, col_idx, feat.GetFieldAsInteger(col_idx))
            elif otype == ogr.OFTReal:
                rat.SetValueAsDouble(row_idx, col_idx, feat.GetFieldAsDouble(col_idx))
            else:
                rat.SetValueAsString(row_idx, col_idx, feat.GetFieldAsString(col_idx))

    print(f"  Loaded RAT from .vat.dbf: {rat.GetRowCount()} rows, {rat.GetColumnCount()} columns")
    return rat


def find_vat_dbf(src_path: str) -> str | None:
    """
    Return the path to a .vat.dbf sidecar for src_path, or None if not found.
    Checks both 'foo.tif.vat.dbf' and 'foo.vat.dbf' conventions.
    """
    candidates = [
        src_path + '.vat.dbf',
        os.path.splitext(src_path)[0] + '.vat.dbf',
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


# ---------------------------------------------------------------------------
# Main conversion
# ---------------------------------------------------------------------------


def make_cog(
    src_path: str,
    dst_path: str,
    compress: str = 'DEFLATE',
    blocksize: int = 512,
    bigtiff: str = 'IF_SAFER',
    resampling: str = 'NEAREST',
) -> None:

    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Source file not found: {src_path}")

    # ------------------------------------------------------------------
    # Open source and collect metadata we want to preserve
    # ------------------------------------------------------------------
    print(f"\n[1/5] Opening source: {src_path}")
    src_ds = gdal.Open(src_path, gdal.GA_ReadOnly)
    if src_ds is None:
        raise RuntimeError(f"GDAL could not open: {src_path}")

    src_band = src_ds.GetRasterBand(1)
    dtype = src_band.DataType

    # Choose PREDICTOR based on data type:
    #   Predictor=2  →  integer (byte, int16, uint16, int32 …)
    #   Predictor=3  →  float32 / float64
    #   Predictor=1  →  everything else (safe default)
    float_types = (gdal.GDT_Float32, gdal.GDT_Float64, gdal.GDT_CFloat32, gdal.GDT_CFloat64)
    int_types = (gdal.GDT_Byte, gdal.GDT_UInt16, gdal.GDT_Int16, gdal.GDT_UInt32, gdal.GDT_Int32)
    if dtype in float_types:
        predictor = '3'
    elif dtype in int_types:
        predictor = '2'
    else:
        predictor = '1'
    print(f"  Data type: {gdal.GetDataTypeName(dtype)}  →  PREDICTOR={predictor}")

    # Collect RAT — GDAL may already have read it from .vat.dbf
    src_rat = src_band.GetDefaultRAT()

    # Collect color table
    src_ct = src_band.GetRasterColorTable()
    if src_ct:
        src_ct = src_ct.Clone()  # clone before closing src_ds

    nodata = src_band.GetNoDataValue()

    width = src_ds.RasterXSize
    height = src_ds.RasterYSize
    src_ds = None  # close — we reopen below after building overviews

    # ------------------------------------------------------------------
    # Build overviews on a temp copy (never touch the source file)
    # ------------------------------------------------------------------
    tmp_path = dst_path + '.building.tif'

    try:
        print(f"\n[2/5] Building temp file with overviews: {tmp_path}")
        src_ds = gdal.Open(src_path, gdal.GA_ReadOnly)

        driver = gdal.GetDriverByName('GTiff')
        tmp_ds = driver.CreateCopy(tmp_path, src_ds, strict=0, options=['COMPRESS=DEFLATE', 'TILED=YES', 'BLOCKXSIZE=512', 'BLOCKYSIZE=512'])
        tmp_ds.FlushCache()
        tmp_ds = None
        src_ds = None

        # Compute overview levels: keep halving until smallest dimension ≤ blocksize
        tmp_ds = gdal.Open(tmp_path, gdal.GA_Update)
        levels = []
        factor = 2
        min_dim = min(tmp_ds.RasterXSize, tmp_ds.RasterYSize)
        while min_dim // factor > blocksize:
            levels.append(factor)
            factor *= 2

        if levels:
            print(f"  Overview levels: {levels}")
            tmp_ds.BuildOverviews(resampling, levels)
        else:
            print("  Raster is small enough — no overviews needed")

        tmp_ds.FlushCache()
        tmp_ds = None

        # ------------------------------------------------------------------
        # Translate temp → COG
        # ------------------------------------------------------------------
        print(f"\n[3/5] Translating to COG: {dst_path}")
        translate_options = gdal.TranslateOptions(
            format='COG',
            creationOptions=[
                f'COMPRESS={compress}',
                f'PREDICTOR={predictor}',
                f'BLOCKSIZE={blocksize}',
                f'RESAMPLING={resampling}',
                'COPY_SRC_OVERVIEWS=YES',
                f'BIGTIFF={bigtiff}',
            ],
        )
        result = gdal.Translate(dst_path, tmp_path, options=translate_options)
        if result is None:
            raise RuntimeError("gdal.Translate returned None — COG creation failed")
        result = None

    finally:
        for path in [tmp_path, tmp_path + '.aux.xml']:
            if os.path.exists(path):
                os.remove(path)

    # ------------------------------------------------------------------
    # Internalize RAT
    # ------------------------------------------------------------------
    print("\n[4/5] Checking for RAT...")

    # Prefer an explicitly found .vat.dbf so we control the parsing
    vat_path = find_vat_dbf(src_path)
    if vat_path:
        print(f"  Found .vat.dbf: {vat_path}")
        rat = rat_from_vat_dbf(vat_path)
    elif src_rat is not None:
        print("  Using RAT already read by GDAL from source")
        rat = src_rat
    else:
        rat = None
        print("  No RAT found — skipping")

    dst_ds = gdal.Open(dst_path, gdal.GA_Update)
    dst_band = dst_ds.GetRasterBand(1)

    if rat is not None:
        dst_band.SetDefaultRAT(rat)
        print("  RAT written internally to COG")

    # Internalize color table if present
    if src_ct is not None:
        dst_band.SetRasterColorTable(src_ct)
        print("  Color table written internally to COG")

    dst_ds.FlushCache()
    dst_ds = None

    # Remove any .aux.xml GDAL may have created alongside the output
    aux_xml = dst_path + '.aux.xml'
    if os.path.exists(aux_xml):
        print(f"  Removing sidecar: {aux_xml}")
        os.remove(aux_xml)

    # ------------------------------------------------------------------
    # Verify
    # ------------------------------------------------------------------
    print("\n[5/5] Verification")
    verify_ds = gdal.Open(dst_path)
    verify_band = verify_ds.GetRasterBand(1)

    rat_check = verify_band.GetDefaultRAT()
    ct_check = verify_band.GetRasterColorTable()
    nd_check = verify_band.GetNoDataValue()

    print(f"  File size : {os.path.getsize(dst_path) / (1024**2):.1f} MB")
    print(f"  Dimensions: {verify_ds.RasterXSize} x {verify_ds.RasterYSize}")
    print(f"  NoData    : {nd_check}")
    print(f"  RAT       : {'✅ ' + str(rat_check.GetRowCount()) + ' rows' if rat_check else '⚠️  not present'}")
    print(f"  Color table: {'✅ ' + str(ct_check.GetCount()) + ' entries' if ct_check else '⚠️  not present'}")

    # Confirm no unexpected sidecar files were left behind
    for sidecar in [dst_path + '.aux.xml', dst_path + '.vat.dbf', os.path.splitext(dst_path)[0] + '.aux.xml']:
        if os.path.exists(sidecar):
            print(f"  ⚠️  Unexpected sidecar still present: {sidecar}")

    verify_ds = None
    print(f"\nDone → {dst_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description='Convert a GeoTIFF to COG with internalized RAT and color table')
    parser.add_argument('src', help='Source GeoTIFF')
    parser.add_argument('dst', help='Output COG GeoTIFF')
    parser.add_argument('--compress', default='DEFLATE', choices=['DEFLATE', 'LZW', 'ZSTD'], help='Compression (default: DEFLATE)')
    parser.add_argument('--blocksize', type=int, default=512, help='Tile block size in pixels (default: 512)')
    parser.add_argument('--bigtiff', default='IF_SAFER', choices=['YES', 'NO', 'IF_NEEDED', 'IF_SAFER'], help='BIGTIFF mode (default: IF_SAFER)')
    parser.add_argument('--resampling', default='NEAREST', choices=['NEAREST', 'AVERAGE', 'BILINEAR', 'CUBIC'], help='Resampling for overviews (default: NEAREST)')

    args = parser.parse_args()

    make_cog(
        src_path=args.src,
        dst_path=args.dst,
        compress=args.compress,
        blocksize=args.blocksize,
        bigtiff=args.bigtiff,
        resampling=args.resampling,
    )


if __name__ == '__main__':
    main()
