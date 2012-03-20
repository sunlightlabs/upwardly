#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"
DATA_DIR="$( cd $DIR/../data && pwd )"

if [ ! -d "$DATA_DIR" ]; then
    mkdir "$DATA_DIR"
fi

#
# consumer price index
#

CPI_DIR="$DATA_DIR/cpi"

if [ ! -d "$CPI_DIR" ]; then
    mkdir "$CPI_DIR"
fi

rm $CPI_DIR/*

wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.area
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.item
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.3.AsizeNorthEast
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.4.AsizeNorthCentral
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.5.AsizeSouth
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.6.AsizeWest
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.7.OtherNorthEast
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.8.OtherNorthCentral
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.9.OtherSouth
wget -P $CPI_DIR ftp://ftp.bls.gov/pub/time.series/cu/cu.data.10.OtherWest

#
# locations
#

LOC_DIR="$DATA_DIR/locations"

if [ ! -d "$LOC_DIR" ]; then
    mkdir "$LOC_DIR"
fi

rm $LOC_DIR/*

wget -P $LOC_DIR http://www.census.gov/population/www/metroareas/lists/2009/List{1..11}.txt
wget -P $LOC_DIR http://www.census.gov/population/www/metroareas/lists/2009/List3.xls
wget -P $LOC_DIR http://www.census.gov/population/www/metroareas/lists/2009/List9.xls

wget -P $LOC_DIR http://www.census.gov/geo/cob/bdy/ma/ma99/ma99shp/ma99_99_shp.zip
wget -P $LOC_DIR http://www.census.gov/geo/cob/bdy/ma/ma99/cm99shp/cm99_99_shp.zip

unzip -d $LOC_DIR $LOC_DIR/ma99_99_shp.zip
unzip -d $LOC_DIR $LOC_DIR/cm99_99_shp.zip

rm $LOC_DIR/ma99_99_shp.zip
rm $LOC_DIR/cm99_99_shp.zip

#
# nces - schools
#

NCES_DIR="$DATA_DIR/nces"

if [ ! -d "$NCES_DIR" ]; then
    mkdir "$NCES_DIR"
fi

rm $NCES_DIR/*

wget -P $NCES_DIR http://nces.ed.gov/ccd/data/zip/sc091a_csv.zip
unzip -d $NCES_DIR $NCES_DIR/sc091a_csv.zip
rm $NCES_DIR/sc091a_csv.zip

#
# FFIEC
#

FFIEC_DIR="$DATA_DIR/ffiec"

if [ ! -d "$FFIEC_DIR" ]; then
    mkdir "$FFIEC_DIR"
fi

rm $FFIEC_DIR/*

wget -P $FFIEC_DIR http://www.ffiec.gov/hmda/pdf/msa11inc.pdf
pdftotext $FFIEC_DIR/msa11inc.pdf $FFIEC_DIR/msa11inc.txt

#
# OES
#

wget ftp://ftp.bls.gov/pub/special.requests/oes/oesm10ma.zip



#
# zipcodes
#

wget http://download.geonames.org/export/zip/US.zip