#!\usr\local\python2\bin\python
#-*-coding: UTF-8-*-
import os, sys
import math
sys.path.append(os.path.dirname(__file__) + '/../')
#import exception.invalid_parameter_exception as ipe

earthRadius = 6378137
earthCircle = 2 * math.pi * earthRadius
maxLevel = 20
tilePixels = 256;
lonRange = [-180.0, 180.0]
latRange = [-85.0511287798, 85.0511287798]

def verifyLevel(level):
    if level < 0 or level > maxLevel:
        raise ipe.InvalidParameterException('invalid level', 'level must be in [0, %d]' % maxLevel)
    
def verifyXy(x, y, level):
    m = tilePixels << level
    if x < 0 or x >= m or y < 0 or y >= m:
        raise ipe.InvalidParameterException('invalid xy', 'x, y must be in [0, %d)' % m)

def verifyLl(lon, lat):
    if lon < lonRange[0] or lon > lonRange[1]:
        raise ipe.InvalidParameterException('invalid lon', 'lon must be in ' + str(lonRange))
    if lat < latRange[0] or lat > latRange[1]:
        raise ipe.InvalidParameterException('invalid lat', 'lat must be in ' + str(latRange))

def xy2ll(x, y):
    return xy2llByLevel(x, y, maxLevel)

def xy2llByLevel(x, y, level):
    verifyLevel(level)
    verifyXy(x, y, level)

    fd = earthCircle / ((1 << level) * tilePixels)
    ia = x * fd - earthCircle / 2;
    ht = earthCircle / 2 - y * fd;
    lat = (math.pi / 2 - 2 * math.atan(math.exp(-ht / earthRadius))) * 180.0 / math.pi;
    lon = (ia / earthRadius) * 180.0 / math.pi
    
    return [lon, lat]
    
def ll2xy(lon, lat):
    return ll2xyByLevel(lon, lat, maxLevel)
    
def ll2xyByLevel(lon, lat, level):
    verifyLevel(level)
    verifyLl(lon, lat)

    rLon = min(max(lon, lonRange[0]), lonRange[1]) * math.pi / 180.0
    rLat = min(max(lat, latRange[0]), latRange[1]) * math.pi / 180.0
    
    xMeters = earthRadius * rLon
    yMeters = earthRadius / 2.0 * math.log((1 + math.sin(rLat)) / (1 - math.sin(rLat)))

    numPixels = long(tilePixels) << level
    metersPerPixel = earthCircle / numPixels
    
    x = long((earthCircle / 2.0 + xMeters) / metersPerPixel + 0.5)
    x = min(max(x, 0), numPixels - 1)

    y = long((earthCircle / 2.0 - yMeters) / metersPerPixel + 0.5)
    y = min(max(y, 0), numPixels - 1)
    
    return [int(x), int(y)]
    
def calcDistanceXy(x1, y1, x2, y2):
    if x1 == x2 and y1 == y2:
        return 0.0
    ll1 = xy2ll(x1, y1)
    ll2 = xy2ll(x2, y2)
    return calcDistanceLl(ll1[0], ll1[1], ll2[0], ll2[1])
    
def calcDistanceLl(lon1, lat1, lon2, lat2):
    verifyLl(lon1, lat1)
    verifyLl(lon2, lat2)
    
    rLon1 = lon1 * math.pi / 180.0
    rLat1 = lat1 * math.pi / 180.0
    rLon2 = lon2 * math.pi / 180.0
    rLat2 = lat2 * math.pi / 180.0

    r = math.sin(rLat1) * math.sin(rLat2) + \
        math.cos(rLat1) * math.cos(rLat2) * math.cos(rLon2 - rLon1)

    if (r > 1):
        r = 1
    elif (r < -1):
        r = -1

    return earthRadius * math.acos(r)
