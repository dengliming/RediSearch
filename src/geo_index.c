#include "index.h"
#include "geo_index.h"
#include "rmutil/util.h"
#include "rmalloc.h"
#include "module.h"
#include "rmutil/rm_assert.h"
#include "geo/geohash_helper.h"
#include "numeric_index.h"

GeoIndex *GeoIndex_Create(const char *ixname) {
  GeoIndex *gi = rm_calloc(1, sizeof(*gi));
  gi->keyname = RedisModule_CreateStringPrintf(RSDummyContext, "_geoidx:%s", ixname);
  return gi;
}

void GeoIndex_PrepareKey(RedisModuleCtx *ctx, GeoIndex *gi) {
  GeoIndex_RemoveKey(ctx, gi);
  gi->isDeleted = 0;
}

void GeoIndex_RemoveKey(RedisModuleCtx *ctx, GeoIndex *gi) {
  gi->isDeleted = 1;
  RedisModuleKey *k = RedisModule_OpenKey(ctx, gi->keyname, REDISMODULE_READ | REDISMODULE_WRITE);
  if (!k) {
    return;
  }
  RedisModule_DeleteKey(k);
  RedisModule_CloseKey(k);
}

void GeoIndex_Free(GeoIndex *gi) {
  if (gi->keyname) {
    RedisModule_FreeString(RSDummyContext, gi->keyname);
  }
  rm_free(gi);
}

/* Add a docId to a geoindex key. Right now we just use redis' own GEOADD */
int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, const char *slon, const char *slat) {
  if (gi->isDeleted) {
    return REDISMODULE_ERR;
  }

  /* GEOADD key longitude latitude member*/
  RedisModuleCallReply *rep =
      RedisModule_Call(RSDummyContext, "GEOADD", "sccl", gi->keyname, slon, slat, docId);
  if (rep == NULL) {
    return REDISMODULE_ERR;
  }

  int repType = RedisModule_CallReplyType(rep);
  RedisModule_FreeCallReply(rep);
  if (repType == REDISMODULE_REPLY_ERROR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void GeoIndex_RemoveEntries(GeoIndex *gi, IndexSpec *sp, t_docId docId) {
  if (gi->isDeleted) {
    return;
  }
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "ZREM", "sl", gi->keyname, docId);

  if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR) {
    RedisModule_Log(ctx, "warning", "Document %s was not removed", docId);
  }
  RedisModule_FreeCallReply(rep);
}

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0], and FILTER
 * is not passed to us.
 * The GEO filter syntax is (FILTER) <property> LONG LAT DIST m|km|ft|mi
 * Returns REDISMODUEL_OK or ERR  */
int GeoFilter_Parse(GeoFilter *gf, ArgsCursor *ac, QueryError *status) {
  gf->lat = 0;
  gf->lon = 0;
  gf->radius = 0;
  gf->unitType = GEO_DISTANCE_KM;

  if (AC_NumRemaining(ac) < 5) {
    QERR_MKBADARGS_FMT(status, "GEOFILTER requires 5 arguments");
    return REDISMODULE_ERR;
  }

  int rv;
  if ((rv = AC_GetString(ac, &gf->property, NULL, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<geo property>", rv);
    return REDISMODULE_ERR;
  } else {
    gf->property = rm_strdup(gf->property);
  }
  if ((rv = AC_GetDouble(ac, &gf->lon, 0) != AC_OK)) {
    QERR_MKBADARGS_AC(status, "<lon>", rv);
    return REDISMODULE_ERR;
  }

  if ((rv = AC_GetDouble(ac, &gf->lat, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<lat>", rv);
    return REDISMODULE_ERR;
  }

  if ((rv = AC_GetDouble(ac, &gf->radius, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<radius>", rv);
    return REDISMODULE_ERR;
  }

  const char *unitstr = AC_GetStringNC(ac, NULL);
  if ((gf->unitType = GeoDistance_Parse(unitstr)) == GEO_DISTANCE_INVALID) {
    QERR_MKBADARGS_FMT(status, "Unknown distance unit %s", unitstr);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void GeoFilter_Free(GeoFilter *gf) {
  if (gf->property) rm_free((char *)gf->property);
  rm_free(gf);
}

static t_docId *geoRangeLoad(const GeoIndex *gi, const GeoFilter *gf, size_t *num) {
  if (gi->isDeleted) {
    return NULL;
  }

  *num = 0;
  t_docId *docIds = NULL;
  /*GEORADIUS key longitude latitude radius m|km|ft|mi */
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModuleString *slon = RedisModule_CreateStringPrintf(ctx, "%f", gf->lon);
  RedisModuleString *slat = RedisModule_CreateStringPrintf(ctx, "%f", gf->lat);
  RedisModuleString *srad = RedisModule_CreateStringPrintf(ctx, "%f", gf->radius);
  const char *unitstr = GeoDistance_ToString(gf->unitType);
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "GEORADIUS", "sssscc", gi->keyname, slon, slat, srad, unitstr, "ASC");
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    goto done;
  }

  size_t sz = RedisModule_CallReplyLength(rep);
  docIds = rm_calloc(sz, sizeof(t_docId));
  for (size_t i = 0; i < sz; i++) {
    const char *s = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, i), NULL);
    if (!s) continue;

    docIds[i] = (t_docId)atol(s);
  }

  *num = sz;

done:
  RedisModule_FreeString(ctx, slon);
  RedisModule_FreeString(ctx, slat);
  RedisModule_FreeString(ctx, srad);
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  return docIds;
}

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, const GeoFilter *gf, double weight) {
  size_t sz;
  t_docId *docIds = geoRangeLoad(gi, gf, &sz);
  if (!docIds) {
    return NULL;
  }

  IndexIterator *ret = NewIdListIterator(docIds, (t_offset)sz, weight);
  rm_free(docIds);
  return ret;
}

GeoDistance GeoDistance_Parse(const char *s) {
#define X(c, val)            \
  if (!strcasecmp(val, s)) { \
    return GEO_DISTANCE_##c; \
  }
  X_GEO_DISTANCE(X)
#undef X
  return GEO_DISTANCE_INVALID;
}

const char *GeoDistance_ToString(GeoDistance d) {
#define X(c, val)              \
  if (d == GEO_DISTANCE_##c) { \
    return val;                \
  }
  X_GEO_DISTANCE(X)
#undef X
  return "<badunit>";
}

double extractUnitFactor(GeoDistance unit) {
  double rv;
  switch (unit) {
    case GEO_DISTANCE_M:
      rv = 1;
      break;
    case GEO_DISTANCE_KM:
      rv = 1000;
      break;
    case GEO_DISTANCE_FT:
      rv = 0.3048;
      break;
    case GEO_DISTANCE_MI:
      rv = 1609.34;
      break;  
    default:
      rv = -1;
      assert(0);
      break;
  }
  return rv;
}

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit) {
  GeoFilter *gf = rm_malloc(sizeof(*gf));
  *gf = (GeoFilter){
      .lon = lon,
      .lat = lat,
      .radius = radius,
      .ranges = { 0 },
  };
  if (unit) {
    gf->unitType = GeoDistance_Parse(unit);
  } else {
    gf->unitType = GEO_DISTANCE_KM;
  }
  return gf;
}

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(GeoFilter *gf, QueryError *status) {
  if (gf->unitType == GEO_DISTANCE_INVALID) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter unit");
    return 0;
  }

  // validate lat/lon
  if (gf->lat > 90 || gf->lat < -90 || gf->lon > 180 || gf->lon < -180) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter lat/lon");
    return 0;
  }

  // validate radius
  if (gf->radius <= 0) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter radius");
    return 0;
  }

  return 1;
}

int encodeGeo(double *xy, double *bits) {
    GeoHashBits hash = { .bits = (uint64_t)*bits, .step = GEO_STEP_MAX };
    int rv = geohashEncodeWGS84(xy[0], xy[1], GEO_STEP_MAX, &hash);
    *bits = (double)geohashAlign52Bits(hash);
    return rv;
}

int decodeGeo(double bits, double *xy) {
    GeoHashBits hash = { .bits = (uint64_t)bits, .step = GEO_STEP_MAX };
    return geohashDecodeToLongLatWGS84(hash, xy);
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
static void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
    /* We want to compute the sorted set scores that will include all the
     * elements inside the specified Geohash 'hash', which has as many
     * bits as specified by hash.step * 2.
     *
     * So if step is, for example, 3, and the hash value in binary
     * is 101010, since our score is 52 bits we want every element which
     * is in binary: 101010?????????????????????????????????????????????
     * Where ? can be 0 or 1.
     *
     * To get the min score we just use the initial hash value left
     * shifted enough to get the 52 bit value. Later we increment the
     * 6 bit prefis (see the hash.bits++ statement), and get the new
     * prefix: 101011, which we align again to 52 bits to get the maximum
     * value (which is excluded from the search). So we get everything
     * between the two following scores (represented in binary):
     *
     * 1010100000000000000000000000000000000000000000000000 (included)
     * and
     * 1010110000000000000000000000000000000000000000000000 (excluded).
     */
    *min = geohashAlign52Bits(hash);
    hash.bits++;
    *max = geohashAlign52Bits(hash);
}

/* Search all eight neighbors + self geohash box */
static void calcAllNeighbors(GeoHashRadius n, double lon, double lat,
                                  double radius, GeoFilter *gf) {
  GeoHashBits neighbors[RANGE_COUNT];
  unsigned int i, last_processed = 0;

  neighbors[0] = n.hash;
  neighbors[1] = n.neighbors.north;
  neighbors[2] = n.neighbors.south;
  neighbors[3] = n.neighbors.east;
  neighbors[4] = n.neighbors.west;
  neighbors[5] = n.neighbors.north_east;
  neighbors[6] = n.neighbors.north_west;
  neighbors[7] = n.neighbors.south_east;
  neighbors[8] = n.neighbors.south_west;

  /* For each neighbor (*and* our own hashbox), get all the matching
    * members and add them to the potential result list. */
  for (i = 0; i < RANGE_COUNT; i++) {
    if (HASHISZERO(neighbors[i])) {
      continue;
    }

    /* When a huge Radius (in the 5000 km range or more) is used,
      * adjacent neighbors can be the same, leading to duplicated
      * elements. Skip every range which is the same as the one
      * processed previously. */
    if (last_processed &&
      neighbors[i].bits == neighbors[last_processed].bits &&
      neighbors[i].step == neighbors[last_processed].step) {
      continue;
    }

    scoresOfGeoHashBox(neighbors[i], &gf->ranges[i][0], &gf->ranges[i][1]);
    last_processed = i;
  }
}

/* Calculate range for relevant squares around center.
 * If min == max, range is included in other ranges */
static int calcRanges(const GeoFilter *gf) {
  double xy[2] = {gf->lon, gf->lat};
  
  double radius_meters = gf->radius * extractUnitFactor(gf->unitType);
  if (radius_meters < 0) {
    return -1;
  }

  GeoHashRadius georadius =
    geohashGetAreasByRadiusWGS84(xy[0], xy[1], radius_meters);
  
  calcAllNeighbors(georadius, xy[0], xy[1], radius_meters, gf);

  return 0;
}

// might make sense to decode outside and pass actual values...
int isWithinRadius(double center, double point, double radius, double *distance) {
  double xyCenter[2], xyPoint[2];
  decodeGeo(center, xyCenter);
  decodeGeo(point, xyPoint);
  *distance = geohashGetDistance(xyCenter[0], xyCenter[1], xyPoint[0], xyPoint[1]);
  if (*distance > radius) return 0;
  return 1;
}

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, const GeoFilter *gf, double weight) {
  calcRanges(gf);
  IndexIterator **iters = rm_calloc(RANGE_COUNT, sizeof(*iters));
  for (size_t ii = 0; ii < RANGE_COUNT; ++ii) {
    NumericFilter *filt = NewNumericFilter((double)gf->ranges[ii][0], 
                                           (double)gf->ranges[ii][1], 1, 1);
    iters[ii] = NewNumericFilterIterator(NULL, &filt, NULL);
  }
  IndexIterator *ret = NewUnionIterator(iters, RANGE_COUNT, NULL, 1, 1);
}