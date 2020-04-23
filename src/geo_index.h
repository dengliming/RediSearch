#ifndef __GEO_INDEX_H__
#define __GEO_INDEX_H__

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"
#include "index_iterator.h"
#include "search_ctx.h"
#include "query_error.h"
#include "geo/geohash_helper.h"
#include "numeric_filter.h"
#include "numeric_index.h"

typedef struct GeoIndex {
  RedisModuleString *keyname;
  int isDeleted;
} GeoIndex;

#define RANGE_COUNT 9

GeoIndex *GeoIndex_Create(const char *ixname);
void GeoIndex_Free(GeoIndex *idx);
void GeoIndex_RemoveKey(RedisModuleCtx *ctx, GeoIndex *gi);
void GeoIndex_PrepareKey(RedisModuleCtx *ctx, GeoIndex *gi);

int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, const char *slon, const char *slat);

void GeoIndex_RemoveEntries(GeoIndex *gi, IndexSpec *sp, t_docId docId);

typedef enum {  // Placeholder for bad/invalid unit
  GEO_DISTANCE_INVALID = -1,
#define X_GEO_DISTANCE(X) \
  X(KM, "km")             \
  X(M, "m")               \
  X(FT, "ft")             \
  X(MI, "mi")

#define X(c, unused) GEO_DISTANCE_##c,
  X_GEO_DISTANCE(X)
#undef X
} GeoDistance;

typedef struct GeoFilter {
  const char *property;
  double lat;
  double lon;
  double radius;
  GeoDistance unitType;
  GeoHashFix52Bits ranges[RANGE_COUNT][2]
} GeoFilter;

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit);

GeoDistance GeoDistance_Parse(const char *s);
const char *GeoDistance_ToString(GeoDistance dist);

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(GeoFilter *f, QueryError *status);

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_Parse(GeoFilter *gf, ArgsCursor *ac, QueryError *status);
void GeoFilter_Free(GeoFilter *gf);
IndexIterator *NewGeoRangeIterator(GeoIndex *gi, const GeoFilter *gf, double weight);

int encodeGeo(double *xy, double *bits);
int decodeGeo(double bits, double *xy);
int isWithinRadius(double center, double point, double radius, double *distance);

#endif
