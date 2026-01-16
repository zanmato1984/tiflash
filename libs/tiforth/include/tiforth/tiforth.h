#pragma once

#define TIFORTH_VERSION_MAJOR 0
#define TIFORTH_VERSION_MINOR 1
#define TIFORTH_VERSION_PATCH 0

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators.h"
#include "tiforth/pipeline.h"
#include "tiforth/spill.h"
#include "tiforth/task.h"

#include "tiforth/operators/filter.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/operators/projection.h"
#include "tiforth/operators/sort.h"
