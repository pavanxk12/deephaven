#include "data/candle_aggregator.h"

namespace algo {

CandleAggregator::CandleAggregator(uint32_t capacity, int64_t start, int32_t bucket)
    : start_(start), bucket_(bucket),
      cur_(capacity, Open{0, 0, 0, 0, 0, 0, 0, 0, 0}),
      prev_cum_(capacity, 0), has_prev_(capacity, 0),
      seen_fp_(capacity, 0), seen_vol_(capacity, 0), seen_(capacity, 0),
      last_price_(capacity, 0), has_last_(capacity, 0), last_dir_(capacity, 0),
      cvd_(capacity, 0),
      open_(capacity, 0), pos_(capacity, 0) {}

}  // namespace algo
