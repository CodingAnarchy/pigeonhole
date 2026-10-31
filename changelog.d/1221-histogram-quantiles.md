### Added
- Histogram and quantile aggregate families (D298, ICR 0096), with parameters fixed at creation:
  - `Aggregation::Histogram(HistogramParams::new(bounds)?)` counts `f64` inputs in the buckets between its bounds, read with `CellRef::histogram_counts`.
  - `Aggregation::Quantiles(QuantileParams::default())` keeps a log-linear sketch, read with `CellRef::quantile(q)` within a relative error of 2^-p (`mantissa_bits`, 0.8% by default). Its smallest magnitudes fold together past `max_buckets`. The same inputs give the same sketch in any order.
  - Inputs are written with `aggregate_f64`. Histograms refuse NaN; quantiles refuse non-finite inputs.
  - `FamilyInfo::aggregation` carries the parameters back.
  - Like the other aggregate families, creating the first one marks the file, which pigeonhole 1.0.x then refuses to open (`UnsupportedFeature`).
- `phdb`: `"aggregation_params"` in the schema JSON (`info`, `dump`, `load`), and the shell's `create … name:histogram=b0,b1,…` and `name:quantiles[=p,n]`.

### Changed
- An aggregate family's values are never moved into blob files, whatever its `blob_threshold`: the built-in aggregations fold stored values, not blob pointers.
