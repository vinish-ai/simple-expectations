# Future Plan: Data Quality Engine (DQE)

## Timeseries & Statistical Anomaly Detection
- Implement Z-Score and MAD (Median Absolute Deviation) expectations to automatically detect data drift.
- Allow dynamic `lookback_days` configuration based on historical data partitions for dynamic baseline shifting.

## Additional Planned Enhancements
- Data freshness expectations (`expect_column_max_to_be_recent`)
- Schema evolution detection (`dqe schema-diff`)
- Plugin system for external custom expectations via `entry_points`
- Auto-generated expectation reference docs (`dqe docs`)
