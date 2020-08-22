from pyflink.common.serialization import SimpleStringEncoder
from pyflink.datastream.connectors import StreamingFileSink, DefaultRollingPolicy

input_ds = ...
sink = StreamingFileSink.for_row_format("output_path", SimpleStringEncoder("UTF-8")) \
    .with_rolling_policy(
    DefaultRollingPolicy.builder()
        .with_rollover_interval(15 * 60 * 1000)
        .with_inactivity_interval(5 * 60 * 1000)
        .with_max_part_size(1024 * 1024 * 1024).build()) \
    .build()

input_ds.add_sink(sink)
