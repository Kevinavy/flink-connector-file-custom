package com.kevinavy.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

public class FileDynamicTableSink implements DynamicTableSink {
    private final String filename;
    private final String suffix;
    private final String separator;
    private final String path;


    public FileDynamicTableSink(String filename,
                                String path,
                                String separator,
                                String suffix) {
        this.filename = filename;
        this.path = path;
        this.suffix = suffix;
        this.separator = separator;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.all();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SinkFunction<RowData> sinkFunction = new FileSinkFunction(
                filename,
                path,
                separator,
                suffix);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new FileDynamicTableSink(filename, path, suffix, separator);
    }

    @Override
    public String asSummaryString() {
        return "set up file connector";
    }
}
