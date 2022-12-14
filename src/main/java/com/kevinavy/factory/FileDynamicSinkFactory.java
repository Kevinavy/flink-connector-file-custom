package com.kevinavy.factory;

import com.kevinavy.sink.FileDynamicTableSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class FileDynamicSinkFactory implements DynamicTableSinkFactory {
    public static final ConfigOption<String> FILENAME = ConfigOptions.key("filename")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PATH = ConfigOptions.key("path")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> SUFFIX = ConfigOptions.key("suffix")
            .stringType()
            .defaultValue("bcp");

    public static final ConfigOption<String> SEPARATOR = ConfigOptions.key("separator")
            .stringType()
            .defaultValue("\t");



    @Override
    public String factoryIdentifier() {
        return "file";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FILENAME);
        options.add(PATH);
//        options.add(FactoryUtil.FORMAT); // 解码的格式器使用预先定义的配置项
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SUFFIX);
        options.add(SEPARATOR);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // 使用提供的工具类或实现你自己的逻辑进行校验
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // 找到合适的编码器
//        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
//                SerializationFormatFactory.class,
//                FactoryUtil.FORMAT);

        // 校验所有的配置项
        helper.validate();

        // 获取校验完的配置项
        final ReadableConfig options = helper.getOptions();
        final String filename = options.get(FILENAME);
        final String path = options.get(PATH);
        final String suffix = options.get(SUFFIX);
        final String separator = options.get(SEPARATOR);

//        // 从 catalog 中抽取要生产的数据类型 (除了需要计算的列)
//        final DataType producedDataType =
//                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // 创建并返回动态表 source
        return new FileDynamicTableSink(filename, path, separator, suffix);
    }
}
