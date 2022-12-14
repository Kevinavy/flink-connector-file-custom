package com.kevinavy.sink;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;

public class FileSinkFunction extends RichSinkFunction<RowData> {
    private final String filename;
    private final String suffix;
    private final String separator;
    private final String path;

    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    private final int batch = 2000;
    private final String PATH_TEMP = "file";
    private File file;
    private int count;
    private long current;

    public FileSinkFunction(String filename,
                            String path,
                            String separator,
                            String suffix) {
        this.filename = filename;
        this.path = path;
        this.suffix = suffix;
        this.separator = separator;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Files.createDirectories(Paths.get(path));
        Files.createDirectories(Paths.get(PATH_TEMP));
        this.file = new File(getFilename(PATH_TEMP, filename, suffix));
        this.current = System.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void finish() throws Exception {
        super.finish();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (count >= batch) {
            moveFileToTarget(PATH_TEMP + "/" + file.getName(),path + "/" + file.getName());
            this.file = new File(getFilename(PATH_TEMP, filename, suffix));
            if (!file.exists()) {
                if (file.createNewFile()) {
                    throw new RuntimeException("file create error");
                }
                this.count = 0;
                this.current = System.currentTimeMillis();
            }
        }
        else if (System.currentTimeMillis() - current > 60000) {
            moveFileToTarget(PATH_TEMP + "/" + file.getName(),path + "/" + file.getName());
            this.count = 0;
            this.current = System.currentTimeMillis();
        }

        GenericRowData rowData = (GenericRowData) value;
        RowKind rowKind = rowData.getRowKind();
        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fileWriter);
            StringBuilder content = new StringBuilder();
            for (int i = 0; i < rowData.getArity(); i++) {
                content.append(rowData.getField(i) == null ? "" : rowData.getField(i)).append(separator);
            }
            content.delete(content.lastIndexOf(separator), content.length());
            content.append("\n");
            bw.write(content.toString());
            bw.close();
        }
    }

    private String getFilename(String path, String filename, String suffix) {
        long currentTimeMillis = System.currentTimeMillis();
        return path + "/" + simpleDateFormat.format(currentTimeMillis) + "_" + filename + "_" + currentTimeMillis + "." + suffix;
    }

    private void moveFileToTarget(String fileFullNameCurrent, String fileFullNameTarget) {
        File oldName = new File(fileFullNameCurrent);
        if (!oldName.exists()) {
            return;
        }
        if (oldName.isDirectory()) {
            return;
        }
        File newName = new File(fileFullNameTarget);
        if (newName.isDirectory()) {
            return;
        }
        String pfile = newName.getParent();
        File pdir = new File(pfile);
        if (!pdir.exists()) {
            pdir.mkdirs();
        }
        oldName.renameTo(newName);
    }
}
