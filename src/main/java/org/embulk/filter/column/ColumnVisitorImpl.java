package org.embulk.filter.column;

import org.embulk.filter.column.ColumnFilterPlugin.ColumnConfig;
import org.embulk.filter.column.ColumnFilterPlugin.PluginTask;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;

import com.google.common.base.Throwables;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;

public class ColumnVisitorImpl implements ColumnVisitor
{
    private static final Logger logger = Exec.getLogger(ColumnFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<Column, Column> outputInputColumnMap = new HashMap<>();
    private final HashMap<Column, Object> outputDefaultMap = new HashMap<>();
    private final JsonVisitor jsonVisitor;

    ColumnVisitorImpl(PluginTask task, Schema inputSchema, Schema outputSchema, PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task = task;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
        buildOutputInputColumnMap();
        buildOutputDefaultMap();
        this.jsonVisitor = new JsonVisitor(task, inputSchema, outputSchema);
    }

    // Map outputColumn => inputColumn
    private void buildOutputInputColumnMap()
    {
        for (Column outputColumn : outputSchema.getColumns()) {
            String name    = outputColumn.getName();
            String srcName = getSrc(name, task.getColumns());
            if (srcName == null) {
                srcName = getSrc(name, task.getAddColumns());
            }
            if (srcName == null) {
                srcName = name;
            }
            Column inputColumn;
            try {
                inputColumn = inputSchema.lookupColumn(srcName);
            }
            catch (SchemaConfigException ex) {
                inputColumn = null;
            }
            outputInputColumnMap.put(outputColumn, inputColumn); // NOTE: inputColumn would be null
        }
    }


    // Map outputColumn => default value if present
    private void buildOutputDefaultMap()
    {
        for (Column outputColumn : outputSchema.getColumns()) {
            String name = outputColumn.getName();
            Type type = outputColumn.getType();

            Object defaultValue = getDefault(task, name, type, task.getColumns());
            if (defaultValue == null) {
                defaultValue = getDefault(task, name, type, task.getAddColumns());
            }
            if (defaultValue != null) {
                outputDefaultMap.put(outputColumn, defaultValue);
            }
        }
    }

    static String getSrc(String name, List<ColumnConfig> columnConfigs)
    {
        for (ColumnConfig columnConfig : columnConfigs) {
            if (columnConfig.getName().equals(name) &&
                    columnConfig.getSrc().isPresent()) {
                return columnConfig.getSrc().get();
            }
        }
        return null;
    }

    static Object getDefault(PluginTask task, String name, Type type, List<ColumnConfig> columnConfigs)
    {
        for (ColumnConfig columnConfig : columnConfigs) {
            if (columnConfig.getName().equals(name)) {
                return getDefault(task, name, type, columnConfig);
            }
        }
        return null;
    }

    static Object getDefault(PluginTask task, String name, Type type, ColumnConfig columnConfig)
    {
        if (type instanceof BooleanType) {
            if (columnConfig.getDefault().isPresent()) {
                return (Boolean) columnConfig.getDefault().get();
            }
        }
        else if (type instanceof LongType) {
            if (columnConfig.getDefault().isPresent()) {
                return new Long(columnConfig.getDefault().get().toString());
            }
        }
        else if (type instanceof DoubleType) {
            if (columnConfig.getDefault().isPresent()) {
                return new Double(columnConfig.getDefault().get().toString());
            }
        }
        else if (type instanceof StringType) {
            if (columnConfig.getDefault().isPresent()) {
                return columnConfig.getDefault().get();
            }
        }
        else if (type instanceof JsonType) {
            if (columnConfig.getDefault().isPresent()) {
                JsonParser parser = new JsonParser();
                return parser.parse((String) columnConfig.getDefault().get());
            }
        }
        else if (type instanceof TimestampType) {
            if (columnConfig.getDefault().isPresent()) {
                String time   = (String) columnConfig.getDefault().get();
                String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
                DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
                TimestampParser parser = new TimestampParser(task.getJRuby(), format, timezone);
                try {
                    Timestamp defaultValue = parser.parse(time);
                    return defaultValue;
                }
                catch (TimestampParseException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }
        return null;
    }

    @Override
    public void booleanColumn(Column outputColumn)
    {
        Column inputColumn = outputInputColumnMap.get(outputColumn);
        if (inputColumn == null || pageReader.isNull(inputColumn)) {
            Boolean defaultValue = (Boolean) outputDefaultMap.get(outputColumn);
            if (defaultValue == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setBoolean(outputColumn, defaultValue.booleanValue());
            }
        }
        else {
            pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
        }
    }

    @Override
    public void longColumn(Column outputColumn)
    {
        Column inputColumn = outputInputColumnMap.get(outputColumn);
        if (inputColumn == null || pageReader.isNull(inputColumn)) {
            Long defaultValue = (Long) outputDefaultMap.get(outputColumn);
            if (defaultValue == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setLong(outputColumn, defaultValue.longValue());
            }
        }
        else {
            pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
        }
    }

    @Override
    public void doubleColumn(Column outputColumn)
    {
        Column inputColumn = outputInputColumnMap.get(outputColumn);
        if (inputColumn == null || pageReader.isNull(inputColumn)) {
            Double defaultValue = (Double) outputDefaultMap.get(outputColumn);
            if (defaultValue == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setDouble(outputColumn, defaultValue.doubleValue());
            }
        }
        else {
            pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
        }
    }

    @Override
    public void stringColumn(Column outputColumn)
    {
        Column inputColumn = outputInputColumnMap.get(outputColumn);
        if (inputColumn == null || pageReader.isNull(inputColumn)) {
            String defaultValue = (String) outputDefaultMap.get(outputColumn);
            if (defaultValue == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setString(outputColumn, defaultValue);
            }
        }
        else {
            pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
        }
    }

    @Override
    public void jsonColumn(Column outputColumn)
    {
        Column inputColumn = outputInputColumnMap.get(outputColumn);
        if (inputColumn == null || pageReader.isNull(inputColumn)) {
            Value defaultValue = (Value) outputDefaultMap.get(outputColumn);
            if (defaultValue == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setJson(outputColumn, defaultValue);
            }
        }
        else {
            Value value = pageReader.getJson(inputColumn) ;
            String jsonPath = new StringBuilder("$.").append(inputColumn.getName()).toString();
            pageBuilder.setJson(outputColumn, jsonVisitor.visit(jsonPath, value));
        }
    }

    @Override
    public void timestampColumn(Column outputColumn)
    {
        Column inputColumn = outputInputColumnMap.get(outputColumn);
        if (inputColumn == null || pageReader.isNull(inputColumn)) {
            Timestamp defaultValue = (Timestamp) outputDefaultMap.get(outputColumn);
            if (defaultValue == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setTimestamp(outputColumn, defaultValue);
            }
        }
        else {
            pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
        }
    }

}
