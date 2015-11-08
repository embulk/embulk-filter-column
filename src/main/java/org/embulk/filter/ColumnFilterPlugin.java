package org.embulk.filter;

import java.util.List;
import java.util.HashMap;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.type.Type;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;

import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

import org.joda.time.DateTimeZone;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampParseException;
import com.google.common.base.Throwables;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import com.google.common.base.Optional;
import org.embulk.spi.SchemaConfigException;

public class ColumnFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(ColumnFilterPlugin.class);

    public ColumnFilterPlugin()
    {
    }

    // NOTE: This is not spi.ColumnConfig
    private interface ColumnConfig extends Task
    {
        @Config("name")
        public String getName();

        @Config("type")
        @ConfigDefault("null")
        public Optional<Type> getType(); // required only for addColumns

        @Config("default")
        @ConfigDefault("null")
        public Optional<Object> getDefault();

        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();

        @Config("timezone")
        @ConfigDefault("null")
        public Optional<DateTimeZone> getTimeZone();

        @Config("src")
        @ConfigDefault("null")
        public Optional<String> getSrc();
    }

    public interface PluginTask extends Task, TimestampParser.Task
    {
        @Config("columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getColumns();

        @Config("add_columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getAddColumns();

        @Config("drop_columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getDropColumns();

        // See TimestampParser for default_timestamp_format, and default_timezone
    }

    @Override
    public void transaction(final ConfigSource config, final Schema inputSchema,
            final FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        List<ColumnConfig> columns = task.getColumns();
        List<ColumnConfig> addColumns = task.getAddColumns();
        List<ColumnConfig> dropColumns = task.getDropColumns();

        if (columns.size() == 0 && addColumns.size() == 0 && dropColumns.size() == 0) {
            throw new ConfigException("One of \"columns\", \"add_columns\", \"drop_columns\" must be specified.");
        }

        if (columns.size() > 0 && dropColumns.size() > 0) {
            throw new ConfigException("Either of \"columns\", \"drop_columns\" can be specified.");
        }

        // Automatically get column type from inputSchema for columns and dropColumns
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        if (dropColumns.size() > 0) {
            for (Column inputColumn: inputSchema.getColumns()) {
                String name = inputColumn.getName();
                boolean matched = false;
                for (ColumnConfig dropColumn : dropColumns) {
                    if (dropColumn.getName().equals(name)) {
                        matched = true;
                        break;
                    }
                }
                if (! matched) {
                    Column outputColumn = new Column(i++, name, inputColumn.getType());
                    builder.add(outputColumn);
                }
            }
        } else if (columns.size() > 0) {
            for (ColumnConfig column : columns) {
                String name                   = column.getName();
                Optional<Type>   type         = column.getType();
                Optional<Object> defaultValue = column.getDefault();
                Optional<String> src = column.getSrc();

                Column inputColumn = getColumn(name, inputSchema);
                if (inputColumn != null) { // filter column
                    Column outputColumn = new Column(i++, name, inputColumn.getType());
                    builder.add(outputColumn);
                }
                else if (type.isPresent() && defaultValue.isPresent()) { // add column
                    checkSrc(name, src, type, inputSchema);
                    Column outputColumn = new Column(i++, name, type.get());
                    builder.add(outputColumn);
                }
                else {
                    throw new SchemaConfigException(String.format("columns: Column '%s' is not found in inputSchema. Column '%s' does not have \"type\" and \"default\"", name, name));
                }
            }
        } else {
            for (Column inputColumn: inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
                builder.add(outputColumn);
            }
        }

        // Add columns to last. If you want to add to head or middle, you can use `columns` option
        if (addColumns.size() > 0) {
            for (ColumnConfig column : addColumns) {
                String name                   = column.getName();
                Optional<Type> type           = column.getType();
                Optional<Object> defaultValue = column.getDefault();
                Optional<String> src = column.getSrc();

                if (type.isPresent() && defaultValue.isPresent()) { // add column
                    checkSrc(name, src, type, inputSchema);
                    Column outputColumn = new Column(i++, name, type.get());
                    builder.add(outputColumn);
                }
                else {
                    throw new SchemaConfigException(String.format("add_columns: Column '%s' does not have \"type\" and \"default\"", name));
                }
            }
        }

        Schema outputSchema = new Schema(builder.build());

        control.run(task.dump(), outputSchema);
    }

    private void checkSrc(String name, Optional<String> src, Optional<Type> type,
                              final Schema inputSchema) {
        if (src.isPresent()) {
            boolean matched = false;
            String src_name = src.get();
            Type src_type = type.get();
            for (Column inputColumn: inputSchema.getColumns()) {
                if (src_name.equals(inputColumn.getName()) &&
                    src_type.equals(inputColumn.getType())) {
                    matched = true;
                    break;
                }
            }
            if (! matched) {
                throw new SchemaConfigException(String.format("add_columns: Column '%s' unmatch \"src column name\" or \"type\"", name));
            }
        }
    }

    private Column getColumn(String name, Schema schema) {
        // hash should be faster, though
        for (Column column: schema.getColumns()) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    private String getSrc(String name, List<ColumnConfig> columnConfigs) {
        for (ColumnConfig columnConfig : columnConfigs) {
            if (columnConfig.getName().equals(name) &&
                columnConfig.getSrc().isPresent()) {
                return (String)columnConfig.getSrc().get();
            }
        }
        return null;
    }

    private Object getDefault(String name, Type type, List<ColumnConfig> columnConfigs, PluginTask task) {
        for (ColumnConfig columnConfig : columnConfigs) {
            if (columnConfig.getName().equals(name)) {
                if (type instanceof BooleanType) {
                    if (columnConfig.getDefault().isPresent()) {
                        return (Boolean)columnConfig.getDefault().get();
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
                        return (String)columnConfig.getDefault().get();
                    }
                }
                else if (type instanceof TimestampType) {
                    if (columnConfig.getDefault().isPresent()) {
                        String time   = (String)columnConfig.getDefault().get();
                        String format = null;
                        if (columnConfig.getFormat().isPresent()) {
                            format = columnConfig.getFormat().get();
                        } else {
                            format = task.getDefaultTimestampFormat();
                        }
                        DateTimeZone timezone = null;
                        if (columnConfig.getTimeZone().isPresent()) {
                            timezone = columnConfig.getTimeZone().get();
                        } else {
                            timezone = task.getDefaultTimeZone();
                        }
                        TimestampParser parser = new TimestampParser(task.getJRuby(), format, timezone);
                        try {
                            Timestamp default_value = parser.parse(time);
                            return default_value;
                        } catch(TimestampParseException ex) {
                            throw Throwables.propagate(ex);
                        }
                    }
                }
                return null;
            }
        }
        return null;
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Map outputColumn => inputColumn
        final HashMap<Column, Column> outputInputColumnMap = new HashMap<Column, Column>();
        for (Column outputColumn: outputSchema.getColumns()) {
            Column inputColumn = getColumn(outputColumn.getName(), inputSchema);
            outputInputColumnMap.put(outputColumn, inputColumn); // NOTE: inputColumn would be null
        }

        // Map outputColumn => src and default value if present
        final HashMap<Column, String> outputSrcMap = new HashMap<Column, String>();
        final HashMap<Column, Object> outputDefaultMap = new HashMap<Column, Object>();

        for (Column outputColumn: outputSchema.getColumns()) {
            String name = outputColumn.getName();
            Type   type = outputColumn.getType();

            String src_name = getSrc(name, task.getColumns());
            if (src_name == null){
                src_name = getSrc(name, task.getAddColumns());
            }
            if (src_name != null) {
                outputSrcMap.put(outputColumn, src_name);
            }

            Object default_value = getDefault(name, type, task.getColumns(), task);
            if (default_value == null) {
                default_value = getDefault(name, type, task.getAddColumns(), task);
            }
            if (default_value != null) {
                outputDefaultMap.put(outputColumn, default_value);
            }
        }

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);

            @Override
            public void finish() {
                pageBuilder.finish();
            }

            @Override
            public void close() {
                pageBuilder.close();
            }

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }

            class ColumnVisitorImpl implements ColumnVisitor {
                private final PageBuilder pageBuilder;

                ColumnVisitorImpl(PageBuilder pageBuilder) {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column outputColumn) {
                    Column inputColumn = outputInputColumnMap.get(outputColumn);
                    if (inputColumn == null || pageReader.isNull(inputColumn)) {
                        Boolean src_value = null;
                        String src_name = (String)outputSrcMap.get(outputColumn);
                        if (src_name != null &&
                            !pageReader.isNull(inputSchema.lookupColumn(src_name))) {
                            src_value = (Boolean)pageReader.getBoolean(inputSchema.lookupColumn(src_name));
                        }
                        Boolean default_value = (Boolean)outputDefaultMap.get(outputColumn);
                        if (src_value != null) {
                            pageBuilder.setBoolean(outputColumn, src_value);
                        } else if (default_value != null) {
                            pageBuilder.setBoolean(outputColumn, default_value.booleanValue());
                        } else {
                            pageBuilder.setNull(outputColumn);
                        }
                    } else {
                        pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
                    }
                }

                @Override
                public void longColumn(Column outputColumn) {
                    Column inputColumn = outputInputColumnMap.get(outputColumn);
                    if (inputColumn == null || pageReader.isNull(inputColumn)) {
                        Long src_value = null;
                        String src_name = (String)outputSrcMap.get(outputColumn);
                        if (src_name != null &&
                            !pageReader.isNull(inputSchema.lookupColumn(src_name))) {
                            src_value = (Long)pageReader.getLong(inputSchema.lookupColumn(src_name));
                        }
                        Long default_value = (Long)outputDefaultMap.get(outputColumn);
                        if (src_value != null) {
                            pageBuilder.setLong(outputColumn, src_value);
                        } else if (default_value != null) {
                            pageBuilder.setLong(outputColumn, default_value.longValue());
                        } else {
                            pageBuilder.setNull(outputColumn);
                        }
                    } else {
                        pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column outputColumn) {
                    Column inputColumn = outputInputColumnMap.get(outputColumn);
                    if (inputColumn == null || pageReader.isNull(inputColumn)) {
                        Double src_value = null;
                        String src_name = (String)outputSrcMap.get(outputColumn);
                        if (src_name != null &&
                            !pageReader.isNull(inputSchema.lookupColumn(src_name))) {
                            src_value = (Double)pageReader.getDouble(inputSchema.lookupColumn(src_name));
                        }
                        Double default_value = (Double)outputDefaultMap.get(outputColumn);
                        if (src_value != null) {
                            pageBuilder.setDouble(outputColumn, src_value);
                        } else if (default_value != null) {
                            pageBuilder.setDouble(outputColumn, default_value.doubleValue());
                        } else {
                            pageBuilder.setNull(outputColumn);
                        }
                    } else {
                        pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
                    }
                }

                @Override
                public void stringColumn(Column outputColumn) {
                    Column inputColumn = outputInputColumnMap.get(outputColumn);
                    if (inputColumn == null || pageReader.isNull(inputColumn)) {
                        String src_value = null;
                        String src_name = (String)outputSrcMap.get(outputColumn);
                        if (src_name != null &&
                            !pageReader.isNull(inputSchema.lookupColumn(src_name))) {
                            src_value = (String)pageReader.getString(inputSchema.lookupColumn(src_name));
                        }
                        String default_value = (String)outputDefaultMap.get(outputColumn);
                        if (src_value != null) {
                            pageBuilder.setString(outputColumn, src_value);
                        } else if (default_value != null) {
                            pageBuilder.setString(outputColumn, default_value);
                        } else {
                            pageBuilder.setNull(outputColumn);
                        }
                    } else {
                        pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column outputColumn) {
                    Column inputColumn = outputInputColumnMap.get(outputColumn);
                    if (inputColumn == null || pageReader.isNull(inputColumn)) {
                        Timestamp src_value = null;
                        String src_name = (String)outputSrcMap.get(outputColumn);
                        if (src_name != null &&
                            !pageReader.isNull(inputSchema.lookupColumn(src_name))) {
                            src_value = (Timestamp)pageReader.getTimestamp(inputSchema.lookupColumn(src_name));
                        }
                        Timestamp default_value = (Timestamp)outputDefaultMap.get(outputColumn);
                        if (src_value != null) {
                            pageBuilder.setTimestamp(outputColumn, src_value);
                        } else if (default_value != null) {
                            pageBuilder.setTimestamp(outputColumn, default_value);
                        } else {
                            pageBuilder.setNull(outputColumn);
                        }
                    } else {
                        pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
                    }
                }
            }
        };
    }
}
