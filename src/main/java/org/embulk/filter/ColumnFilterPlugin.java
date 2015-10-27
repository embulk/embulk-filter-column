package org.embulk.filter;

import java.util.List;
import java.util.HashMap;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
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
import org.embulk.filter.column.ColumnConfig; // note: different with spi.ColumnConfig

import org.joda.time.DateTimeZone;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampParseException;
import com.google.common.base.Throwables;

public class ColumnFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(ColumnFilterPlugin.class);

    public ColumnFilterPlugin()
    {
    }

    public interface PluginTask extends Task, TimestampParser.Task
    {
        @Config("columns")
        public List<ColumnConfig> getColumns();
    }

    @Override
    public void transaction(final ConfigSource config, final Schema inputSchema,
            final FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // Automatically get column type from inputSchema
        List<ColumnConfig> columnConfigs = task.getColumns();
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (ColumnConfig columnConfig : columnConfigs) {
            String columnName = columnConfig.getName();
            for (Column inputColumn: inputSchema.getColumns()) {
                if (inputColumn.getName().equals(columnName)) {
                    Column outputColumn = new Column(i++, columnName, inputColumn.getType());
                    builder.add(outputColumn);
                    break;
                }
            }
        }
        Schema outputSchema = new Schema(builder.build());

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Map outputColumn => inputColumn
        final HashMap<Column, Column> outputInputColumnMap = new HashMap<Column, Column>();
        for (Column outputColumn: outputSchema.getColumns()) {
            for (Column inputColumn: inputSchema.getColumns()) {
                if (inputColumn.getName().equals(outputColumn.getName())) {
                    outputInputColumnMap.put(outputColumn, inputColumn);
                    break;
                }
            }
        }

        // Map outputColumn => default value if present
        final HashMap<Column, Object> outputDefaultMap = new HashMap<Column, Object>();
        for (Column outputColumn: outputSchema.getColumns()) {
            Type columnType = outputColumn.getType();

            for (ColumnConfig columnConfig : task.getColumns()) {
                if (columnConfig.getName().equals(outputColumn.getName())) {

                    if (columnType instanceof BooleanType) {
                        if (columnConfig.getDefault().isPresent()) {
                            Boolean default_value = (Boolean)columnConfig.getDefault().get();
                            outputDefaultMap.put(outputColumn, default_value);
                        }
                    }
                    else if (columnType instanceof LongType) {
                        if (columnConfig.getDefault().isPresent()) {
                            Long default_value = new Long(columnConfig.getDefault().get().toString());
                            outputDefaultMap.put(outputColumn, default_value);
                        }
                    }
                    else if (columnType instanceof DoubleType) {
                        if (columnConfig.getDefault().isPresent()) {
                            Double default_value = new Double(columnConfig.getDefault().get().toString());
                            outputDefaultMap.put(outputColumn, default_value);
                        }
                    }
                    else if (columnType instanceof StringType) {
                        if (columnConfig.getDefault().isPresent()) {
                            String default_value = (String)columnConfig.getDefault().get();
                            outputDefaultMap.put(outputColumn, default_value);
                        }
                    }
                    else if (columnType instanceof TimestampType) {
                        if (columnConfig.getDefault().isPresent()) {
                            String time            = (String)columnConfig.getDefault().get();
                            String format          = (String)columnConfig.getFormat().get();
                            DateTimeZone timezone  = DateTimeZone.forID((String)columnConfig.getTimezone().get());
                            TimestampParser parser = new TimestampParser(task.getJRuby(), format, timezone);
                            try {
                                Timestamp default_value = parser.parse(time);
                                outputDefaultMap.put(outputColumn, default_value);
                            } catch(TimestampParseException ex) {
                                throw Throwables.propagate(ex);
                            }
                        }
                    }
                }
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
                    if (pageReader.isNull(inputColumn)) {
                        Boolean default_value = (Boolean)outputDefaultMap.get(outputColumn);
                        if (default_value != null) {
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
                    if (pageReader.isNull(inputColumn)) {
                        Long default_value = (Long)outputDefaultMap.get(outputColumn);
                        if (default_value != null) {
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
                    if (pageReader.isNull(inputColumn)) {
                        Double default_value = (Double)outputDefaultMap.get(outputColumn);
                        if (default_value != null) {
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
                    if (pageReader.isNull(inputColumn)) {
                        String default_value = (String)outputDefaultMap.get(outputColumn);
                        if (default_value != null) {
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
                    if (pageReader.isNull(inputColumn)) {
                        Timestamp default_value = (Timestamp)outputDefaultMap.get(outputColumn);
                        if (default_value != null) {
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
