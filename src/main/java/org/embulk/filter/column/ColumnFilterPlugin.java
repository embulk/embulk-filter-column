package org.embulk.filter.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.List;

public class ColumnFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(ColumnFilterPlugin.class);

    public ColumnFilterPlugin()
    {
    }

    // NOTE: This is not spi.ColumnConfig
    interface ColumnConfig extends Task, TimestampParser.TimestampColumnOption
    {
        @Config("name")
        public String getName();

        @Config("type")
        @ConfigDefault("null")
        public Optional<Type> getType(); // required only for addColumns

        @Config("default")
        @ConfigDefault("null")
        public Optional<Object> getDefault();

        @Config("src")
        @ConfigDefault("null")
        public Optional<String> getSrc();
    }

    interface PluginTask extends Task, TimestampParser.Task
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

        configure(task);
        Schema outputSchema = buildOutputSchema(task, inputSchema);

        control.run(task.dump(), outputSchema);
    }

    private void configure(PluginTask task)
    {
        if (task.getColumns().size() > 0 && task.getDropColumns().size() > 0) {
            throw new ConfigException("Either of \"columns\", \"drop_columns\" can be specified.");
        }
    }

    static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        List<ColumnConfig> columns = task.getColumns();
        List<ColumnConfig> addColumns = task.getAddColumns();
        List<ColumnConfig> dropColumns = task.getDropColumns();

        // Automatically get column type from inputSchema for columns and dropColumns
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        if (dropColumns.size() > 0) {
            for (Column inputColumn : inputSchema.getColumns()) {
                String name = inputColumn.getName();
                boolean matched = false;
                for (ColumnConfig dropColumn : dropColumns) {
                    // skip json path notation to build outputSchema
                    if (PathCompiler.isProbablyJsonPath(dropColumn.getName())) {
                        continue;
                    }
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
        }
        else if (columns.size() > 0) {
            for (ColumnConfig column : columns) {
                // skip json path notation to build output schema
                if (PathCompiler.isProbablyJsonPath(column.getName())) {
                    continue;
                }
                if (column.getSrc().isPresent() && PathCompiler.isProbablyJsonPath(column.getSrc().get())) {
                    continue;
                }

                String name                   = column.getName();
                Optional<Type>   type         = column.getType();
                Optional<Object> defaultValue = column.getDefault();
                Optional<String> src          = column.getSrc();

                String srcName = src.isPresent() ? src.get() : name;
                Column inputColumn;
                try {
                    inputColumn = inputSchema.lookupColumn(srcName);
                }
                catch (SchemaConfigException ex) {
                    inputColumn = null;
                }
                if (inputColumn != null) { // filter or copy column
                    Column outputColumn = new Column(i++, name, inputColumn.getType());
                    builder.add(outputColumn);
                }
                else if (type.isPresent() && defaultValue.isPresent()) { // add column
                    Column outputColumn = new Column(i++, name, type.get());
                    builder.add(outputColumn);
                }
                else {
                    throw new SchemaConfigException(String.format("columns: Column src '%s' is not found in inputSchema. Column '%s' does not have \"type\" and \"default\"", srcName, name));
                }
            }
        }
        else {
            for (Column column : inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, column.getName(), column.getType());
                builder.add(outputColumn);
            }
        }

        // Add columns to last. If you want to add to head or middle, you can use `columns` option
        if (addColumns.size() > 0) {
            for (ColumnConfig column : addColumns) {
                // skip json path notation to build output schema
                if (PathCompiler.isProbablyJsonPath(column.getName())) {
                    continue;
                }
                if (column.getSrc().isPresent() && PathCompiler.isProbablyJsonPath(column.getSrc().get())) {
                    continue;
                }

                String name                   = column.getName();
                Optional<Type> type           = column.getType();
                Optional<Object> defaultValue = column.getDefault();
                Optional<String> src          = column.getSrc();

                String srcName = null;
                Column inputColumn = null;
                if (src.isPresent()) {
                    srcName = src.get();
                    try {
                        inputColumn = inputSchema.lookupColumn(srcName);
                    }
                    catch (SchemaConfigException ex) {
                        inputColumn = null;
                    }
                }
                if (inputColumn != null) { // copy column
                    Column outputColumn = new Column(i++, name, inputColumn.getType());
                    builder.add(outputColumn);
                }
                else if (type.isPresent() && defaultValue.isPresent()) { // add column
                    Column outputColumn = new Column(i++, name, type.get());
                    builder.add(outputColumn);
                }
                else {
                    throw new SchemaConfigException(String.format("add_columns: Column src '%s' is not found in inputSchema, Column '%s' does not have \"type\" and \"default\"", srcName, name));
                }
            }
        }

        return new Schema(builder.build());
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
