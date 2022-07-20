/*
 * Copyright 2015 Naotoshi Seo, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.filter.column;

import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
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
import org.embulk.spi.type.Type;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ColumnFilterPlugin implements FilterPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFilterPlugin.class);
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    public ColumnFilterPlugin()
    {
    }

    // NOTE: This is not spi.ColumnConfig
    interface ColumnConfig extends Task
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

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimeZoneId();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("date")
        @ConfigDefault("null")
        public Optional<String> getDate();
    }

    interface PluginTask extends Task
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

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();
    }

    @Override
    public void transaction(final ConfigSource config, final Schema inputSchema,
            final FilterPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

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
        List<Column> newColumns = new ArrayList<>();
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
                    newColumns.add(outputColumn);
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
                    newColumns.add(outputColumn);
                }
                else if (type.isPresent() && defaultValue.isPresent()) { // add column
                    Column outputColumn = new Column(i++, name, type.get());
                    newColumns.add(outputColumn);
                }
                else {
                    throw new SchemaConfigException(String.format("columns: Column src '%s' is not found in inputSchema. Column '%s' does not have \"type\" and \"default\"", srcName, name));
                }
            }
        }
        else {
            for (Column column : inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, column.getName(), column.getType());
                newColumns.add(outputColumn);
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
                    newColumns.add(outputColumn);
                }
                else if (type.isPresent() && defaultValue.isPresent()) { // add column
                    Column outputColumn = new Column(i++, name, type.get());
                    newColumns.add(outputColumn);
                }
                else {
                    throw new SchemaConfigException(String.format("add_columns: Column src '%s' is not found in inputSchema, Column '%s' does not have \"type\" and \"default\"", srcName, name));
                }
            }
        }

        return new Schema(Collections.unmodifiableList(newColumns));
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

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
