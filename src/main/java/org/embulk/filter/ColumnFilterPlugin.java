package org.embulk.filter;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
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
import java.util.HashMap;

public class ColumnFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public SchemaConfig getColumns();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = task.getColumns().toSchema();

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        HashMap<Column, Column> columnMap = new HashMap<Column, Column>();
        for (Column outputColumn: outputSchema.getColumns()) {
            for (Column inputColumn: inputSchema.getColumns()) {
                if (inputColumn.getName().equals(outputColumn.getName())) {
                    columnMap.put(inputColumn, outputColumn);
                    break;
                }
            }
        }

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

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

                ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);
                while (pageReader.nextRecord()) {
                    inputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }

            class ColumnVisitorImpl implements ColumnVisitor {
                private final PageBuilder pageBuilder;

                ColumnVisitorImpl(PageBuilder pageBuilder) {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column inputColumn) {
                    Column outputColumn = (Column)columnMap.get(inputColumn);
                    if (outputColumn == null) { return; }
                    if (pageReader.isNull(inputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
                    }
                }

                @Override
                public void longColumn(Column inputColumn) {
                    Column outputColumn = (Column)columnMap.get(inputColumn);
                    if (outputColumn == null) { return; }
                    if (pageReader.isNull(inputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column inputColumn) {
                    Column outputColumn = (Column)columnMap.get(inputColumn);
                    if (outputColumn == null) { return; }
                    if (pageReader.isNull(inputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
                    }
                }

                @Override
                public void stringColumn(Column inputColumn) {
                    Column outputColumn = (Column)columnMap.get(inputColumn);
                    if (outputColumn == null) { return; }
                    if (pageReader.isNull(inputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column inputColumn) {
                    Column outputColumn = (Column)columnMap.get(inputColumn);
                    if (outputColumn == null) { return; }
                    if (pageReader.isNull(inputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
                    }
                }
            }
        };
    }
}
