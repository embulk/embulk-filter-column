package org.embulk.filter.column;

import com.google.common.collect.Lists;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.column.ColumnFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class TestColumnFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ColumnFilterPlugin plugin;

    @Before
    public void createReasource()
    {
        plugin = new ColumnFilterPlugin();
    }

    private Schema schema(Column... columns)
    {
        return new Schema(Lists.newArrayList(columns));
    }

    private ConfigSource configFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        ConfigSource config = configFromYamlString(lines);
        return config.loadConfig(PluginTask.class);
    }

    private void transaction(ConfigSource config, Schema inputSchema)
    {
        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
            }
        });
    }

    @Test
    public void buildOutputSchema_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: timestamp}",
                "  - {name: string}",
                "  - {name: boolean}",
                "  - {name: long}",
                "  - {name: double}",
                "  - {name: json}");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("remove_me", STRING)
                .build();

        Schema outputSchema = ColumnFilterPlugin.buildOutputSchema(task, inputSchema);
        assertEquals(6, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("timestamp", column.getName());
        }
    }

    @Test
    public void buildOutputSchema_DropColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: timestamp}",
                "  - {name: string}",
                "  - {name: boolean}",
                "  - {name: long}",
                "  - {name: double}",
                "  - {name: json}");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("keep_me", STRING)
                .build();

        Schema outputSchema = ColumnFilterPlugin.buildOutputSchema(task, inputSchema);
        assertEquals(1, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("keep_me", column.getName());
        }
    }

    @Test
    public void buildOutputSchema_AddColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: timestamp, type: timestamp, default: 2015-07-13, format: \"%Y-%m-%d\", timezone: UTC}",
                "  - {name: string, type: string, default: string}",
                "  - {name: boolean, type: boolean, default: true}",
                "  - {name: long, type: long, default: 0}",
                "  - {name: double, type: double, default: 0.5}",
                "  - {name: json, type: json, default: \"{\\\"foo\\\":\\\"bar\\\"}\" }");
        Schema inputSchema = Schema.builder()
                .add("keep_me", STRING)
                .build();

        Schema outputSchema = ColumnFilterPlugin.buildOutputSchema(task, inputSchema);
        assertEquals(7, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("keep_me", column.getName());
        }
        {
            column = outputSchema.getColumn(1);
            assertEquals("timestamp", column.getName());
        }
    }

    @Test(expected = ConfigException.class)
    public void configure_EitherOfColumnsOrDropColumnsCanBeSpecified()
    {
        ConfigSource config = configFromYamlString(
                "type: column",
                "columns:",
                "- {name: a}",
                "drop_columns:",
                "- {name: a}");
        Schema inputSchema = schema(
                new Column(0, "a", STRING),
                new Column(1, "b", STRING));

        transaction(config, inputSchema);
    }
}
