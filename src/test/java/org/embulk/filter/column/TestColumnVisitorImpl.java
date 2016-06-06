package org.embulk.filter.column;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.column.ColumnFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class TestColumnVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createReasource()
    {
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return config.loadConfig(PluginTask.class);
    }

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object ... objects)
    {
        MockPageOutput output = new MockPageOutput();
        Schema outputSchema = ColumnFilterPlugin.buildOutputSchema(task, inputSchema);
        PageBuilder pageBuilder = new PageBuilder(runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, objects);
        for (Page page : pages) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
        pageBuilder.close();
        return Pages.toObjects(outputSchema, output.pages);
    }

    @Test
    public void visit_Columns_WithDrop()
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
        List<Object[]> records = filter(task, inputSchema,
                Timestamp.ofEpochSecond(0), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "remove_me",
                Timestamp.ofEpochSecond(0), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "remove_me");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(6, record.length);
            assertEquals(Timestamp.ofEpochSecond(0), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
        }
    }

    @Test
    public void visit_Columns_WithDefault()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: timestamp, type: timestamp, default: 2015-07-13, format: \"%Y-%m-%d\", timezone: UTC}",
                "  - {name: string, type: string, default: string}",
                "  - {name: boolean, type: boolean, default: true}",
                "  - {name: long, type: long, default: 0}",
                "  - {name: double, type: double, default: 0.5}",
                "  - {name: json, type: json, default: \"{\\\"foo\\\":\\\"bar\\\"}\" }");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("remove_me", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "remove_me",
                null, null, null, null, null, null, "remove_me");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(6, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
        }
        {
            record = records.get(1);
            assertEquals(6, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[0]);
            assertEquals("string", record[1]);
            assertEquals(new Boolean(true), record[2]);
            assertEquals(new Long(0), record[3]);
            assertEquals(new Double(0.5), record[4]);
            assertEquals("{\"foo\":\"bar\"}", record[5].toString());
        }
    }

    @Test
    public void visit_Columns_WithSrc()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: copy, src: src}");
        Schema inputSchema = Schema.builder()
                .add("src", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                "src");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals("src", record[0]);
        }
    }

    @Test
    public void visit_DropColumns()
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
        List<Object[]> records = filter(task, inputSchema,
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "keep_me",
                null, null, null, null, null, null, "keep_me");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals("keep_me", record[0]);
        }
        {
            record = records.get(1);
            assertEquals(1, record.length);
            assertEquals("keep_me", record[0]);
        }
    }

    @Test
    public void visit_AddColumns_WithDefault()
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
        List<Object[]> records = filter(task, inputSchema,
                "keep_me",
                "keep_me");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals("keep_me", record[0]);
            assertEquals(Timestamp.ofEpochSecond(1436745600), record[1]);
            assertEquals("string", record[2]);
            assertEquals(new Boolean(true), record[3]);
            assertEquals(new Long(0), record[4]);
            assertEquals(new Double(0.5), record[5]);
            assertEquals("{\"foo\":\"bar\"}", record[6].toString());
        }
    }

    @Test
    public void visit_AddColumns_WithSrc()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: copy, src: src}");
        Schema inputSchema = Schema.builder()
                .add("src", STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                "src");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(2, record.length);
            assertEquals("src", record[0]);
            assertEquals("src", record[1]);
        }
    }
}
