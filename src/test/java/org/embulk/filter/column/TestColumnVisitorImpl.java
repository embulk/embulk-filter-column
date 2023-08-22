/*
 * Copyright 2016 Naotoshi Seo, and the Embulk project
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

import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.column.ColumnFilterPlugin.PluginTask;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.PageTestUtils;
import org.embulk.test.TestPageBuilderReader.MockPageOutput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

import java.time.Instant;
import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class TestColumnVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createResource()
    {
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

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

        ConfigLoader loader = new ConfigLoader(ExecInternal.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return CONFIG_MAPPER.map(config, PluginTask.class);
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
                Instant.ofEpochSecond(0), "string", Boolean.valueOf(true), Long.valueOf(0), Double.valueOf(0.5), ValueFactory.newString("json"), "remove_me",
                Instant.ofEpochSecond(0), "string", Boolean.valueOf(true), Long.valueOf(0), Double.valueOf(0.5), ValueFactory.newString("json"), "remove_me");

        assertEquals(2, records.size());
        Object[] record;
        {
            record = records.get(0);
            Boolean r = record[0] instanceof Timestamp;
            System.out.println("---->");
            System.out.println(record[0].getClass());
            assertEquals(6, record.length);
            assertEquals(Instant.ofEpochSecond(0), ((Timestamp) record[0]).getInstant());
            assertEquals("string", record[1]);
            assertEquals(Boolean.valueOf(true), record[2]);
            assertEquals(Long.valueOf(0), record[3]);
            assertEquals(Double.valueOf(0.5), record[4]);
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
                Instant.ofEpochSecond(1436745600), "string", Boolean.valueOf(true), Long.valueOf(0), Double.valueOf(0.5), ValueFactory.newString("json"), "remove_me",
                null, null, null, null, null, null, "remove_me");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(6, record.length);
            assertEquals(Instant.ofEpochSecond(1436745600), ((Timestamp) record[0]).getInstant());
            assertEquals("string", record[1]);
            assertEquals(Boolean.valueOf(true), record[2]);
            assertEquals(Long.valueOf(0), record[3]);
            assertEquals(Double.valueOf(0.5), record[4]);
            assertEquals(ValueFactory.newString("json"), record[5]);
        }
        {
            record = records.get(1);
            assertEquals(6, record.length);
            assertEquals(Instant.ofEpochSecond(1436745600), ((Timestamp) record[0]).getInstant());
            assertEquals("string", record[1]);
            assertEquals(Boolean.valueOf(true), record[2]);
            assertEquals(Long.valueOf(0), record[3]);
            assertEquals(Double.valueOf(0.5), record[4]);
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
                Instant.ofEpochSecond(1436745600), "string", Boolean.valueOf(true), Long.valueOf(0), Double.valueOf(0.5), ValueFactory.newString("json"), "keep_me",
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
            assertEquals(Instant.ofEpochSecond(1436745600), ((Timestamp) record[1]).getInstant());
            assertEquals("string", record[2]);
            assertEquals(Boolean.valueOf(true), record[3]);
            assertEquals(Long.valueOf(0), record[4]);
            assertEquals(Double.valueOf(0.5), record[5]);
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
