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

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.column.ColumnFilterPlugin.PluginTask;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.embulk.spi.type.Types.JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestJsonVisitor
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    @Rule
    public org.embulk.test.EmbulkTestRuntime runtime = new org.embulk.test.EmbulkTestRuntime();

    @Before
    public void createResource()
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

        ConfigLoader loader = new ConfigLoader(ExecInternal.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }

    private JsonVisitor jsonVisitor(PluginTask task, Schema inputSchema)
    {
        Schema outputSchema = ColumnFilterPlugin.buildOutputSchema(task, inputSchema);
        return new JsonVisitor(task, inputSchema, outputSchema);
    }

    @Test
    public void getAncestorJsonColumnList()
    {
        ArrayList<JsonColumn> subject;

        subject = JsonVisitor.getAncestorJsonColumnList("$.json1.a.default");
        assertEquals("$['json1']", subject.get(0).getPath());
        assertTrue(subject.get(0).getDefaultValue().isMapValue());
        assertEquals("$['json1']['a']", subject.get(1).getPath());
        assertTrue(subject.get(1).getDefaultValue().isMapValue());

        subject = JsonVisitor.getAncestorJsonColumnList("$.json1.a[0].default");
        assertEquals("$['json1']", subject.get(0).getPath());
        assertTrue(subject.get(0).getDefaultValue().isMapValue());
        assertEquals("$['json1']['a']", subject.get(1).getPath());
        assertTrue(subject.get(1).getDefaultValue().isArrayValue());
        assertEquals("$['json1']['a'][0]", subject.get(2).getPath());
        assertTrue(subject.get(2).getDefaultValue().isMapValue());

        subject = JsonVisitor.getAncestorJsonColumnList("$.json1.a.default[0]");
        assertEquals("$['json1']", subject.get(0).getPath());
        assertTrue(subject.get(0).getDefaultValue().isMapValue());
        assertEquals("$['json1']['a']", subject.get(1).getPath());
        assertTrue(subject.get(1).getDefaultValue().isMapValue());
        assertEquals("$['json1']['a']['default']", subject.get(2).getPath());
        assertTrue(subject.get(2).getDefaultValue().isArrayValue());
    }

    @Test
    public void buildShouldVisitSet()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: \"$.json1.a.a.a\"}",
                "add_columns:",
                "  - {name: \"$.json1.b.b[1].b\", type: string, default: foo}",
                "drop_columns:",
                "  - {name: \"$.json1.c.c[*].c\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        assertTrue(subject.shouldVisit("$['json1']['a']['a']['a']"));
        assertTrue(subject.shouldVisit("$['json1']['a']['a']"));
        assertTrue(subject.shouldVisit("$['json1']['a']"));
        assertTrue(subject.shouldVisit("$['json1']['b']['b'][1]['b']"));
        assertTrue(subject.shouldVisit("$['json1']['b']['b'][1]"));
        assertTrue(subject.shouldVisit("$['json1']['b']['b']"));
        assertTrue(subject.shouldVisit("$['json1']['b']"));
        assertTrue(subject.shouldVisit("$['json1']['c']['c'][*]['c']"));
        assertTrue(subject.shouldVisit("$['json1']['c']['c'][*]"));
        assertTrue(subject.shouldVisit("$['json1']['c']['c']"));
        assertTrue(subject.shouldVisit("$['json1']['c']"));
        assertTrue(subject.shouldVisit("$['json1']"));
        assertFalse(subject.shouldVisit("$['json2']"));
    }

    @Test
    public void buildJsonDropColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: $.json1.a.default}",
                "  - {name: $.json1.a.copy}",
                "  - {name: \"$.json1.a.copy_array[1]\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        assertFalse(subject.jsonDropColumns.containsKey("$['json1']"));
        assertTrue(subject.jsonDropColumns.containsKey("$['json1']['a']"));
        assertTrue(subject.jsonDropColumns.containsKey("$['json1']['a']['copy_array']"));

        {
            HashSet<String> jsonColumns = subject.jsonDropColumns.get("$['json1']['a']");
            assertEquals(2, jsonColumns.size());
            assertTrue(jsonColumns.contains("$['json1']['a']['default']"));
            assertTrue(jsonColumns.contains("$['json1']['a']['copy']"));
        }

        {
            HashSet<String> jsonColumns = subject.jsonDropColumns.get("$['json1']['a']['copy_array']");
            assertEquals(1, jsonColumns.size());
            assertTrue(jsonColumns.contains("$['json1']['a']['copy_array'][1]"));
        }
    }

    @Test(expected = ConfigException.class)
    public void assertDoNotEndsWithArrayWildcard_AddColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: \"$.json1.b.b[*]\", type: json, default: []}");
        Schema inputSchema = Schema.builder().build();
        // b[*] should be written as b
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void assertDoNotEndsWithArrayWildcard_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: \"$.json1.b.b[*]\"}");
        Schema inputSchema = Schema.builder().build();
        // b[*] should be written as b
        jsonVisitor(task, inputSchema);
    }

    @Test
    public void buildJsonAddColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: $.json1.a.default, type: string, default: foo}",
                "  - {name: $.json1.a.copy, src: $.json1.a.src}",
                "  - {name: \"$.json1.a.copy_array[1]\", src: \"$.json1.a.copy_array[0]\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        assertTrue(subject.jsonAddColumns.containsKey("$"));
        assertTrue(subject.jsonAddColumns.containsKey("$['json1']"));
        assertTrue(subject.jsonAddColumns.containsKey("$['json1']['a']"));
        assertTrue(subject.jsonAddColumns.containsKey("$['json1']['a']['copy_array']"));

        {
            HashMap<String, JsonColumn> jsonColumns = subject.jsonAddColumns.get("$['json1']['a']");
            assertEquals(3, jsonColumns.size());
            String[] keys = jsonColumns.keySet().toArray(new String[0]);
            JsonColumn[] values = jsonColumns.values().toArray(new JsonColumn[0]);
            assertEquals("$['json1']['a']['default']", keys[0]);
            assertEquals("$['json1']['a']['default']", values[0].getPath());
            assertEquals("$['json1']['a']['copy']", keys[1]);
            assertEquals("$['json1']['a']['copy']", values[1].getPath());
            assertEquals("$['json1']['a']['copy_array']", keys[2]);
            assertEquals("$['json1']['a']['copy_array']", values[2].getPath());
        }

        {
            HashMap<String, JsonColumn> jsonColumns = subject.jsonAddColumns.get("$['json1']['a']['copy_array']");
            assertEquals(1, jsonColumns.size());
            String[] keys = jsonColumns.keySet().toArray(new String[0]);
            JsonColumn[] values = jsonColumns.values().toArray(new JsonColumn[0]);
            assertEquals("$['json1']['a']['copy_array'][1]", keys[0]);
            assertEquals("$['json1']['a']['copy_array'][1]", values[0].getPath());
        }
    }

    @Test
    public void buildJsonColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: $.json1.a.default, type: string, default: foo}",
                "  - {name: $.json1.a.copy, src: $.json1.a.src}",
                "  - {name: \"$.json1.a.copy_array[1]\", src: \"$.json1.a.copy_array[0]\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // 1st level keys are parents of jsonpath
        assertTrue(subject.jsonColumns.containsKey("$"));
        assertTrue(subject.jsonColumns.containsKey("$['json1']"));
        assertTrue(subject.jsonColumns.containsKey("$['json1']['a']"));
        assertTrue(subject.jsonColumns.containsKey("$['json1']['a']['copy_array']"));

        {
            HashMap<String, JsonColumn> jsonColumns = subject.jsonColumns.get("$['json1']['a']");
            assertEquals(3, jsonColumns.size());
            String[] keys = jsonColumns.keySet().toArray(new String[0]);
            JsonColumn[] values = jsonColumns.values().toArray(new JsonColumn[0]);
            assertEquals("$['json1']['a']['default']", keys[0]);
            assertEquals("$['json1']['a']['default']", values[0].getPath());
            assertEquals("$['json1']['a']['copy']", keys[1]);
            assertEquals("$['json1']['a']['copy']", values[1].getPath());
            assertEquals("$['json1']['a']['copy_array']", keys[2]);
            assertEquals("$['json1']['a']['copy_array']", values[2].getPath());
        }

        {
            HashMap<String, JsonColumn> jsonColumns = subject.jsonColumns.get("$['json1']['a']['copy_array']");
            assertEquals(1, jsonColumns.size());
            String[] keys = jsonColumns.keySet().toArray(new String[0]);
            JsonColumn[] values = jsonColumns.values().toArray(new JsonColumn[0]);
            assertEquals("$['json1']['a']['copy_array'][1]", keys[0]);
            assertEquals("$['json1']['a']['copy_array'][1]", values[0].getPath());
        }
    }

    @Test
    public void buildJsonSchema()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: $.json1.a.default}",
                "add_columns:",
                "  - {name: $.json1.a.copy, src: $.json1.a.src}",
                "columns:",
                "  - {name: \"$.json1.a.copy_array[1]\", src: \"$.json1.a.copy_array[0]\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        assertFalse(subject.jsonDropColumns.isEmpty());
        assertFalse(subject.jsonAddColumns.isEmpty());
        assertTrue(subject.jsonColumns.isEmpty()); // drop_columns overcome columns
    }

    @Test
    public void visitMap_DropColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: $.json1.k1.k1}",
                "  - {name: $.json1.k2}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":{"k1":"v"},"k2":{"k2":"v"}}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newMap(k1, v),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":{}}", visited.toString());
    }

    @Test
    public void visitMap_AddColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: $.json1.k3.k3, type: string, default: v}",
                "  - {name: $.json1.k4, src: $.json1.k2}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":{"k1":"v"},"k2":{"k2":"v"}}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newMap(k1, v),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":{\"k1\":\"v\"},\"k2\":{\"k2\":\"v\"},\"k3\":{\"k3\":\"v\"},\"k4\":{\"k2\":\"v\"}}", visited.toString());
    }

    @Test
    public void visitMap_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: $.json1.k1}",
                "  - {name: $.json1.k2.k2}",
                "  - {name: $.json1.k3.k3, type: string, default: v}",
                "  - {name: $.json1.k4, src: $.json1.k2}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":{"k1":"v"},"k2":{"k1":"v","k2":"v"}}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newMap(k1, v),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":{\"k1\":\"v\"},\"k2\":{\"k2\":\"v\"},\"k3\":{\"k3\":\"v\"},\"k4\":{\"k2\":\"v\"}}", visited.toString());
    }

    @Test
    public void visitArray_DropColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: \"$.json1.k1[0].k1\"}",
                "  - {name: \"$.json1.k2[*]\"}", // ending with [*] is allowed for drop_columns, but not for others
                "  - {name: \"$.json1.k3[*].k1\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":[{"k1":"v"}],"k2":["v","v"],"k3":[{"k3":"v"}]}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value k3 = ValueFactory.newString("k3");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newArray(v, v),
                k3, ValueFactory.newArray(ValueFactory.newMap(k1, v)));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":[{}],\"k2\":[],\"k3\":[{}]}", visited.toString());
    }

    @Test
    public void visitArray_AddColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: \"$.json1.k1[1]\", src: \"$.json1.k1[0]\"}",
                "  - {name: \"$.json1.k3[*].k2\", type: string, default: v}",
                "  - {name: \"$.json1.k4[*].k1\", type: string, default: v}",
                "  - {name: \"$.json1.k5[0].k1\", type: string, default: v}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":[{"k1":"v"}],"k2":["v","v"],"k3":[{"k1":"v"}]}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value k3 = ValueFactory.newString("k3");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newArray(v, v),
                k3, ValueFactory.newArray(ValueFactory.newMap(k1, v)));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":[{\"k1\":\"v\"},{\"k1\":\"v\"}],\"k2\":[\"v\",\"v\"],\"k3\":[{\"k1\":\"v\",\"k2\":\"v\"}],\"k4\":[],\"k5\":[{\"k1\":\"v\"}]}", visited.toString());
    }

    @Test
    public void visitArray_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: \"$.json1.k1[1]\", src: \"$.json1.k1[0]\"}",
                "  - {name: \"$.json1.k2[0]\"}",
                "  - {name: \"$.json1.k3[*].k1\"}",
                "  - {name: \"$.json1.k3[*].k3\", src: \"$.json1.k3[*].k1\"}",
                "  - {name: \"$.json1.k4[*].k1\", type: string, default: v}",
                "  - {name: \"$.json1.k5[0].k1\", type: string, default: v}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":[{"k1":"v"},"v"],"k2":["v","v"],"k3":[{"k1":"v","k2":"v"}]}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value k3 = ValueFactory.newString("k3");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v), v),
                k2, ValueFactory.newArray(v, v),
                k3, ValueFactory.newArray(ValueFactory.newMap(k1, v, k2, v)));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":[{\"k1\":\"v\"}],\"k2\":[\"v\"],\"k3\":[{\"k1\":\"v\",\"k3\":\"v\"}],\"k4\":[],\"k5\":[{\"k1\":\"v\"}]}", visited.toString());
    }

    @Test
    public void visitMap_dropColumnsUsingBracketNotation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: \"$['json1']['k1']['k1']\"}",
                "  - {name: \"$['json1']['k2']\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":{"k1":"v"},"k2":{"k2":"v"}}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newMap(k1, v),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":{}}", visited.toString());
    }

    @Test
    public void visitMap_addColumnsUsingBracketNotation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: \"$['json1']['k3']['k3']\", type: string, default: v}",
                "  - {name: \"$['json1']['k4']\", src: \"$['json1']['k2']\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":{"k1":"v"},"k2":{"k2":"v"}}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newMap(k1, v),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":{\"k1\":\"v\"},\"k2\":{\"k2\":\"v\"},\"k3\":{\"k3\":\"v\"},\"k4\":{\"k2\":\"v\"}}", visited.toString());
    }

    @Test
    public void visitMap_columnsUsingBracketNotation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: \"$['json1']['k1']\"}",
                "  - {name: \"$['json1']['k2']['k2']\"}",
                "  - {name: \"$['json1']['k3']['k3']\", type: string, default: v}",
                "  - {name: \"$['json1']['k4']\", src: \"$['json1']['k2']\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":{"k1":"v"},"k2":{"k1":"v","k2":"v"}}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newMap(k1, v),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":{\"k1\":\"v\"},\"k2\":{\"k2\":\"v\"},\"k3\":{\"k3\":\"v\"},\"k4\":{\"k2\":\"v\"}}", visited.toString());
    }

    @Test
    public void visitArray_dropColumnsUsingBracketNotation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "drop_columns:",
                "  - {name: \"$['json1']['k1'][0]['k1']\"}",
                "  - {name: \"$['json1']['k2'][*]\"}"); // ending with [*] is allowed for drop_columns, but not for others
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":[{"k1":"v"}[,"k2":["v","v"]}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newArray(v, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":[{}],\"k2\":[]}", visited.toString());
    }

    @Test
    public void visitArray_addColumnsUsingBracketNotation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: \"$['json1']['k1'][1]\", src: \"$['json1']['k1'][0]\"}",
                "  - {name: \"$['json1']['k3'][0]['k3']\", type: string, default: v}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .add("json2", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":[{"k1":"v"}],"k2":["v","v"]}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newArray(v, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":[{\"k1\":\"v\"},{\"k1\":\"v\"}],\"k2\":[\"v\",\"v\"],\"k3\":[{\"k3\":\"v\"}]}", visited.toString());
    }

    @Test
    public void visitArray_columnsUsingBracketNotation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "  - {name: \"$['json1']['k1'][1]\", src: \"$['json1']['k1'][0]\"}",
                "  - {name: \"$['json1']['k2'][0]\"}",
                "  - {name: \"$['json1']['k3'][0]['k3']\", type: string, default: v}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":[{"k1":"v"},"v"],"k2":["v","v"]}
        Value k1 = ValueFactory.newString("k1");
        Value k2 = ValueFactory.newString("k2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v), v),
                k2, ValueFactory.newArray(v, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k1\":[{\"k1\":\"v\"}],\"k2\":[\"v\"],\"k3\":[{\"k3\":\"v\"}]}", visited.toString());
    }

    // Because the dot notation is converted to single quotes by default,
    // it can be mixed with the bracket notation of single quotes
    @Test
    public void visit_withDotAndBracket()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                " - {name: \"$.json1['k_1']\"}",
                " - {name: \"$.json1['k_1'][0]['k_1']\"}",
                " - {name: \"$['json1']['k_2']\"}",
                " - {name: \"$['json1']['k_2']['k_2']\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k.1":[{"k.1":"v"}], "k.2":{"k.2":"v"}}
        Value k1 = ValueFactory.newString("k_1");
        Value k2 = ValueFactory.newString("k_2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k_1\":[{\"k_1\":\"v\"}],\"k_2\":{\"k_2\":\"v\"}}", visited.toString());
    }

    // Because the bracket notation of double quotes converted to single quotes internally
    // it can be mixed with the bracket notation of single quotes
    @Test
    public void visit_withSingleQuotesAndDoubleQuotes()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                " - {name: \"$['json1']['k_1']\", src: \"$['json1']['k.1']\"}",
                " - {name: '$[\"json1\"][\"k_1\"][0][\"k_1\"]', src: '$[\"json1\"][\"k_1\"][0][\"k.1\"]'}",
                " - {name: '$[\"json1\"][\"k_2\"]', src: '$[\"json1\"][\"k.2\"]'}",
                " - {name: '$[\"json1\"][\"k_2\"][\"k_2\"]', src: '$[\"json1\"][\"k_2\"][\"k.2\"]'}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k.1":[{"k.1":"v"}], "k.2":{"k.2":"v"}}
        Value k1 = ValueFactory.newString("k.1");
        Value k2 = ValueFactory.newString("k.2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k_1\":[{\"k_1\":\"v\"}],\"k_2\":{\"k_2\":\"v\"}}", visited.toString());
    }

    @Test
    public void visit_withComplexRename()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                " - {name: \"$.json1['k____1']\", src: \"$.json1['k.-=+1']\"}",
                " - {name: \"$.json1['k____1'][0]['k____1']\", src: \"$.json1['k____1'][0]['k.-=+1']\"}",
                " - {name: \"$['json1']['k_2']\", src: \"$['json1']['k.2']\"}",
                " - {name: \"$['json1']['k_2']['k_2']\", src: \"$['json1']['k_2']['k.2']\"}");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k.1":[{"k.1":"v"}], "k.2":{"k.2":"v"}}
        Value k1 = ValueFactory.newString("k.-=+1");
        Value k2 = ValueFactory.newString("k.2");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(
                k1, ValueFactory.newArray(ValueFactory.newMap(k1, v)),
                k2, ValueFactory.newMap(k2, v));

        MapValue visited = subject.visit("$['json1']", map).asMapValue();
        assertEquals("{\"k____1\":[{\"k____1\":\"v\"}],\"k_2\":{\"k_2\":\"v\"}}", visited.toString());
    }

    @Test
    public void visit_withColumnNameIncludingSingleQuotes()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                " - {name: \"$['\\\\'json1']['k1']\"}");
        Schema inputSchema = Schema.builder()
                .add("'json1", JSON)
                .build();
        JsonVisitor subject = jsonVisitor(task, inputSchema);

        // {"k1":"v"}
        Value k1 = ValueFactory.newString("k1");
        Value v = ValueFactory.newString("v");
        Value map = ValueFactory.newMap(k1, v);

        MapValue visited = subject.visit("$['\\'json1']", map).asMapValue();
        assertEquals("{\"k1\":\"v\"}", visited.toString());
    }

    @Test(expected = ConfigException.class)
    public void configException_MultiProperties()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$['json1','k1']\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    // It is recognized multi properties if the square brackets does not close properly
    @Test(expected = ConfigException.class)
    public void configException_PropertyIsNotSeparatedByCommas()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                " - name: \"$['json1'}['k1']\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_FunctionPathToken()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$['json1'].length()\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_PredicatePathToken()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$.store.book[?(@.price < 10)]\"");
        Schema inputSchema = Schema.builder()
                .add("store", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_ScanPathToken()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$.json1..key1\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_MultiIndexOperation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$.json1[0,1]\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_IndexOperationAtMiddlePosition()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$.json1[0,1].key1\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_ArraySliceOperation()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$.json1[1:2]\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Test(expected = ConfigException.class)
    public void configException_MArraySliceOperationAtMiddlePosition()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$.json1[1:2].key1\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();
        jsonVisitor(task, inputSchema);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void configException_PathCompileError()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "columns:",
                "- name: \"$['json][''key1']\"");
        Schema inputSchema = Schema.builder()
                .add("json1", JSON)
                .build();

        thrown.expectMessage("path $['json][''key1'], Property must be separated by comma or Property must be terminated close square bracket at index 9");

        jsonVisitor(task, inputSchema);
    }
}
