package org.embulk.filter.column;

import io.github.medjed.jsonpathcompiler.InvalidPathException;
import io.github.medjed.jsonpathcompiler.expressions.Path;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import io.github.medjed.jsonpathcompiler.expressions.path.PathToken;
import org.embulk.config.ConfigException;
import org.embulk.filter.column.ColumnFilterPlugin.ColumnConfig;
import org.embulk.filter.column.ColumnFilterPlugin.PluginTask;

import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonVisitor
{
    static final Logger logger = Exec.getLogger(ColumnFilterPlugin.class);
    final PluginTask task;
    final Schema inputSchema;
    final Schema outputSchema;
    // jsonpath
    final HashSet<String> shouldVisitSet = new HashSet<>();
    // parent jsonpath => { jsonpath => json column }
    final HashMap<String, LinkedHashMap<String, JsonColumn>> jsonColumns = new HashMap<>();
    // parent jsonpath => { jsonpath => json column }
    final HashMap<String, LinkedHashMap<String, JsonColumn>> jsonAddColumns = new HashMap<>();
    // parent jsonpath => [ jsonpath ]
    final HashMap<String, HashSet<String>> jsonDropColumns = new HashMap<>();

    JsonVisitor(PluginTask task, Schema inputSchema, Schema outputSchema)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;

        buildShouldVisitSet();
        buildJsonSchema();
    }

    static Value getDefault(PluginTask task, String name, Type type, ColumnConfig columnConfig)
    {
        Object defaultValue = ColumnVisitorImpl.getDefault(task, name, type, columnConfig);
        if (defaultValue == null) {
            return ValueFactory.newNil();
        }
        if (type instanceof BooleanType) {
            return ValueFactory.newBoolean((Boolean) defaultValue);
        }
        else if (type instanceof LongType) {
            return ValueFactory.newInteger((Long) defaultValue);
        }
        else if (type instanceof DoubleType) {
            return ValueFactory.newFloat((Double) defaultValue);
        }
        else if (type instanceof StringType) {
            return ValueFactory.newString((String) defaultValue.toString());
        }
        else if (type instanceof JsonType) {
            return (Value) defaultValue;
        }
        else if (type instanceof TimestampType) {
            throw new ConfigException("type: timestamp is not available in json path");
        }
        else {
            throw new ConfigException(String.format("type: '%s' is not supported", type));
        }
    }

    private void jsonColumnsPut(String path, JsonColumn value)
    {
        Path compiledPath = PathCompiler.compile(path);
        String parentPath = compiledPath.getParentPath();
        if (! jsonColumns.containsKey(parentPath)) {
            jsonColumns.put(parentPath, new LinkedHashMap<String, JsonColumn>());
        }
        jsonColumns.get(parentPath).put(compiledPath.toString(), value);
    }

    private boolean jsonColumnsContainsKey(String path)
    {
        Path compiledPath = PathCompiler.compile(path);
        String parentPath = compiledPath.getParentPath();
        if (jsonColumns.containsKey(parentPath)) {
            return jsonColumns.get(parentPath).containsKey(compiledPath.toString());
        }
        else {
            return false;
        }
    }

    private void jsonAddColumnsPut(String path, JsonColumn value)
    {
        Path compiledPath = PathCompiler.compile(path);
        String parentPath = compiledPath.getParentPath();
        if (! jsonAddColumns.containsKey(parentPath)) {
            jsonAddColumns.put(parentPath, new LinkedHashMap<String, JsonColumn>());
        }
        jsonAddColumns.get(parentPath).put(compiledPath.toString(), value);
    }

    private boolean jsonAddColumnsContainsKey(String path)
    {
        Path compiledPath = PathCompiler.compile(path);
        String parentPath = compiledPath.getParentPath();
        if (jsonAddColumns.containsKey(parentPath)) {
            return jsonAddColumns.get(parentPath).containsKey(compiledPath.toString());
        }
        else {
            return false;
        }
    }

    private void jsonDropColumnsPut(String path)
    {
        Path compiledPath = PathCompiler.compile(path);
        String parentPath = compiledPath.getParentPath();
        if (! jsonDropColumns.containsKey(parentPath)) {
            jsonDropColumns.put(parentPath, new HashSet<String>());
        }
        jsonDropColumns.get(parentPath).add(compiledPath.toString());
    }

    private void buildJsonColumns()
    {
        List<ColumnConfig> columns = task.getColumns();
        for (ColumnConfig column : columns) {
            String name = column.getName();
            // skip NON json path notation to build output schema
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            JsonPathTokenUtil.assertDoNotEndsWithArrayWildcard(name);
            // automatically fill ancestor jsonpaths
            for (JsonColumn ancestorJsonColumn : getAncestorJsonColumnList(name)) {
                String ancestorJsonPath = ancestorJsonColumn.getPath();
                if (!jsonColumnsContainsKey(ancestorJsonPath)) {
                    jsonColumnsPut(ancestorJsonPath, ancestorJsonColumn);
                }
            }
            // leaf jsonpath
            if (column.getSrc().isPresent()) {
                String src = column.getSrc().get();
                jsonColumnsPut(name, new JsonColumn(name, null, null, src));
            }
            else if (column.getType().isPresent() && column.getDefault().isPresent()) { // add column
                Type type = column.getType().get();
                Value defaultValue = getDefault(task, name, type, column);
                jsonColumnsPut(name, new JsonColumn(name, type, defaultValue));
            }
            else {
                Type type = column.getType().isPresent() ? column.getType().get() : null;
                jsonColumnsPut(name, new JsonColumn(name, type));
            }
        }
    }

    private void buildJsonAddColumns()
    {
        List<ColumnConfig> addColumns = task.getAddColumns();
        for (ColumnConfig column : addColumns) {
            String name = column.getName();
            // skip NON json path notation to build output schema
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            JsonPathTokenUtil.assertDoNotEndsWithArrayWildcard(name);
            // automatically fill ancestor jsonpaths
            for (JsonColumn ancestorJsonColumn : getAncestorJsonColumnList(name)) {
                String ancestorJsonPath = ancestorJsonColumn.getPath();
                if (!jsonAddColumnsContainsKey(ancestorJsonPath)) {
                    jsonAddColumnsPut(ancestorJsonPath, ancestorJsonColumn);
                }
            }
            // leaf jsonpath
            if (column.getSrc().isPresent()) {
                String src = column.getSrc().get();
                jsonAddColumnsPut(name, new JsonColumn(name, null, null, src));
            }
            else if (column.getType().isPresent() && column.getDefault().isPresent()) { // add column
                Type type = column.getType().get();
                Value defaultValue = getDefault(task, name, type, column);
                jsonAddColumnsPut(name, new JsonColumn(name, type, defaultValue));
            }
            else {
                throw new SchemaConfigException(String.format("add_columns: Column '%s' does not have \"src\", or \"type\" and \"default\"", name));
            }
        }
    }

    private void buildJsonDropColumns()
    {
        List<ColumnConfig> dropColumns = task.getDropColumns();
        for (ColumnConfig dropColumn : dropColumns) {
            String name = dropColumn.getName();
            // skip NON json path notation to build output schema
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            jsonDropColumnsPut(name);
        }
    }

    // build jsonColumns, jsonAddColumns, and jsonDropColumns
    private void buildJsonSchema()
    {
        if (task.getDropColumns().size() > 0) {
            buildJsonDropColumns();
        }
        else if (task.getColumns().size() > 0) {
            buildJsonColumns();
        }
        // Add columns to last. If you want to add to head or middle, you can use `columns` option
        if (task.getAddColumns().size() > 0) {
            buildJsonAddColumns();
        }
    }

    // json partial path => Boolean to avoid unnecessary type: json visit
    private void buildShouldVisitSet()
    {
        ArrayList<ColumnConfig> columnConfigs = new ArrayList<>(task.getColumns());
        columnConfigs.addAll(task.getAddColumns());
        columnConfigs.addAll(task.getDropColumns());

        for (ColumnConfig columnConfig : columnConfigs) {
            String name = columnConfig.getName();
            if (!PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            for (JsonColumn ancestorJsonColumn : getAncestorJsonColumnList(name)) {
                this.shouldVisitSet.add(ancestorJsonColumn.getPath());
            }
            Path path = PathCompiler.compile(name);
            this.shouldVisitSet.add(path.toString());
        }
    }

    /*
     * <pre>
     * $['foo']['bar'][0]['baz']
     * #=>
     * name: $['foo'], type: json, default: {}
     * name: $['foo']['bar'], type: json, default: []
     * name: $['foo']['bar'][0], type: json, default: {}
     * </pre>
     *
     * @return ancestors as an array
     */
    public static ArrayList<JsonColumn> getAncestorJsonColumnList(String jsonPath)
    {
        ArrayList<JsonColumn> ancestorJsonColumnList = new ArrayList<>();
        Path path;
        try {
            path = PathCompiler.compile(jsonPath);
        }
        catch (InvalidPathException e) {
            throw new ConfigException(String.format("jsonpath %s, %s", jsonPath, e.getMessage()));
        }
        StringBuilder partialPath = new StringBuilder("$");
        PathToken parts = path.getRoot();
        parts = parts.next(); // skip "$"
        while (! parts.isLeaf()) {
            JsonPathTokenUtil.assertSupportedPathToken(parts, jsonPath);
            partialPath.append(parts.getPathFragment());
            PathToken next = parts.next();
            JsonColumn jsonColumn;
            if (next instanceof ArrayPathToken) {
                jsonColumn = new JsonColumn(partialPath.toString(), Types.JSON, ValueFactory.newArray(new Value[0], false));
            }
            else {
                jsonColumn = new JsonColumn(partialPath.toString(), Types.JSON, ValueFactory.newMap(new Value[0]));
            }
            ancestorJsonColumnList.add(jsonColumn);
            parts = next;
        }
        JsonPathTokenUtil.assertSupportedPathToken(parts, jsonPath);
        return ancestorJsonColumnList;
    }

    boolean shouldVisit(String jsonPath)
    {
        return shouldVisitSet.contains(jsonPath);
    }

    String newArrayJsonPath(String rootPath, int i)
    {
        String newPath = new StringBuilder(rootPath).append("[").append(Integer.toString(i)).append("]").toString();
        if (! shouldVisit(newPath)) {
            newPath = new StringBuilder(rootPath).append("[*]").toString(); // try [*] too
        }
        return newPath;
    }

    String newMapJsonPath(String rootPath, Value elementPathValue)
    {
        String elementPath = elementPathValue.asStringValue().asString();
        String newPath = new StringBuilder(rootPath).append("['").append(elementPath).append("']").toString();
        return newPath;
    }

    Value visitArray(String rootPath, ArrayValue arrayValue)
    {
        int size = arrayValue.size();
        ArrayList<Value> newValue = new ArrayList<>(size);
        int j = 0;
        if (this.jsonDropColumns.containsKey(rootPath)) {
            HashSet<String> jsonDropColumns = this.jsonDropColumns.get(rootPath);
            for (int i = 0; i < size; i++) {
                String newPath = newArrayJsonPath(rootPath, i);
                if (! jsonDropColumns.contains(newPath)) {
                    Value v = arrayValue.get(i);
                    newValue.add(j++, visit(newPath, v));
                }
            }
        }
        else if (this.jsonColumns.containsKey(rootPath)) {
            for (JsonColumn jsonColumn : this.jsonColumns.get(rootPath).values()) {
                int src = jsonColumn.getSrcTailIndex().intValue();
                Value v = (src < arrayValue.size() ? arrayValue.get(src) : null);
                if (v == null) {
                    v = jsonColumn.getDefaultValue();
                }
                String newPath = jsonColumn.getPath();
                Value visited = visit(newPath, v);
                // int i = jsonColumn.getTailIndex().intValue();
                // index is shifted, so j++ is used.
                newValue.add(j++, visited == null ? ValueFactory.newNil() : visited);
            }
        }
        else {
            for (int i = 0; i < size; i++) {
                String newPath = newArrayJsonPath(rootPath, i);
                Value v = arrayValue.get(i);
                newValue.add(j++, visit(newPath, v));
            }
        }
        if (this.jsonAddColumns.containsKey(rootPath)) {
            for (JsonColumn jsonColumn : this.jsonAddColumns.get(rootPath).values()) {
                int i = jsonColumn.getTailIndex().intValue();
                if (0 <= i && i < size) {
                    continue; // possibly already visit, avoid duplication
                }
                int src = jsonColumn.getSrcTailIndex().intValue();
                Value v = (src < arrayValue.size() ? arrayValue.get(src) : null);
                if (v == null) {
                    v = jsonColumn.getDefaultValue();
                }
                String newPath = jsonColumn.getPath();
                Value visited = visit(newPath, v);
                // this ignores specified index, but appends to last now
                newValue.add(j++, visited == null ? ValueFactory.newNil() : visited);
            }
        }
        return ValueFactory.newArray(newValue.toArray(new Value[0]), true);
    }

    Value visitMap(String rootPath, MapValue mapValue)
    {
        int size = mapValue.size();
        int i = 0;
        ArrayList<Value> newValue = new ArrayList<>(size * 2);
        if (this.jsonDropColumns.containsKey(rootPath)) {
            HashSet<String> jsonDropColumns = this.jsonDropColumns.get(rootPath);
            for (Map.Entry<Value, Value> entry : mapValue.entrySet()) {
                Value k = entry.getKey();
                Value v = entry.getValue();
                String newPath = newMapJsonPath(rootPath, k);
                if (! jsonDropColumns.contains(newPath)) {
                    Value visited = visit(newPath, v);
                    newValue.add(i++, k);
                    newValue.add(i++, visited);
                }
            }
        }
        else if (this.jsonColumns.containsKey(rootPath)) {
            Map<Value, Value> map = mapValue.map();
            for (JsonColumn jsonColumn : this.jsonColumns.get(rootPath).values()) {
                Value src = jsonColumn.getSrcTailNameValue();
                Value v = map.get(src);
                if (v == null) {
                    v = jsonColumn.getDefaultValue();
                }
                String newPath = jsonColumn.getPath();
                Value visited = visit(newPath, v);
                newValue.add(i++, jsonColumn.getTailNameValue());
                newValue.add(i++, visited == null ? ValueFactory.newNil() : visited);
            }
        }
        else {
            for (Map.Entry<Value, Value> entry : mapValue.entrySet()) {
                Value k = entry.getKey();
                Value v = entry.getValue();
                String newPath = newMapJsonPath(rootPath, k);
                Value visited = visit(newPath, v);
                newValue.add(i++, k);
                newValue.add(i++, visited);
            }
        }
        if (this.jsonAddColumns.containsKey(rootPath)) {
            Map<Value, Value> map = mapValue.map();
            for (JsonColumn jsonColumn : this.jsonAddColumns.get(rootPath).values()) {
                Value k = jsonColumn.getTailNameValue();
                if (map.containsKey(k)) {
                    continue; // possibly already visit, avoid duplication
                }
                Value src = jsonColumn.getSrcTailNameValue();
                Value v = map.get(src);
                if (v == null) {
                    v = jsonColumn.getDefaultValue();
                }
                String newPath = jsonColumn.getPath();
                Value visited = visit(newPath, v);
                newValue.add(i++, jsonColumn.getTailNameValue());
                newValue.add(i++, visited == null ? ValueFactory.newNil() : visited);
            }
        }
        return ValueFactory.newMap(newValue.toArray(new Value[0]), true);
    }

    public Value visit(String rootPath, Value value)
    {
        if (! shouldVisit(rootPath)) {
            return value;
        }
        if (value == null) {
            return null;
        }
        else if (value.isArrayValue()) {
            return visitArray(rootPath, value.asArrayValue());
        }
        else if (value.isMapValue()) {
            return visitMap(rootPath, value.asMapValue());
        }
        else {
            return value;
        }
    }
}
