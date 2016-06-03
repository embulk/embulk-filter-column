package org.embulk.filter.column;

import org.embulk.config.ConfigException;
import org.embulk.spi.type.Type;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class JsonColumn
{
    private final String path;
    private final Type type;
    private final Value defaultValue;
    private final String src;

    private StringValue pathValue = null;
    private String parentPath = null;
    private String baseName = null;
    private Long baseIndex = null;
    private StringValue parentPathValue = null;
    private StringValue baseNameValue = null;

    private StringValue srcValue = null;
    private String srcParentPath = null;
    private String srcBaseName = null;
    private Long srcBaseIndex = null;
    private StringValue srcParentPathValue = null;
    private StringValue srcBaseNameValue = null;

    public JsonColumn(String path, Type type)
    {
        this(path, type, null, null);
    }

    public JsonColumn(String path, Type type, Value defaultValue)
    {
        this(path, type, defaultValue, null);
    }

    public JsonColumn(String path, Type type, Value defaultValue, String src)
    {
        this.path = path;
        this.type = type;
        this.defaultValue = (defaultValue == null ? ValueFactory.newNil() : defaultValue);
        this.src = (src == null ? path : src);

        this.pathValue = ValueFactory.newString(path);
        this.parentPath = parentPath(path);
        this.baseName = baseName(path);
        this.baseIndex = baseIndex(path);
        this.parentPathValue = ValueFactory.newString(parentPath);
        this.baseNameValue = ValueFactory.newString(baseName);

        this.srcValue = ValueFactory.newString(this.src);
        this.srcParentPath = parentPath(this.src);
        this.srcBaseName = baseName(this.src);
        this.srcBaseIndex = baseIndex(this.src);
        this.srcParentPathValue = ValueFactory.newString(this.srcParentPath);
        this.srcBaseNameValue = ValueFactory.newString(this.srcBaseName);

        if (! srcParentPath.equals(parentPath)) {
            throw new ConfigException(String.format("The branch (parent path) of src \"%s\" must be same with of name \"%s\" yet", src, path));
        }
    }

    public String getPath()
    {
        return path;
    }

    public Type getType()
    {
        return type;
    }

    public Value getDefaultValue()
    {
        return defaultValue;
    }

    public String getSrc()
    {
        return src;
    }

    public StringValue getPathValue()
    {
        return pathValue;
    }

    public String getParentPath()
    {
        return parentPath;
    }

    public String getBaseName()
    {
        return baseName;
    }

    public Long getBaseIndex()
    {
        return baseIndex;
    }

    public StringValue getParentPathValue()
    {
        return parentPathValue;
    }

    public StringValue getBaseNameValue()
    {
        return baseNameValue;
    }

    public StringValue getSrcValue()
    {
        return srcValue;
    }

    public String getSrcParentPath()
    {
        return srcParentPath;
    }

    public String getSrcBaseName()
    {
        return srcBaseName;
    }

    public Long getSrcBaseIndex()
    {
        return srcBaseIndex;
    }

    public StringValue getSrcParentPathValue()
    {
        return srcParentPathValue;
    }

    public StringValue getSrcBaseNameValue()
    {
        return srcBaseNameValue;
    }

    // like File.dirname
    public static String parentPath(String path)
    {
        String[] parts = path.split("\\.");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            builder.append(".").append(parts[i]);
        }
        if (parts[parts.length - 1].contains("[")) {
            String[] arrayParts = parts[parts.length - 1].split("\\[");
            builder.append(".").append(arrayParts[0]);
            for (int j = 1; j < arrayParts.length - 1; j++) {
                builder.append("[").append(arrayParts[j]);
            }
        }
        return builder.deleteCharAt(0).toString();
    }

    public static String baseName(String path)
    {
        String[] parts = path.split("\\.");
        String[] arrayParts = parts[parts.length - 1].split("\\[");
        if (arrayParts.length == 1) { // no [i]
            return arrayParts[arrayParts.length - 1];
        }
        else {
            return "[" + arrayParts[arrayParts.length - 1];
        }
    }

    public static Long baseIndex(String path)
    {
        String baseName = baseName(path);
        if (baseName.startsWith("[") && baseName.endsWith("]")) {
            String baseIndex = baseName.substring(1, baseName.length() - 1);
            try {
                return Long.parseLong(baseIndex);
            }
            catch (NumberFormatException e) {
                return null;
            }
        }
        else {
            return null;
        }
    }
}
