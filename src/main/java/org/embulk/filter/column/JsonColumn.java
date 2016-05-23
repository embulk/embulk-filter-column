package org.embulk.filter.column;

import org.embulk.spi.type.Type;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class JsonColumn
{
    private final String name;
    private final Type type;
    private final Value defaultValue;
    private Value nameValue = null;
    private String objectPath = null;
    private String elementPath = null;

    public JsonColumn(
            String name,
            Type type,
            Value defaultValue)
    {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.nameValue = ValueFactory.newString(name);
        this.objectPath = objectPath(name);
        this.elementPath = elementPath(name);
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public Value getDefaultValue()
    {
        return defaultValue;
    }

    public Value getNameValue()
    {
        return nameValue;
    }

    public String getObjectPath()
    {
        return objectPath;
    }

    public String getElementPath()
    {
        return elementPath;
    }

    public static String objectPath(String path)
    {
        String[] parts = path.split("\\.");
        StringBuilder builder = new StringBuilder();
        builder.append(parts[0]);
        for (int i = 1; i < parts.length - 1; i++) {
            builder.append(".").append(parts[i]);
        }
        if (parts[parts.length - 1].contains("[")) {
            String[] arrayParts = parts[parts.length - 1].split("\\[");
            builder.append(".").append(arrayParts[0]);
            for (int j = 1; j < arrayParts.length - 1; j++) {
                builder.append("[").append(arrayParts[j]);
            }
        }
        return builder.toString();
    }

    public static String elementPath(String path)
    {
        String[] parts = path.split("\\.");
        return parts[parts.length - 1];
    }
}
