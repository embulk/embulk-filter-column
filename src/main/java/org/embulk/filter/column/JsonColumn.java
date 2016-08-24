package org.embulk.filter.column;

import org.embulk.config.ConfigException;
import org.embulk.filter.column.path.ArrayPathToken;
import org.embulk.filter.column.path.CompiledPath;
import org.embulk.filter.column.path.PathCompiler;
import org.embulk.filter.column.path.PathToken;
import org.embulk.filter.column.path.PropertyPathToken;
import org.embulk.spi.type.Type;
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
    private Long tailIndex = null;
    private StringValue parentPathValue = null;
    private StringValue tailNameValue = null;

    private StringValue srcValue = null;
    private String srcParentPath = null;
    private Long srcTailIndex = null;
    private StringValue srcParentPathValue = null;
    private StringValue srcTailNameValue = null;

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
        CompiledPath compiledPath = PathCompiler.compile(path);
        CompiledPath compiledSrc = src == null ? compiledPath : PathCompiler.compile(src);
        this.path = compiledPath.toString();
        this.type = type;
        this.defaultValue = (defaultValue == null ? ValueFactory.newNil() : defaultValue);
        this.src = compiledSrc.toString();

        this.pathValue = ValueFactory.newString(path);
        this.parentPath = compiledPath.getParentPath();
        if (compiledPath.getTailPath().equals("[*]")) {
            throw new ConfigException(String.format("%s wrongly ends with [*], perhaps you can remove the [*]", path));
        }
        this.tailIndex = compiledPath.tailIndex();
        this.parentPathValue = ValueFactory.newString(parentPath);
        String tailName = getTailName(compiledPath);
        this.tailNameValue = ValueFactory.newString(tailName);

        this.srcValue = ValueFactory.newString(this.src);
        this.srcParentPath = compiledSrc.getParentPath();
        this.srcTailIndex = compiledSrc.tailIndex();
        this.srcParentPathValue = ValueFactory.newString(this.srcParentPath);
        String srcTailName = getTailName(compiledSrc);
        this.srcTailNameValue = ValueFactory.newString(srcTailName);

        if (! srcParentPath.equals(parentPath)) {
            throw new ConfigException(String.format("The branch (parent path) of src \"%s\" must be same with of name \"%s\" yet", src, path));
        }
    }

    // $['foo'] or $.foo => foo
    // $['foo'][0] or $.foo[0] => [0]
    private String getTailName(CompiledPath path) {
      if (path.getTail() instanceof PropertyPathToken) {
          return ((PropertyPathToken) path.getTail()).getProperty();
      } else {
          return path.getTailPath();
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

    public Long getTailIndex()
    {
        return tailIndex;
    }

    public StringValue getParentPathValue()
    {
        return parentPathValue;
    }

    public StringValue getTailNameValue()
    {
        return tailNameValue;
    }

    public StringValue getSrcValue()
    {
        return srcValue;
    }

    public String getSrcParentPath()
    {
        return srcParentPath;
    }

    public Long getSrcTailIndex()
    {
        return srcTailIndex;
    }

    public StringValue getSrcParentPathValue()
    {
        return srcParentPathValue;
    }

    public StringValue getSrcTailNameValue()
    {
        return srcTailNameValue;
    }

    // like File.dirname
    public static String parentPath(String path)
    {
        return PathCompiler.compile(path).getParentPath();
    }

    public static String tailName(String path)
    {
        return PathCompiler.compile(path).getTailPath();
    }

    public static Long tailIndex(String path)
    {
        PathToken pathToken = PathCompiler.compile(path).getTail();
        if (pathToken instanceof ArrayPathToken) {
            return ((ArrayPathToken) pathToken).index().longValue();
        } else {
            return null;
        }
    }
}
