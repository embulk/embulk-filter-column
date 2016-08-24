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
        CompiledPath compiledPath = PathCompiler.compile(path);
        CompiledPath compiledSrc = src == null ? compiledPath : PathCompiler.compile(src);
        this.path = compiledPath.toString();
        this.type = type;
        this.defaultValue = (defaultValue == null ? ValueFactory.newNil() : defaultValue);
        this.src = compiledSrc.toString();

        this.pathValue = ValueFactory.newString(path);
        this.parentPath = compiledPath.getParentPath();
        this.baseName = compiledPath.getTailPath();
        if (this.baseName.equals("[*]")) {
            throw new ConfigException(String.format("%s wrongly ends with [*], perhaps you can remove the [*]", path));
        }
        this.baseIndex = compiledPath.baseIndex();
        this.parentPathValue = ValueFactory.newString(parentPath);
        String baseName = getPropertyOrBaseName(compiledPath, this.baseName);
        this.baseNameValue = ValueFactory.newString(baseName);

        this.srcValue = ValueFactory.newString(this.src);
        this.srcParentPath = compiledSrc.getParentPath();
        this.srcBaseName = compiledSrc.getTailPath();
        this.srcBaseIndex = compiledSrc.baseIndex();
        this.srcParentPathValue = ValueFactory.newString(this.srcParentPath);
        String srcBaseName = getPropertyOrBaseName(compiledSrc, this.srcBaseName);
        this.srcBaseNameValue = ValueFactory.newString(srcBaseName);

        if (! srcParentPath.equals(parentPath)) {
            throw new ConfigException(String.format("The branch (parent path) of src \"%s\" must be same with of name \"%s\" yet", src, path));
        }
    }

    private String getPropertyOrBaseName(CompiledPath path, String baseName) {
      if (path.getTail() instanceof PropertyPathToken) {
          return ((PropertyPathToken) path.getTail()).getProperty();
      } else {
          return baseName;
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
        return PathCompiler.compile(path).getParentPath();
    }

    public static String baseName(String path)
    {
        return PathCompiler.compile(path).getTailPath();
    }

    public static Long baseIndex(String path)
    {
        PathToken pathToken = PathCompiler.compile(path).getTail();
        if (pathToken instanceof ArrayPathToken) {
            return ((ArrayPathToken) pathToken).index().longValue();
        } else {
            return null;
        }
    }
}
