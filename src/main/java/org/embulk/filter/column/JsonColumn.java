package org.embulk.filter.column;

import io.github.medjed.jsonpathcompiler.expressions.Path;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayIndexOperation;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import io.github.medjed.jsonpathcompiler.expressions.path.PathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.RootPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PropertyPathToken;
import org.embulk.config.ConfigException;
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
    private Value tailNameValue = null;

    private StringValue srcValue = null;
    private String srcParentPath = null;
    private Long srcTailIndex = null;
    private StringValue srcParentPathValue = null;
    private Value srcTailNameValue = null;

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
        Path compiledPath = PathCompiler.compile(path);
        Path compiledSrc = src == null ? compiledPath : PathCompiler.compile(src);
        RootPathToken compiledRoot = (RootPathToken) compiledPath.getRoot();
        RootPathToken compiledSrcRoot = (RootPathToken) compiledSrc.getRoot();
        this.path = compiledPath.toString();
        this.type = type;
        this.defaultValue = (defaultValue == null ? ValueFactory.newNil() : defaultValue);
        this.src = compiledSrc.toString();

        this.pathValue = ValueFactory.newString(path);
        this.parentPath = compiledPath.getParentPath();

        this.tailIndex = tailIndex(compiledRoot);
        this.parentPathValue = ValueFactory.newString(parentPath);
        String tailName = getTailName(compiledRoot);
        this.tailNameValue = tailName == null ? ValueFactory.newNil() : ValueFactory.newString(tailName);

        this.srcValue = ValueFactory.newString(this.src);
        this.srcParentPath = compiledSrc.getParentPath();
        this.srcTailIndex = tailIndex(compiledSrcRoot);
        this.srcParentPathValue = ValueFactory.newString(this.srcParentPath);
        String srcTailName = getTailName(compiledSrcRoot);
        this.srcTailNameValue = srcTailName == null ? ValueFactory.newNil() : ValueFactory.newString(srcTailName);

        if (!srcParentPath.equals(parentPath)) {
            throw new ConfigException(String.format("The branch (parent path) of src \"%s\" must be same with of name \"%s\" yet", src, path));
        }
    }

    // $['foo'] or $.foo => foo
    // $['foo'][0] or $.foo[0] or $['foo'][*] or $.foo[*] => null
    private String getTailName(RootPathToken root)
    {
        PathToken pathToken = root.getTail();
        if (pathToken instanceof PropertyPathToken) {
            if (!((PropertyPathToken) pathToken).singlePropertyCase()) {
                throw new ConfigException(String.format("Multiple property is not supported \"%s\"", root.toString()));
            }
            return ((PropertyPathToken) pathToken).getProperties().get(0);
        }
        else {
            return null;
        }
    }

    private Long tailIndex(RootPathToken root)
    {
        PathToken tail = root.getTail();
        if (tail instanceof ArrayPathToken) {
            ArrayIndexOperation arrayIndexOperation = ((ArrayPathToken) tail).getArrayIndexOperation();
            JsonPathTokenUtil.assertSupportedArrayPathToken(arrayIndexOperation, path);
            return arrayIndexOperation.indexes().get(0).longValue();
        }
        else {
            return null;
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

    public Long tailIndex()
    {
        return tailIndex;
    }

    public StringValue getParentPathValue()
    {
        return parentPathValue;
    }

    public Value getTailNameValue()
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

    public Value getSrcTailNameValue()
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
        return ((RootPathToken) PathCompiler.compile(path).getRoot()).getTailPath();
    }

    public static Long tailIndex(String path)
    {
        Path compiledPath = PathCompiler.compile(path);
        PathToken tail = ((RootPathToken) compiledPath.getRoot()).getTail();
        if (tail instanceof ArrayPathToken) {
            ArrayIndexOperation arrayIndexOperation = ((ArrayPathToken) tail).getArrayIndexOperation();
            if (arrayIndexOperation == null) {
                throw new ConfigException(String.format("Array Slice Operation is not supported \"%s\"", path));
            }
            if (arrayIndexOperation.isSingleIndexOperation()) {
                return arrayIndexOperation.indexes().get(0).longValue();
            }
            else {
                throw new ConfigException(String.format("Multi Array Indexes is not supported \"%s\"", path));
            }
        }
        else {
            return null;
        }
    }
}
