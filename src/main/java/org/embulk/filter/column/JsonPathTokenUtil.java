package org.embulk.filter.column;

import com.dena.analytics.jsonpathcompiler.expressions.path.ArrayIndexOperation;
import com.dena.analytics.jsonpathcompiler.expressions.path.ArrayPathToken;
import com.dena.analytics.jsonpathcompiler.expressions.path.FunctionPathToken;
import com.dena.analytics.jsonpathcompiler.expressions.path.PathToken;
import com.dena.analytics.jsonpathcompiler.expressions.path.PredicatePathToken;
import com.dena.analytics.jsonpathcompiler.expressions.path.ScanPathToken;
import org.embulk.config.ConfigException;

public class JsonPathTokenUtil
{
    public static void assertSupportedPathToken(PathToken pathToken, String path)
    {
        if (pathToken instanceof ArrayPathToken) {
            ArrayIndexOperation arrayIndexOperation = ((ArrayPathToken) pathToken).getArrayIndexOperation();
            assertSupportedArrayPathToken(arrayIndexOperation, path);
        }
        else if (pathToken instanceof ScanPathToken) {
            throw new ConfigException(String.format("scan path token is not supported \"%s\"", path));
        }
        else if (pathToken instanceof FunctionPathToken) {
            throw new ConfigException(String.format("function path token is not supported \"%s\"", path));
        }
        else if (pathToken instanceof PredicatePathToken) {
            throw new ConfigException(String.format("predicate path token is not supported \"%s\"", path));
        }
    }

    public static void assertSupportedArrayPathToken(ArrayIndexOperation arrayIndexOperation, String path)
    {
        if (arrayIndexOperation == null) {
            throw new ConfigException(String.format("Array Slice Operation is not supported \"%s\"", path));
        }
        else if (!arrayIndexOperation.isSingleIndexOperation()) {
            throw new ConfigException(String.format("Multi Array Indexes is not supported \"%s\"", path));
        }
    }
}
