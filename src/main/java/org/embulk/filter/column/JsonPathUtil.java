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

import io.github.medjed.jsonpathcompiler.InvalidPathException;
import io.github.medjed.jsonpathcompiler.expressions.Path;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayIndexOperation;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.FunctionPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import io.github.medjed.jsonpathcompiler.expressions.path.PathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PredicatePathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.RootPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.ScanPathToken;
import org.embulk.config.ConfigException;

public class JsonPathUtil
{
    private JsonPathUtil() {}

    public static void assertJsonPathFormat(String path)
    {
        Path compiledPath;
        try {
            compiledPath = PathCompiler.compile(path);
        }
        catch (InvalidPathException e) {
            throw new ConfigException(String.format("jsonpath %s, %s", path, e.getMessage()));
        }
        PathToken pathToken = compiledPath.getRoot();
        while (true) {
            assertSupportedPathToken(pathToken, path);
            if (pathToken.isLeaf()) {
                break;
            }
            pathToken = pathToken.next();
        }
    }

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

    public static void assertDoNotEndsWithArrayWildcard(String path)
    {
        Path compiledPath;
        try {
            compiledPath = PathCompiler.compile(path);
        }
        catch (InvalidPathException e) {
            throw new ConfigException(String.format("jsonpath %s, %s", path, e.getMessage()));
        }
        if (((RootPathToken) compiledPath.getRoot()).getTailPath().equals("[*]")) {
            throw new ConfigException(String.format("%s wrongly ends with [*], perhaps you can remove the [*]", compiledPath.toString()));
        }
    }
}
