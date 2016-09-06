/*
 * Copyright 2011 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.embulk.filter.column.path;

public class PathTokenFactory
{

    public static CompiledPath createRootPathToken(char token)
    {
        return new CompiledPath(token);
    }

    public static PathToken createPropertyPathToken(String property, boolean singleQuote)
    {
        return new PropertyPathToken(property, singleQuote);
    }

    public static PathToken createIndexArrayPathToken(final Integer index)
    {
        return new ArrayPathToken(index);
    }

    public static PathToken createWildCardPathToken()
    {
        return new WildcardPathToken();
    }

}
