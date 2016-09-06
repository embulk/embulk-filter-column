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
import static org.embulk.filter.column.path.PathConstants.SINGLE_QUOTE;
import static org.embulk.filter.column.path.PathConstants.DOUBLE_QUOTE;

public class PropertyPathToken extends PathToken
{

    private final String property;
    private final String stringDelimiter;
    private final boolean singleQuote;

    // singleQuote ? "'" : "\""
    public PropertyPathToken(String property, boolean singleQuote)
    {
        if (property.isEmpty()) {
            throw new InvalidPathException("Empty property");
        }
        this.property = property;
        this.stringDelimiter = singleQuote ? Character.toString(SINGLE_QUOTE) : Character.toString(DOUBLE_QUOTE);
        this.singleQuote = singleQuote;
    }

    public String getProperty() { return property; }

    @Override
    public String getPathFragment()
    {
        return new StringBuilder()
                .append("[")
                .append(stringDelimiter)
                .append(Utils.escape(property, singleQuote))
                .append(stringDelimiter)
                .append("]").toString();
    }
}
