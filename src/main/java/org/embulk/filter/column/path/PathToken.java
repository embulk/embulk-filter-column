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

public abstract class PathToken
{
    private PathToken prev;
    private PathToken next;

    PathToken appendTailToken(PathToken next)
    {
        this.next = next;
        this.next.prev = this;
        return next;
    }

    public PathToken prev()
    {
        return prev;
    }

    public PathToken next()
    {
        if (isLeaf()) {
            throw new IllegalStateException("Current path token is a leaf");
        }
        return next;
    }

    boolean isLeaf()
    {
        return next == null;
    }

    boolean isRoot()
    {
        return prev == null;
    }

    public abstract String getPathFragment();

    @Override
    public String toString()
    {
        if (isLeaf()) {
            return getPathFragment();
        } else {
            return getPathFragment() + next().toString();
        }
    }
}
