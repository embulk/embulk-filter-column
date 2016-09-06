package org.embulk.filter.column.path;

import org.embulk.filter.column.path.CompiledPath;
import org.embulk.filter.column.path.PathCompiler;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCompiledPath
{
    @Test
    public void parentPath()
    {
        CompiledPath compiledPath = PathCompiler.compile("$.json1.a");
        CompiledPath compiledPathWithArrayIndex = PathCompiler.compile("$.json1[0].a");
        CompiledPath compiledPathWithWildCard = PathCompiler.compile("$.json1[*].a");

        assertEquals("$['json1']", compiledPath.getParentPath());
        assertEquals("$['json1'][0]", compiledPathWithArrayIndex.getParentPath());
        assertEquals("$['json1'][*]", compiledPathWithWildCard.getParentPath());
    }
}
