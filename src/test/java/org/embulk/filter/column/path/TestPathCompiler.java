package org.embulk.filter.column.path;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.column.ColumnFilterPlugin;
import org.embulk.filter.column.ColumnFilterPlugin.ColumnConfig;
import org.embulk.filter.column.path.ArrayPathToken;
import org.embulk.filter.column.path.PathCompiler;
import org.embulk.filter.column.path.PropertyPathToken;
import org.embulk.spi.Exec;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPathCompiler
{
    @Test
    public void isJsonPathNotation_DotNotation()
    {
        String path = "$.json";
        assert (PathCompiler.isJsonPathNotation(path));
    }

    @Test
    public void isJsonPathNotation_BracketNotation()
    {
        String path = "$['json']";
        assert (PathCompiler.isJsonPathNotation(path));
    }

    @Test
    public void compile()
    {
        assertEquals("$['json1']", PathCompiler.compile("$.json1").toString());
        assertEquals("$['json1']['a']", PathCompiler.compile("$.json1['a']").toString());
    }

    @Test
    public void compile_ArrayPathToken()
    {
        assertEquals("$['json1'][0]", PathCompiler.compile("$.json1[0]").toString());
        assertEquals("$['json1'][0]['a']", PathCompiler.compile("$.json1[0].a").toString());
    }

    @Test
    public void compile_WildCardPathToken()
    {
        assertEquals("$['json1'][*]", PathCompiler.compile("$.json1[*]").toString());
        assertEquals("$['json1'][*]['a']", PathCompiler.compile("$.json1[*].a").toString());
    }

    @Test
    public void compile_Escaped()
    {
        // double quotes converted to single quotes internally
        assertEquals("$['foo\'bar']", PathCompiler.compile("$[\"foo\\\'bar\"]").toString());
        assertEquals("$['foo\"bar']", PathCompiler.compile("$['foo\\\"bar']").toString());
    }

    @Test
    public void pathToken()
    {
        assertEquals("['json1']", PathCompiler.compile("$.json1").next().getPathFragment().toString());
        assertTrue(PathCompiler.compile("$.json1").next().isLeaf());
        assertTrue((PathCompiler.compile("$.json1").next() instanceof PropertyPathToken));
        assertEquals("[0]", PathCompiler.compile("$.json1[0].a").next().next().getPathFragment().toString());
        assertEquals("['a']", PathCompiler.compile("$.json1[0].a").next().next().next().getPathFragment().toString());
        assertTrue((PathCompiler.compile("$.json1[0].a").next().next() instanceof ArrayPathToken));
        assertTrue((PathCompiler.compile("$.json1[0].a").next().next().next() instanceof PropertyPathToken));
        assertEquals("$['json1'][0]", PathCompiler.compile("$.json1[0].a").getParentPath());
    }
}
