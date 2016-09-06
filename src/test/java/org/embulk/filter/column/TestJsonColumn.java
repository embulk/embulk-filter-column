package org.embulk.filter.column;

import org.embulk.spi.type.Types;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestJsonColumn
{
    @Test
    public void initialize()
    {
        try {
            JsonColumn column = new JsonColumn("$.foo.bar", Types.BOOLEAN);
            assertEquals("$['foo']['bar']", column.getSrc());
            assertEquals(ValueFactory.newNil(), column.getDefaultValue());
        }
        catch (Exception e) {
            fail();
        }

        try {
            Value defaultValue = ValueFactory.newBoolean(true);
            JsonColumn column = new JsonColumn("$['foo']['bar']", Types.BOOLEAN, defaultValue);
            assertEquals("$['foo']['bar']", column.getSrc());
            assertEquals(defaultValue, column.getDefaultValue());
        }
        catch (Exception e) {
            fail();
        }
    }

    @Test
    public void parentPath()
    {
        assertEquals("$['foo']['bar']", JsonColumn.parentPath("$.foo.bar.baz"));
        assertEquals("$['foo']", JsonColumn.parentPath("$.foo.bar"));
        assertEquals("$", JsonColumn.parentPath("$['foo']"));
        assertEquals("$['foo'][0]", JsonColumn.parentPath("$.foo[0][1]"));
        assertEquals("$['foo']", JsonColumn.parentPath("$.foo[0]"));
        assertEquals("$", JsonColumn.parentPath("$[0]"));
    }

    @Test
    public void baseName()
    {
        assertEquals("['baz']", JsonColumn.baseName("$['foo'].bar.baz"));
        assertEquals("['bar']", JsonColumn.baseName("$.foo.bar"));
        assertEquals("['foo']", JsonColumn.baseName("$.foo"));
        assertEquals("[1]", JsonColumn.baseName("$.foo[0][1]"));
        assertEquals("[0]", JsonColumn.baseName("$.foo[0]"));
        assertEquals("[0]", JsonColumn.baseName("$[0]"));
    }

    @Test
    public void getBaseNameValue()
    {
        assertEquals("baz", new JsonColumn("$['foo'].bar.baz", Types.BOOLEAN).getBaseNameValue().toString());
        assertEquals("bar", new JsonColumn("$.foo.bar", Types.BOOLEAN).getBaseNameValue().toString());
        assertEquals("foo", new JsonColumn("$.foo", Types.BOOLEAN).getBaseNameValue().toString());
        assertEquals("[1]", new JsonColumn("$.foo[0][1]", Types.BOOLEAN).getBaseNameValue().toString());
        assertEquals("[0]", new JsonColumn("$.foo[0]", Types.BOOLEAN).getBaseNameValue().toString());
        assertEquals("[0]", new JsonColumn("$[0]", Types.BOOLEAN).getBaseNameValue().toString());
    }

    @Test
    public void baseIndex()
    {
        assertEquals(null, JsonColumn.baseIndex("$['foo'].bar.baz"));
        assertEquals(null, JsonColumn.baseIndex("$.foo.bar"));
        assertEquals(null, JsonColumn.baseIndex("$.foo"));
        assertEquals(new Long(1), JsonColumn.baseIndex("$.foo[0][1]"));
        assertEquals(new Long(0), JsonColumn.baseIndex("$.foo[0]"));
        assertEquals(new Long(0), JsonColumn.baseIndex("$[0]"));
    }
}
