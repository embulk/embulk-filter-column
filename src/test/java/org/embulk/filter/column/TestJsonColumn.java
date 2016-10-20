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
    public void tailName()
    {
        assertEquals("['baz']", JsonColumn.tailName("$['foo'].bar.baz"));
        assertEquals("['bar']", JsonColumn.tailName("$.foo.bar"));
        assertEquals("['foo']", JsonColumn.tailName("$.foo"));
        assertEquals("[1]", JsonColumn.tailName("$.foo[0][1]"));
        assertEquals("[0]", JsonColumn.tailName("$.foo[0]"));
        assertEquals("[0]", JsonColumn.tailName("$[0]"));
    }

    @Test
    public void getTailNameValue()
    {
        assertEquals(ValueFactory.newString("baz"), new JsonColumn("$['foo'].bar.baz", Types.BOOLEAN).getTailNameValue());
        assertEquals(ValueFactory.newString("bar"), new JsonColumn("$.foo.bar", Types.BOOLEAN).getTailNameValue());
        assertEquals(ValueFactory.newString("foo"), new JsonColumn("$.foo", Types.BOOLEAN).getTailNameValue());
        assertEquals(ValueFactory.newNil(), new JsonColumn("$.foo[0][1]", Types.BOOLEAN).getTailNameValue());
        assertEquals(ValueFactory.newNil(), new JsonColumn("$.foo[0]", Types.BOOLEAN).getTailNameValue());
        assertEquals(ValueFactory.newNil(), new JsonColumn("$[0]", Types.BOOLEAN).getTailNameValue());
    }

    @Test
    public void getTailIndex()
    {
        assertEquals(null, JsonColumn.getTailIndex("$['foo'].bar.baz"));
        assertEquals(null, JsonColumn.getTailIndex("$.foo.bar"));
        assertEquals(null, JsonColumn.getTailIndex("$.foo"));
        assertEquals(Long.valueOf(1), JsonColumn.getTailIndex("$.foo[0][1]"));
        assertEquals(Long.valueOf(0), JsonColumn.getTailIndex("$.foo[0]"));
        assertEquals(Long.valueOf(0), JsonColumn.getTailIndex("$[0]"));
    }
}
