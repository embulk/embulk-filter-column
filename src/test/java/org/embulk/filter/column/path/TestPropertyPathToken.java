package org.embulk.filter.column.path;

import org.embulk.filter.column.path.PathTokenFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPropertyPathToken
{
    @Test
    public void createPropetyPathToken()
    {
        assertEquals("['json']", PathTokenFactory.createPropertyPathToken("json", true).toString());
    }
}
