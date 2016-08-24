package org.embulk.filter.column.path;

public class WildcardPathToken extends PathToken
{
    WildcardPathToken()
    {
    }

    @Override
    public String getPathFragment()
    {
        return "[*]";
    }
}
