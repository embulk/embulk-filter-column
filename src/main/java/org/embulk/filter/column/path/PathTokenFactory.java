package org.embulk.filter.column.path;

public class PathTokenFactory
{

    public static CompiledPath createRootPathToken(char token)
    {
        return new CompiledPath(token);
    }

    public static PathToken createPropertyPathToken(String property, char stringDelimiter)
    {
        return new PropertyPathToken(property, stringDelimiter);
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
