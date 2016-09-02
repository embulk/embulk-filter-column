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
