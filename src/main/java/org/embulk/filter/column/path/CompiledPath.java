package org.embulk.filter.column.path;

import org.apache.commons.lang3.StringUtils;

// rename from RootPathToken
public class CompiledPath extends PathToken
{

    private PathToken tail;
    private int tokenCount;
    private final String rootToken;


    CompiledPath(char rootToken)
    {
        this.rootToken = Character.toString(rootToken);
        ;
        this.tail = this;
        this.tokenCount = 1;
    }

    public PathToken getTail() { return tail; }

    public String getTailPath() { return tail.toString(); }

    public int getTokenCount() { return tokenCount; }

    public Long tailIndex()
    {
        if (tail instanceof ArrayPathToken) {
            return ((ArrayPathToken) tail).index().longValue();
        } else {
            return null;
        }
    }

    public String getParentPath()
    {
        return StringUtils.removeEnd(this.toString(), tail.toString());
    }

    public CompiledPath append(PathToken next)
    {
        this.tail = tail.appendTailToken(next);
        this.tokenCount++;
        return this;
    }

    public PathTokenAppender getPathTokenAppender()
    {
        return new PathTokenAppender()
        {
            @Override
            public PathTokenAppender appendPathToken(PathToken next)
            {
                append(next);
                return this;
            }
        };
    }

    @Override
    public String getPathFragment()
    {
        return rootToken;
    }

}
