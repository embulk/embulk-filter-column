package org.embulk.filter.column.path;

public interface PathTokenAppender
{
    PathTokenAppender appendPathToken(PathToken next);
}
