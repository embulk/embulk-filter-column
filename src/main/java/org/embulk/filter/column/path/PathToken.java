package org.embulk.filter.column.path;

public abstract class PathToken
{
    private PathToken prev;
    private PathToken next;

    PathToken appendTailToken(PathToken next)
    {
        this.next = next;
        this.next.prev = this;
        return next;
    }

    public PathToken prev()
    {
        return prev;
    }

    public PathToken next()
    {
        if (isLeaf()) {
            throw new IllegalStateException("Current path token is a leaf");
        }
        return next;
    }

    boolean isLeaf()
    {
        return next == null;
    }

    boolean isRoot()
    {
        return prev == null;
    }

    public abstract String getPathFragment();

    @Override
    public String toString()
    {
        if (isLeaf()) {
            return getPathFragment();
        } else {
            return getPathFragment() + next().toString();
        }
    }
}
