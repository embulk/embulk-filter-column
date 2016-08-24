package org.embulk.filter.column.path;

public class ArrayPathToken extends PathToken
{
    private final Integer index;

    public ArrayPathToken(Integer index)
    {
        this.index = index;
    }

    public Integer index() { return index; }

    @Override
    public String getPathFragment()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(index);
        sb.append("]");

        return sb.toString();
    }
}
