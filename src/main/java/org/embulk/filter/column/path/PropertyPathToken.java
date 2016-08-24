package org.embulk.filter.column.path;

public class PropertyPathToken extends PathToken
{

    private final String property;
    private final String stringDelimiter;

    public PropertyPathToken(String property, char stringDelimiter)
    {
        if (property.isEmpty()) {
            throw new InvalidPathException("Empty property");
        }
        this.property = property;
        this.stringDelimiter = Character.toString(stringDelimiter);
    }

    public String getProperty() { return property; }

    @Override
    public String getPathFragment()
    {
        return new StringBuilder()
                .append("[")
                .append(stringDelimiter)
                .append(property)
                .append(stringDelimiter)
                .append("]").toString();
    }
}
