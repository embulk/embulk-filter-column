package org.embulk.filter.column.path;
import static org.embulk.filter.column.path.PathConstants.SINGLE_QUOTE;
import static org.embulk.filter.column.path.PathConstants.DOUBLE_QUOTE;

public class PropertyPathToken extends PathToken
{

    private final String property;
    private final String stringDelimiter;
    private final boolean singleQuote;

    // singleQuote ? "'" : "\""
    public PropertyPathToken(String property, boolean singleQuote)
    {
        if (property.isEmpty()) {
            throw new InvalidPathException("Empty property");
        }
        this.property = property;
        this.stringDelimiter = singleQuote ? Character.toString(SINGLE_QUOTE) : Character.toString(DOUBLE_QUOTE);
        this.singleQuote = singleQuote;
    }

    public String getProperty() { return property; }

    @Override
    public String getPathFragment()
    {
        return new StringBuilder()
                .append("[")
                .append(stringDelimiter)
                .append(Utils.escape(property, singleQuote))
                .append(stringDelimiter)
                .append("]").toString();
    }
}
