package org.embulk.filter.column.path;

/*
    Porting from https://github.com/jayway/JsonPath
    equivalent version 2.2.0, latest commit is c2c1686

    dropped features
    - Filter
    - Placeholder
    - Function
    - multi properties (ex. $.json1['a','b'])
    - ArrayIndexOperation (ex. $.json1[0,1])
    - ArraySliceOperation (ex. $.json1[1:2] (start:end))
 */

import static org.embulk.filter.column.path.PathConstants.*;
import static java.lang.Character.isDigit;

public class PathCompiler
{

    private final CharacterIndex path;

    public static Boolean isJsonPathNotation(String path)
    {
        StringBuilder dotNotationRootPath = new StringBuilder(DOC_CONTEXT).append(PERIOD);
        StringBuilder bracketNotationRootPath = new StringBuilder(DOC_CONTEXT).append(OPEN_SQUARE_BRACKET);
        return path.contains(dotNotationRootPath.toString()) || path.contains(bracketNotationRootPath.toString());
    }

    private PathCompiler(String path)
    {
        this.path = new CharacterIndex(path);
    }

    private CompiledPath compile()
    {
        CompiledPath root = readContextToken();
        return root;
    }


    public static CompiledPath compile(String path)
    {
        return new PathCompiler(path).compile();
    }

    private void readWhitespace()
    {
        while (path.inBounds()) {
            char c = path.currentChar();
            if (!isWhitespace(c)) {
                break;
            }
            path.incrementPosition(1);
        }
    }

    private boolean isWhitespace(char c)
    {
        return (c == SPACE || c == TAB || c == LF || c == CR);
    }

    private Boolean isDocContext(char c)
    {
        return c == DOC_CONTEXT;
    }

    // $
    private CompiledPath readContextToken()
    {

        readWhitespace();

        if (!isDocContext(path.currentChar())) {
            fail("Path must start with '$'");
        }

        CompiledPath pathToken = PathTokenFactory.createRootPathToken(path.currentChar());
        PathTokenAppender appender = pathToken.getPathTokenAppender();

        if (path.currentIsTail()) {
            return pathToken;
        }

        path.incrementPosition(1);

        if (path.currentChar() != PERIOD && path.currentChar() != OPEN_SQUARE_BRACKET) {
            fail("Illegal character at position " + path.position() + " expected '.' or '[");
        }

        readNextToken(appender);

        return pathToken;
    }

    //
    //
    //
    private boolean readNextToken(PathTokenAppender appender)
    {

        char c = path.currentChar();

        switch (c) {
            case OPEN_SQUARE_BRACKET:
                return readBracketPropertyToken(appender) ||
                        readArrayToken(appender) ||
                        readWildCardToken(appender) ||
                        fail("Could not parse token starting at position " + path.position() + ". Expected ?, ', 0-9, * ");
            case PERIOD:
                return readDotToken(appender) ||
                        fail("Could not parse token starting at position " + path.position());
            case WILDCARD:
                return readWildCardToken(appender) ||
                        fail("Could not parse token starting at position " + path.position());
            default:
                return readPropertyToken(appender) ||
                        fail("Could not parse token starting at position " + path.position());
        }
    }

    //
    // . and ..
    //
    private boolean readDotToken(PathTokenAppender appender)
    {
        if (!path.hasMoreCharacters()) {
            throw new InvalidPathException("Path must not end with a '.");
        } else {
            path.incrementPosition(1);
        }
        if (path.currentCharIs(PERIOD)) {
            throw new InvalidPathException("Character '.' on position " + path.position() + " is not valid.");
        }
        return readNextToken(appender);
    }

    //
    // fooBar or fooBar()
    //
    private boolean readPropertyToken(PathTokenAppender appender)
    {
        if (path.currentCharIs(OPEN_SQUARE_BRACKET) || path.currentCharIs(WILDCARD) || path.currentCharIs(PERIOD) || path.currentCharIs(SPACE)) {
            return false;
        }
        int startPosition = path.position();
        int readPosition = startPosition;
        int endPosition = 0;

        while (path.inBounds(readPosition)) {
            char c = path.charAt(readPosition);
            if (c == SPACE) {
                throw new InvalidPathException("Use bracket notion ['my prop'] if your property contains blank characters. position: " + path.position());
            } else if (c == PERIOD || c == OPEN_SQUARE_BRACKET) {
                endPosition = readPosition;
                break;
            }
            readPosition++;
        }
        if (endPosition == 0) {
            endPosition = path.length();
        }

        path.setPosition(endPosition);

        String property = path.subSequence(startPosition, endPosition).toString();

        appender.appendPathToken(PathTokenFactory.createPropertyPathToken(property, true));

        return path.currentIsTail() || readNextToken(appender);
    }

    //
    // [*]
    // *
    //
    private boolean readWildCardToken(PathTokenAppender appender)
    {

        boolean inBracket = path.currentCharIs(OPEN_SQUARE_BRACKET);

        if (inBracket && !path.nextSignificantCharIs(WILDCARD)) {
            return false;
        }
        if (!path.currentCharIs(WILDCARD) && path.isOutOfBounds(path.position() + 1)) {
            return false;
        }
        if (inBracket) {
            int wildCardIndex = path.indexOfNextSignificantChar(WILDCARD);
            if (!path.nextSignificantCharIs(wildCardIndex, CLOSE_SQUARE_BRACKET)) {
                throw new InvalidPathException("Expected wildcard token to end with ']' on position " + wildCardIndex + 1);
            }
            int bracketCloseIndex = path.indexOfNextSignificantChar(wildCardIndex, CLOSE_SQUARE_BRACKET);
            path.setPosition(bracketCloseIndex + 1);
        } else {
            path.incrementPosition(1);
        }

        appender.appendPathToken(PathTokenFactory.createWildCardPathToken());

        return path.currentIsTail() || readNextToken(appender);
    }

    //
    // [1]
    //
    private boolean readArrayToken(PathTokenAppender appender)
    {

        if (!path.currentCharIs(OPEN_SQUARE_BRACKET)) {
            return false;
        }
        char nextSignificantChar = path.nextSignificantChar();
        if (!isDigit(nextSignificantChar) && nextSignificantChar != MINUS) {
            return false;
        }

        int expressionBeginIndex = path.position() + 1;
        int expressionEndIndex = path.nextIndexOf(expressionBeginIndex, CLOSE_SQUARE_BRACKET);

        if (expressionEndIndex == -1) {
            return false;
        }

        String expression = path.subSequence(expressionBeginIndex, expressionEndIndex).toString().trim();

        if ("*".equals(expression)) {
            return false;
        }

        //check valid chars
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (!isDigit(c) && c != MINUS && c != SPACE) {
                return false;
            }
        }

        boolean isSliceOperation = expression.contains(":");

        if (isSliceOperation) {
            fail("slice is not supported");
        } else {
            appender.appendPathToken(PathTokenFactory.createIndexArrayPathToken(parseInteger(expression)));
        }

        path.setPosition(expressionEndIndex + 1);

        return path.currentIsTail() || readNextToken(appender);
    }

    private static Integer parseInteger(String token)
    {
        try {
            return Integer.parseInt(token);
        } catch (Exception e) {
            throw new InvalidPathException("Failed to parse token in ArrayIndexOperation: " + token, e);
        }
    }

    //
    // ['foo']
    //
    private boolean readBracketPropertyToken(PathTokenAppender appender)
    {
        if (!path.currentCharIs(OPEN_SQUARE_BRACKET)) {
            return false;
        }
        char potentialStringDelimiter = path.nextSignificantChar();
        if (potentialStringDelimiter != SINGLE_QUOTE && potentialStringDelimiter != DOUBLE_QUOTE) {
            return false;
        }

        String property = "";

        int startPosition = path.position() + 1;
        int readPosition = startPosition;
        int endPosition = 0;
        boolean inProperty = false;
        boolean inEscape = false;
        boolean lastSignificantWasComma = false;

        while (path.inBounds(readPosition)) {
            char c = path.charAt(readPosition);

            if (inEscape) {
                inEscape = false;
            } else if ('\\' == c) {
                inEscape = true;
            } else if (c == CLOSE_SQUARE_BRACKET && !inProperty) {
                if (lastSignificantWasComma) {
                    fail("Found empty property at index " + readPosition);
                }
                break;
            } else if (c == potentialStringDelimiter) {
                if (inProperty && !inEscape) {
                    endPosition = readPosition;
                    String prop = path.subSequence(startPosition, endPosition).toString();
                    property = Utils.unescape(prop);
                    inProperty = false;
                } else {
                    startPosition = readPosition + 1;
                    inProperty = true;
                    lastSignificantWasComma = false;
                }

            }
            readPosition++;
        }

        int endBracketIndex = path.indexOfNextSignificantChar(endPosition, CLOSE_SQUARE_BRACKET) + 1;

        if (endBracketIndex < endPosition) {
            fail("endBracketIndex must be greater than endPosition " + path);
        }

        path.setPosition(endBracketIndex);

        appender.appendPathToken(PathTokenFactory.createPropertyPathToken(property, true));

        return path.currentIsTail() || readNextToken(appender);
    }

    public static boolean fail(String message)
    {
        throw new InvalidPathException(message);
    }
}
