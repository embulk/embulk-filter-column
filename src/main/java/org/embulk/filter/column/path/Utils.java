package org.embulk.filter.column.path;

import java.io.StringWriter;

public final class Utils
{
    public static String escape(String str, boolean escapeSingleQuote) {
        if (str == null) {
            return null;
        }
        int len = str.length();
        StringWriter writer = new StringWriter(len * 2);

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);

            // handle unicode
            if (ch > 0xfff) {
                writer.write("\\u" + hex(ch));
            } else if (ch > 0xff) {
                writer.write("\\u0" + hex(ch));
            } else if (ch > 0x7f) {
                writer.write("\\u00" + hex(ch));
            } else if (ch < 32) {
                switch (ch) {
                    case '\b':
                        writer.write('\\');
                        writer.write('b');
                        break;
                    case '\n':
                        writer.write('\\');
                        writer.write('n');
                        break;
                    case '\t':
                        writer.write('\\');
                        writer.write('t');
                        break;
                    case '\f':
                        writer.write('\\');
                        writer.write('f');
                        break;
                    case '\r':
                        writer.write('\\');
                        writer.write('r');
                        break;
                    default :
                        if (ch > 0xf) {
                            writer.write("\\u00" + hex(ch));
                        } else {
                            writer.write("\\u000" + hex(ch));
                        }
                        break;
                }
            } else {
                switch (ch) {
                    case '\'':
                        if (escapeSingleQuote) {
                            writer.write('\\');
                        }
                        writer.write('\'');
                        break;
                    case '"':
                        writer.write('\\');
                        writer.write('"');
                        break;
                    case '\\':
                        writer.write('\\');
                        writer.write('\\');
                        break;
                    case '/':
                        writer.write('\\');
                        writer.write('/');
                        break;
                    default :
                        writer.write(ch);
                        break;
                }
            }
        }
        return writer.toString();
    }

    public static String unescape(String str) {
        if (str == null) {
            return null;
        }
        int len = str.length();
        StringWriter writer = new StringWriter(len);
        StringBuffer unicode = new StringBuffer(4);
        boolean hadSlash = false;
        boolean inUnicode = false;
        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (inUnicode) {
                unicode.append(ch);
                if (unicode.length() == 4) {
                    try {
                        int value = Integer.parseInt(unicode.toString(), 16);
                        writer.write((char) value);
                        unicode.setLength(0);
                        inUnicode = false;
                        hadSlash = false;
                    } catch (NumberFormatException nfe) {
                        throw new InvalidPathException("Unable to parse unicode value: " + unicode, nfe);
                    }
                }
                continue;
            }
            if (hadSlash) {
                hadSlash = false;
                switch (ch) {
                    case '\\':
                        writer.write('\\');
                        break;
                    case '\'':
                        writer.write('\'');
                        break;
                    case '\"':
                        writer.write('"');
                        break;
                    case 'r':
                        writer.write('\r');
                        break;
                    case 'f':
                        writer.write('\f');
                        break;
                    case 't':
                        writer.write('\t');
                        break;
                    case 'n':
                        writer.write('\n');
                        break;
                    case 'b':
                        writer.write('\b');
                        break;
                    case 'u':
                    {
                        inUnicode = true;
                        break;
                    }
                    default :
                        writer.write(ch);
                        break;
                }
                continue;
            } else if (ch == '\\') {
                hadSlash = true;
                continue;
            }
            writer.write(ch);
        }
        if (hadSlash) {
            writer.write('\\');
        }
        return writer.toString();
    }

    /**
     * Returns an upper case hexadecimal <code>String</code> for the given
     * character.
     *
     * @param ch The character to map.
     * @return An upper case hexadecimal <code>String</code>
     */
    public static String hex(char ch) {
        return Integer.toHexString(ch).toUpperCase();
    }
}
