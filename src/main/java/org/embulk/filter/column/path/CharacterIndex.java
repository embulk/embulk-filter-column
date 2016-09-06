/*
 * Copyright 2011 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.embulk.filter.column.path;

public class CharacterIndex {
    private static final char SPACE = ' ';
    private static final char MINUS = '-';
    private static final char PERIOD = '.';

    private final CharSequence charSequence;
    private int position;

    public CharacterIndex(CharSequence charSequence)
    {
        this.charSequence = charSequence;
        this.position = 0;
    }

    public int length() {
        return charSequence.length();
    }

    public char charAt(int idx) {
        return charSequence.charAt(idx);
    }

    public char currentChar() {
        return charSequence.charAt(position);
    }

    public boolean currentCharIs(char c) {
        return (charSequence.charAt(position) == c);
    }

    public boolean currentIsTail()
    {
        return position >= charSequence.length() - 1;
    }

    public boolean nextCharIs(char c) {
        return inBounds(position + 1) && (charSequence.charAt(position + 1) == c);
    }

    public int incrementPosition(int charCount) {
        return setPosition(position + charCount);
    }

    public int setPosition(int newPosition)
    {
        //position = min(newPosition, charSequence.length() - 1);
        position = newPosition;
        return position;
    }

    public int position(){
        return position;
    }

    public int indexOfNextSignificantChar(char c) {
        return indexOfNextSignificantChar(position, c);
    }

    public int indexOfNextSignificantChar(int startPosition, char c)
    {
        int readPosition = startPosition + 1;
        while (!isOutOfBounds(readPosition) && charAt(readPosition) == SPACE) {
            readPosition++;
        }
        if (charAt(readPosition) == c) {
            return readPosition;
        } else {
            return -1;
        }
    }

    public int nextIndexOf(int startPosition, char c)
    {
        int readPosition = startPosition;
        while (!isOutOfBounds(readPosition)) {
            if (charAt(readPosition) == c) {
                return readPosition;
            }
            readPosition++;
        }
        return -1;
    }

    public boolean nextSignificantCharIs(int startPosition, char c)
    {
        int readPosition = startPosition + 1;
        while (!isOutOfBounds(readPosition) && charAt(readPosition) == SPACE) {
            readPosition++;
        }
        return !isOutOfBounds(readPosition) && charAt(readPosition) == c;
    }

    public boolean nextSignificantCharIs(char c) {
        return nextSignificantCharIs(position, c);
    }

    public char nextSignificantChar() {
        return nextSignificantChar(position);
    }

    public char nextSignificantChar(int startPosition)
    {
        int readPosition = startPosition + 1;
        while (!isOutOfBounds(readPosition) && charAt(readPosition) == SPACE) {
            readPosition++;
        }
        if (!isOutOfBounds(readPosition)) {
            return charAt(readPosition);
        } else {
            return ' ';
        }
    }

    public boolean hasMoreCharacters()
    {
        return inBounds(position + 1);
    }

    public boolean inBounds(int idx) {
        return (idx >= 0) && (idx < charSequence.length());
    }
    public boolean inBounds() {
        return inBounds(position);
    }

    public boolean isOutOfBounds(int idx)
    {
        return !inBounds(idx);
    }

    public CharSequence subSequence(int start, int end) {
        return charSequence.subSequence(start, end);
    }

    public CharSequence charSequence() {
        return charSequence;
    }

    @Override
    public String toString()
    {
        return charSequence.toString();
    }

    public boolean isNumberCharacter(int readPosition)
    {
        char c = charAt(readPosition);
        return Character.isDigit(c) || c == MINUS || c == PERIOD;
    }

    public CharacterIndex skipBlanks()
    {
        while (inBounds() && currentChar() == SPACE) {
            incrementPosition(1);
        }
        return this;
    }
}
