package com.btoddb.chronicle;

/*
 * #%L
 * fast-persistent-queue
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;


/**
 *
 */
public class TokenizedFormatter {
    private String delimiter = ":";

    private String pattern;
    private List<TokenizingPart> compiledPattern;

    public TokenizedFormatter(String pattern) {
        this.pattern = pattern;
        this.compiledPattern = compilePattern();
    }

    public String render(Event event) {
        return render(event, null);
    }

    public String render(TokenValueProvider provider) {
        return render(null, provider);
    }

    public String render(Event event, TokenValueProvider provider) {
        StringBuilder sb = new StringBuilder();
        for (TokenizingPart part : compiledPattern) {
            // if string, then just append it
            if (part instanceof StringPart) {
                sb.append(part.raw);
            }
            // if a header part, then lookup in event headers
            else if (null != event && part instanceof HeaderPart) {
                sb.append(((HeaderPart)part).render(event));
            }
            // if key is found in headers, then use it.  otherwise let it be
            else if (part instanceof ProviderPart && null != provider
                    && provider.canRender(((ProviderPart)part).field)) {
                sb.append(((ProviderPart)part).render(provider));
            }
            else {
                sb.append(part.raw);
            }
        }
        return sb.toString();
    }

    private List<TokenizingPart> compilePattern() {
        List<TokenizingPart> compiledList = new ArrayList<>();

        int startIndex = 0;
        int endIndex = 0;

        while (-1 != startIndex && -1 != endIndex && endIndex < pattern.length()) {
            // find 'start of token'
            startIndex = pattern.indexOf("${", endIndex);

            // save the 'not-token' part
            if (-1 != startIndex) {
                if (0 < startIndex) {
                    compiledList.add(new StringPart(pattern.substring(endIndex, startIndex)));
                }
                startIndex += 2;

                // find 'end of token'
                endIndex = pattern.indexOf("}", startIndex);

                // replace the token
                String raw = pattern.substring(startIndex-2, endIndex+1);
                String token = pattern.substring(startIndex, endIndex);

                // get the parts
                String[] tokenParts = token.split(delimiter);
                String formatter = null;
                if (3 == tokenParts.length) {
                    formatter = tokenParts[2];
                }
                if (tokenParts[0].equalsIgnoreCase("header")) {
                    compiledList.add(pickHeaderType(raw, tokenParts[1], formatter));
                }
                else if (token.startsWith("provider")) {
                    compiledList.add(new ProviderPart(raw, tokenParts[1], formatter));
                }
                else {
                    throw new ChronicleException("unknown qualifier on token, "+token);
                }

                endIndex++;
            }
            else {
                compiledList.add(new StringPart(pattern.substring(endIndex)));
            }
        }
        return compiledList;
    }

    private TokenizingPart pickHeaderType(String raw, String header, String formatter) {
        if ("date".equals(formatter)) {
            return new HeaderDatePart(raw, header);
        }
        else {
            return new HeaderStringPart(raw, header);
        }
    }

    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o, false);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public List<TokenizingPart> getCompiledPattern() {
        return compiledPattern;
    }

// ----------

    abstract class TokenizingPart {
        final String raw;
        TokenizingPart(String raw) {
            this.raw = raw;
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    class StringPart extends TokenizingPart {
        StringPart(String raw) {
            super(raw);
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    abstract class HeaderPart extends TokenizingPart {
        final String header;
        HeaderPart(String raw, String header) {
            super(raw);
            this.header = header;
        }

        public abstract String render(Event event);

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    class HeaderStringPart extends HeaderPart {
        HeaderStringPart(String raw, String header) {
            super(raw, header);
        }

        @Override
        public String render(Event event) {
            return event.getHeaders().get(header);
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    class HeaderDatePart extends HeaderPart {
        private final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);

        HeaderDatePart(String raw, String header) {
            super(raw, header);
        }

        @Override
        public String render(Event event) {
            return fmt.print(Long.parseLong(event.getHeaders().get(header)));
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }
    class ProviderPart extends TokenizingPart {
        final String field;
        final String formatter;
        ProviderPart(String raw, String field, String formatter) {
            super(raw);
            this.field = field;
            this.formatter = formatter;
        }

        public String render(TokenValueProvider provider) {
            return provider.render(field);
        }
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

}
