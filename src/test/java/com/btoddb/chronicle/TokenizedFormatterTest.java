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

import com.btoddb.chronicle.plunkers.hdfs.HdfsTokenValueProviderImpl;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;


public class TokenizedFormatterTest {

    @Test
    public void testCompileNoTokens() {
        List<TokenizedFormatter.TokenizingPart> tokenizer = new TokenizedFormatter("this is the string").getCompiledPattern();
        assertThat(tokenizer, hasSize(1));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFormatter.StringPart.class)));
        assertThat(tokenizer.get(0).raw, is("this is the string"));
    }

    @Test
    public void testCompileWithSingleTokenAmongString() {
        List<TokenizedFormatter.TokenizingPart> tokenizer = new TokenizedFormatter("this ${header:is} the string").getCompiledPattern();
        assertThat(tokenizer, hasSize(3));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFormatter.StringPart.class)));
        assertThat(tokenizer.get(0).raw, is("this "));
        assertThat(tokenizer.get(1), is(instanceOf(TokenizedFormatter.HeaderStringPart.class)));
        assertThat(((TokenizedFormatter.HeaderStringPart)tokenizer.get(1)).header, is("is"));
        assertThat(tokenizer.get(2), is(instanceOf(TokenizedFormatter.StringPart.class)));
        assertThat(tokenizer.get(2).raw, is(" the string"));
    }

    @Test
    public void testCompileWithOnlyAToken() {
        List<TokenizedFormatter.TokenizingPart> tokenizer = new TokenizedFormatter("${header:token}").getCompiledPattern();
        assertThat(tokenizer, hasSize(1));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFormatter.HeaderStringPart.class)));
        assertThat(((TokenizedFormatter.HeaderStringPart)tokenizer.get(0)).header, is("token"));
    }

    @Test
    public void testCompileWithProvider() {
        List<TokenizedFormatter.TokenizingPart> tokenizer = new TokenizedFormatter("${provider:token}").getCompiledPattern();
        assertThat(tokenizer, hasSize(1));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFormatter.ProviderPart.class)));
        assertThat(((TokenizedFormatter.ProviderPart)tokenizer.get(0)).field, is("token"));
    }

    @Test
    public void testRenderSimpleHeaderToken() throws Exception {
        TokenizedFormatter tokenizer = new TokenizedFormatter("tmp/${header:customer}/file");

        String path = tokenizer.render(new Event("the-body")
                                               .withHeader("customer", "the-customer")
                                               .withHeader("foo", "bar"));


        assertThat(path, is("tmp/the-customer/file"));
    }

    @Test
    public void testRenderComplexHeaderToken() throws Exception {
        TokenizedFormatter tokenizer = new TokenizedFormatter("tmp/${header:timestamp:date}/file");

        long now = 1413431922709L;
        String path = tokenizer.render(new Event("the-body")
                                               .withHeader("customer", "the-customer")
                                               .withHeader("timestamp", ""+now)
                                               .withHeader("foo", "bar"));


        assertThat(path, is("tmp/2014-10-16/file"));
    }

    @Test
    public void testRenderUnknownProviderToken() throws Exception {
        TokenizedFormatter tokenizer = new TokenizedFormatter("tmp/${header:customer}/file-${provider:foo-unknown}");

        String path = tokenizer.render(new Event("the-body")
                                               .withHeader("customer", "the-customer")
                                               .withHeader("foo", "bar"),
                                       new HdfsTokenValueProviderImpl());

        assertThat(path, is("tmp/the-customer/file-${provider:foo-unknown}"));
    }

    @Test
    public void testRenderUnknownQualifierThrowsException() throws Exception {
        try {
            TokenizedFormatter tokenizer = new TokenizedFormatter("tmp/${header:customer}/file-${foo-unknown}");
            fail("should have thrown " + ChronicleException.class.getSimpleName() + " because of unknown token qualifier");
        }
        catch (ChronicleException e) {
            assertThat(e.getMessage(), containsString("unknown qualifier"));
        }
    }

    @Test
    public void testCompileWithMultipleTokens() {
        List<TokenizedFormatter.TokenizingPart> tokenizer = new TokenizedFormatter("${header:this} string ${header:is} the string ${provider:theField}").getCompiledPattern();
        assertThat(tokenizer, hasSize(5));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFormatter.HeaderStringPart.class)));
        assertThat(((TokenizedFormatter.HeaderStringPart)tokenizer.get(0)).header, is("this"));
        assertThat(tokenizer.get(1), is(instanceOf(TokenizedFormatter.StringPart.class)));
        assertThat(tokenizer.get(1).raw, is(" string "));
        assertThat(tokenizer.get(2), is(instanceOf(TokenizedFormatter.HeaderStringPart.class)));
        assertThat(((TokenizedFormatter.HeaderStringPart)tokenizer.get(2)).header, is("is"));
        assertThat(tokenizer.get(3), is(instanceOf(TokenizedFormatter.StringPart.class)));
        assertThat(tokenizer.get(3).raw, is(" the string "));
        assertThat(tokenizer.get(4), is(instanceOf(TokenizedFormatter.ProviderPart.class)));
        assertThat(((TokenizedFormatter.ProviderPart)tokenizer.get(4)).field, is("theField"));
    }

}