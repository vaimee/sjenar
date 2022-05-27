/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.update;

import static org.apache.jena.query.Syntax.defaultUpdateSyntax ;

import java.io.InputStream ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.irix.IRIs;
import org.apache.jena.irix.IRIx;
import org.apache.jena.query.Syntax ;
import org.apache.jena.sparql.core.Prologue ;
import org.apache.jena.sparql.lang.UpdateParser ;
import org.apache.jena.sparql.modify.UpdateRequestSink ;
import org.apache.jena.sparql.modify.UpdateSink ;
import org.apache.jena.sparql.modify.UsingList ;
import org.apache.jena.sparql.modify.UsingUpdateSink ;
import org.apache.jena.sparql.util.Context;

public class UpdateFactory
{
    /** Create an empty UpdateRequest */
    public static UpdateRequest create(Context ctx) { return new UpdateRequest(ctx) ; }
    
    public static UpdateRequest create() { return new UpdateRequest(null) ; }

    /**  Create an UpdateRequest by parsing from a string.
     * See also <tt>read</tt> operations for parsing contents of a file.
     * @param string    The update request as a string.
     */
    public static UpdateRequest create(String string,Context ctx) {
        return create(string, defaultUpdateSyntax,ctx);
    }

    public static UpdateRequest create(String string) {
        return create(string, defaultUpdateSyntax,null);
    }
    
    /**  Create an UpdateRequest by parsing from a string.
     * See also <tt>read</tt> operations for parsing contents of a file.
     * @param string    The update request as a string.
     * @param syntax    The update language syntax
     */
    public static UpdateRequest create(String string, Syntax syntax,Context ctx) {
        return create(string, null, syntax,ctx);
    }
    public static UpdateRequest create(String string, Syntax syntax) {
        return create(string, null, syntax,null);
    }

    /**  Create an UpdateRequest by parsing from a string.
     * See also <tt>read</tt> operations for parsing contents of a file.
     * @param string    The update request as a string.
     * @param baseURI   The base URI for resolving relative URIs.
     */
    public static UpdateRequest create(String string, String baseURI,Context ctx) {
        return create(string, baseURI, defaultUpdateSyntax,ctx);
    }
    public static UpdateRequest create(String string, String baseURI) {
        return create(string, baseURI, defaultUpdateSyntax,null);
    }

    /**  Create an UpdateRequest by parsing from a string.
     * See also <tt>read</tt> operations for parsing contents of a file.
     * @param string    The update request as a string.
     * @param baseURI   The base URI for resolving relative URIs.
     * @param syntax    The update language syntax
     */
    public static UpdateRequest create(String string, String baseURI, Syntax syntax,Context ctx) {
        UpdateRequest request = new UpdateRequest(ctx);
        make(request, string, baseURI, syntax);
        return request;
    }
    public static UpdateRequest create(String string, String baseURI, Syntax syntax) {
        UpdateRequest request = new UpdateRequest(null);
        make(request, string, baseURI, syntax);
        return request;
    }

    // Worker.
    /** Append update operations to a request */
    private static void make(UpdateRequest request, String input, String baseURI, Syntax syntax) {
        UpdateParser parser = setupParser(request, baseURI, syntax);
        parser.parse(new UpdateRequestSink(request), request, input);
    }

    /* Parse operations and add to an UpdateRequest */
    public static void parse(UpdateRequest request, String updateString) {
        make(request, updateString, null, defaultUpdateSyntax);
    }

    /* Parse operations and add to an UpdateRequest */
    public static void parse(UpdateRequest request, String updateString, Syntax syntax) {
        make(request, updateString, null, syntax);
    }

    /* Parse operations and add to an UpdateRequest */
    public static void parse(UpdateRequest request, String updateString, String baseURI) {
        make(request, updateString, baseURI, defaultUpdateSyntax);
    }

    /* Parse operations and add to an UpdateRequest */
    public static void parse(UpdateRequest request, String updateString, String baseURI, Syntax syntax) {
        make(request, updateString, baseURI, syntax);
    }

    /** Append update operations to a request */
    protected static UpdateParser setupParser(Prologue prologue, String baseURI, Syntax syntax) {
        UpdateParser parser = UpdateParser.createParser(syntax);

        if ( parser == null )
            throw new UnsupportedOperationException("Unrecognized syntax for parsing update: " + syntax);

        if ( prologue.getBase() == null ) {
            IRIx base;
            // Sort out the baseURI - if that fails, dump in a dummy one and
            // continue.
            try {
                base = (baseURI != null) ? IRIs.resolveIRI(baseURI) : IRIs.getSystemBase();
            } catch (Exception ex) {
                base = IRIx.create("http://localhost/update/defaultBase#");
            }
            prologue.setBase(base);
        }
        return parser;
    }

    /** Create an UpdateRequest by reading it from a file */
    public static UpdateRequest read(UsingList usingList, String fileName,Context ctx) {
        return read(usingList, fileName, null, defaultUpdateSyntax,ctx);
    }

    /** Create an UpdateRequest by reading it from a file */
    public static UpdateRequest read(String fileName,Context ctx) {
        return read(fileName, fileName, defaultUpdateSyntax,ctx);
    }

    /** Create an UpdateRequest by reading it from a file */
    public static UpdateRequest read(String fileName, Syntax syntax,Context ctx) {
        return read(fileName, fileName, syntax,ctx);
    }

    /** Create an UpdateRequest by reading it from a file */
    public static UpdateRequest read(UsingList usingList, String fileName, Syntax syntax,Context ctx) {
        return read(usingList, fileName, fileName, syntax,ctx);
    }

    /** Create an UpdateRequest by reading it from a file */
    public static UpdateRequest read(String fileName, String baseURI, Syntax syntax,Context ctx) {
        return read(null, fileName, baseURI, syntax,ctx);
    }

    /** Create an UpdateRequest by reading it from a file */
    public static UpdateRequest read(UsingList usingList, String fileName, String baseURI, Syntax syntax,Context ctx) {
        InputStream in = null;
        try {
            if ( fileName.equals("-") )
                in = System.in;
            else {
                in = IO.openFile(fileName);
                if ( in == null )
                    throw new UpdateException("File could not be opened: " + fileName);
            }
            return read(usingList, in, baseURI, syntax,ctx);
        }
        finally {
            if ( in != null && !fileName.equals("-") )
                IO.close(in);
        }
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param input The source of the update request (must be UTF-8).
     */
    public static UpdateRequest read(InputStream input,Context ctx) {
        return read(input, defaultUpdateSyntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param usingList The list of externally defined USING statements
     * @param input The source of the update request (must be UTF-8).
     */
    public static UpdateRequest read(UsingList usingList, InputStream input,Context ctx) {
        return read(usingList, input, defaultUpdateSyntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param input The source of the update request (must be UTF-8).
     * @param syntax The update language syntax
     */
    public static UpdateRequest read(InputStream input, Syntax syntax,Context ctx) {
        return read(input, null, syntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param usingList The list of externally defined USING statements
     * @param input The source of the update request (must be UTF-8).
     * @param syntax The update language syntax
     */
    public static UpdateRequest read(UsingList usingList, InputStream input, Syntax syntax,Context ctx) {
        return read(usingList, input, null, syntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param input The source of the update request (must be UTF-8).
     * @param baseURI The base URI for resolving relative URIs.
     */
    public static UpdateRequest read(InputStream input, String baseURI,Context ctx) {
        return read(input, baseURI, defaultUpdateSyntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param usingList The list of externally defined USING statements
     * @param input The source of the update request (must be UTF-8).
     * @param baseURI The base URI for resolving relative URIs.
     */
    public static UpdateRequest read(UsingList usingList, InputStream input, String baseURI,Context ctx) {
        return read(usingList, input, baseURI, defaultUpdateSyntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param input The source of the update request (must be UTF-8).
     * @param baseURI The base URI for resolving relative URIs.
     * @param syntax The update language syntax
     */
    public static UpdateRequest read(InputStream input, String baseURI, Syntax syntax,Context ctx) {
        return read(null, input, baseURI, syntax,ctx);
    }

    /**
     * Create an UpdateRequest by parsing from an InputStream. See also <tt>read</tt>
     * operations for parsing contents of a file.
     *
     * @param usingList The list of externally defined USING statements
     * @param input The source of the update request (must be UTF-8).
     * @param baseURI The base URI for resolving relative URIs.
     * @param syntax The update language syntax
     */
    public static UpdateRequest read(UsingList usingList, InputStream input, String baseURI, Syntax syntax,Context ctx) {
        UpdateRequest request = new UpdateRequest(ctx);
        make(request, usingList, input, baseURI, syntax);
        return request;
    }

    /** Append update operations to a request */
    private static void make(UpdateRequest request, UsingList usingList, InputStream input, String baseURI, Syntax syntax) {
        UpdateParser parser = setupParser(request, baseURI, syntax);
        UpdateSink sink = new UsingUpdateSink(new UpdateRequestSink(request), usingList);
        try {
            parser.parse(sink, request, input);
        }
        finally {
            sink.close();
        }
    }
}
