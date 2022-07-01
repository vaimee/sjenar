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

package org.apache.jena.tdb.hadoop.assembler;

import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty ;
import static org.apache.jena.sparql.util.graph.GraphUtils.getAsStringValue ;
import static org.apache.jena.tdb.hadoop.assembler.VocabTDB.pDescription ;
import static org.apache.jena.tdb.hadoop.assembler.VocabTDB.pFile ;

import java.util.Locale ;

import org.apache.jena.assembler.Assembler ;
import org.apache.jena.assembler.Mode ;
import org.apache.jena.assembler.assemblers.AssemblerBase ;
import org.apache.jena.rdf.model.Resource ;
import org.apache.jena.tdb.hadoop.TDBException ;
import org.apache.jena.tdb.hadoop.base.file.FileSet ;
import org.apache.jena.tdb.hadoop.base.file.Location ;
import org.apache.jena.tdb.hadoop.base.record.RecordFactory ;
import org.apache.jena.tdb.hadoop.index.IndexFactory ;
import org.apache.jena.tdb.hadoop.index.IndexParams ;
import org.apache.jena.tdb.hadoop.index.RangeIndex ;
import org.apache.jena.tdb.hadoop.lib.ColumnMap ;
import org.apache.jena.tdb.hadoop.setup.StoreParams ;
import org.apache.jena.tdb.hadoop.store.tupletable.TupleIndex ;
import org.apache.jena.tdb.hadoop.store.tupletable.TupleIndexRecord ;
import org.apache.jena.tdb.hadoop.sys.Names ;
import org.apache.jena.tdb.hadoop.sys.SystemTDB ;

public class IndexAssembler extends AssemblerBase //implements Assembler
{
    /* 
     * [ :description "SPO" ; :file "SPO.idx" ]
     */
    
    private Location location = null ;
    private IndexAssembler()                   { this.location = null ; }
    private IndexAssembler(Location location)  { this.location = location ; }
    
    @Override
    public TupleIndex open(Assembler a, Resource root, Mode mode)
    {
        exactlyOneProperty(root, pDescription) ;
        String desc = getAsStringValue(root, pDescription).toUpperCase(Locale.ENGLISH) ;
        exactlyOneProperty(root, pFile) ;
        String filename = getAsStringValue(root, pFile) ;
        
        // Need to get location from the enclosing PGraphAssembler
        if ( location != null )
            filename = location.absolute(filename) ;
        
        String primary = null ;
        RecordFactory rf = null ;
        
        switch ( desc.length() )
        {
            case 3:
                primary = Names.primaryIndexTriples ;
                rf = SystemTDB.indexRecordTripleFactory ;
                break ;
            case 4:
                primary = Names.primaryIndexQuads;
                rf = SystemTDB.indexRecordQuadFactory ;
                break ;
            default:
                throw new TDBException("Bad length for index description: "+desc) ;
                
        }
        // Problems with spotting the index technology.
        FileSet fileset = null ; //FileSet.fromFilename(filename) ;
        IndexParams idxParams = StoreParams.getDftStoreParams() ;
        RangeIndex rIndex = IndexFactory.buildRangeIndex(fileset, rf, idxParams) ;
        return new TupleIndexRecord(desc.length(), new ColumnMap(primary, desc), desc, rf, rIndex) ;
    }
}
