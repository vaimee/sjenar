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

package org.apache.jena.fuseki.build ;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.fuseki.server.FusekiVocab.*;
import static org.apache.jena.riot.RDFLanguages.filenameToLang;
import static org.apache.jena.riot.RDFParserRegistry.isRegistered;

import java.io.File ;
import java.io.IOException ;
import java.lang.reflect.Method ;
import java.nio.file.DirectoryStream ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;
import java.util.ArrayList ;
import java.util.Collection;
import java.util.Collections ;
import java.util.List ;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.JA ;
import org.apache.jena.atlas.lib.IRILib ;
import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.fuseki.Fuseki ;
import org.apache.jena.fuseki.FusekiConfigException ;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.server.DataServiceStatus;
import org.apache.jena.fuseki.server.FusekiVocab;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.QuerySolution ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.query.ResultSet ;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.assembler.AssemblerUtils ;
import org.apache.jena.sparql.util.FmtUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger ;

public class FusekiConfig {
    static { Fuseki.init() ; }
    
    private static Logger log = Fuseki.configLog ;
    
    /** Process the server section, if any, of a configuration file.
     * This includes setting global context and ja:loadClass.
     * It does not include services - see {@link #servicesAndDatasets}
     */ 
    public static void processServerConfig(Model model) {
        // Find one server.
        List<Resource> servers = GraphUtils.listResourcesByType(model, FusekiVocab.tServer) ;
        if ( servers.size() == 0 )
            return ; 
        if ( servers.size() > 1 )
            throw new FusekiConfigException(servers.size()
                                            + " servers found (must be exactly one in a configuration file)") ;
        // ---- Server
        Resource server = servers.get(0) ;
        processServer(server) ;
    }

    private static void processServer(Resource server) {
        // Global, currently.
        AssemblerUtils.setContext(server, Fuseki.getContext()) ;

        StmtIterator sIter = server.listProperties(JA.loadClass) ;
        for ( ; sIter.hasNext() ; ) {
            Statement s = sIter.nextStatement() ;
            RDFNode rn = s.getObject() ;
            String className = null ;
            if ( rn instanceof Resource ) {
                String uri = ((Resource)rn).getURI() ;
                if ( uri == null ) {
                    log.warn("Blank node for class to load") ;
                    continue ;
                }
                String javaScheme = "java:" ;
                if ( !uri.startsWith(javaScheme) ) {
                    log.warn("Class to load is not 'java:': " + uri) ;
                    continue ;
                }
                className = uri.substring(javaScheme.length()) ;
            }
            if ( rn instanceof Literal )
                className = ((Literal)rn).getLexicalForm() ;
            /* Loader. */loadAndInit(className) ;
        }
    }
    
    /** Find and process datasets and services in a configuration file.
     * This can be a Fuseki server configuration file or a services-only configuration file.
     * It looks {@code fuseki:services ( .... )} then, if not found, all {@code rtdf:type fuseki:services}. 
     */
    public static List<DataAccessPoint> servicesAndDatasets(Model model) {
        // Old style configuration file : server to services.
        DatasetDescriptionRegistry dsDescMap = new DatasetDescriptionRegistry();
        // ---- Services
        ResultSet rs = FusekiBuildLib.query("SELECT * { ?s fu:services [ list:member ?service ] }", model) ;
        List<DataAccessPoint> accessPoints = new ArrayList<>() ;

        if ( ! rs.hasNext() )
            // No "fu:services ( .... )" so try looking for services directly.
            // This means Fuseki2, service configuration files (no server section) work for --conf. 
            rs = FusekiBuildLib.query("SELECT ?service { ?service a fu:Service }", model) ;

        for ( ; rs.hasNext() ; ) {
            QuerySolution soln = rs.next() ;
            Resource svc = soln.getResource("service") ;
            DataAccessPoint acc = buildDataAccessPoint(svc, dsDescMap) ;
            accessPoints.add(acc) ;
        }
        return accessPoints ;
    }
    
    private static void loadAndInit(String className) {
        try {
            Class<? > classObj = Class.forName(className) ;
            log.info("Loaded " + className) ;
            Method initMethod = classObj.getMethod("init") ;
            initMethod.invoke(null) ;
        }
        catch (ClassNotFoundException ex) {
            log.warn("Class not found: " + className) ;
        }
        catch (Exception e) {
            throw new FusekiConfigException(e) ;
        }
    }
    
    private static Model readAssemblerFile(String filename) {
        return AssemblerUtils.readAssemblerFile(filename) ;
    }
    
    // ---- Directory of assemblers
    
    /** Read service descriptions in the given directory */ 
    public static List<DataAccessPoint> readConfigurationDirectory(String dir) {
        Path pDir = Paths.get(dir).normalize() ;
        File dirFile = pDir.toFile() ;
        if ( ! dirFile.exists() ) {
            log.warn("Not found: directory for assembler files for services: '"+dir+"'") ;
            return Collections.emptyList() ;
        }
        if ( ! dirFile.isDirectory() ) {
            log.warn("Not a directory: '"+dir+"'") ;
            return Collections.emptyList() ;
        }
        // Files that are not hidden.
        DirectoryStream.Filter<Path> filter = (entry)-> {
            File f = entry.toFile() ;
            final Lang lang = filenameToLang(f.getName());
            return ! f.isHidden() && f.isFile() && lang != null && isRegistered(lang) ;
        } ;

        List<DataAccessPoint> dataServiceRef = new ArrayList<>() ;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pDir, filter)) {
            for ( Path p : stream ) {
                DatasetDescriptionRegistry dsDescMap = new DatasetDescriptionRegistry() ;
                String fn = IRILib.filenameToIRI(p.toString()) ;
                log.info("Load configuration: "+fn);
                Model m = readAssemblerFile(fn) ;
                readConfiguration(m, dsDescMap, dataServiceRef) ; 
            }
        } catch (IOException ex) {
            log.warn("IOException:"+ex.getMessage(), ex);
        }
        return dataServiceRef ;
    }

    /** Read a configuration in a model.
     * Allow dataset descriptions to be carried over from another place.
     * Add to a list. 
     */
    private static void readConfiguration(Model m, DatasetDescriptionRegistry dsDescMap, List<DataAccessPoint> dataServiceRef) {
        List<Resource> services = GraphUtils.listResourcesByType(m, FusekiVocab.fusekiService) ; 

        if ( services.size() == 0 ) {
            log.error("No services found") ;
            throw new FusekiConfigException() ;
        }

        for ( Resource service : services ) {
            DataAccessPoint acc = buildDataAccessPoint(service, dsDescMap) ; 
            dataServiceRef.add(acc) ;
        }
    }
    
    /** Build a DataAccessPoint, including DataService, from the description at Resource svc */ 
    public static DataAccessPoint buildDataAccessPoint(Resource svc, DatasetDescriptionRegistry dsDescMap) {
        RDFNode n = FusekiBuildLib.getOne(svc, "fu:name") ;
        if ( ! n.isLiteral() )
            throw new FusekiConfigException("Not a literal for access point name: "+FmtUtils.stringForRDFNode(n));
        Literal object = n.asLiteral() ;
        
        if ( object.getDatatype() != null && ! object.getDatatype().equals(XSDDatatype.XSDstring) )
            Fuseki.configLog.error(format("Service name '%s' is not a string", FmtUtils.stringForRDFNode(object)));

        String name = object.getLexicalForm() ;
        name = DataAccessPoint.canonical(name) ;
        DataService dataService = buildDataService(svc, dsDescMap) ;
        Collection<String> allowedUsers = getAllowedUsers(svc);
        dataService.setAllowedUsers(allowedUsers);
        DataAccessPoint dataAccess = new DataAccessPoint(name, dataService) ;
        return dataAccess ;
    }
    
    /** Get the allowed users on some resources.
     *  
     * @param resource
     * @return Collection<String>
     */
    public static Collection<String> getAllowedUsers(Resource resource) {
        Collection<RDFNode> allowedUsers = FusekiBuildLib.getAll(resource, "fu:"+pAllowedUsers.getLocalName());
        Collection<String> userNames = null;
        if ( allowedUsers != null ) {
            // Check all values are simple strings  
            List<String> bad = allowedUsers.stream()
                .map(RDFNode::asNode)
                .filter(rn -> ! Util.isSimpleString(rn))
                .map(rn->rn.toString())
                .collect(toList());
            if ( ! bad.isEmpty() ) {
                //Fuseki.configLog.error(format("User names must be a simple string: bad = %s", bad));
                throw new FusekiConfigException(format("User names should be a simple string: bad = %s", bad));
            }
            // RDFNodes/literals to strings.
            userNames = allowedUsers.stream()
                .map(RDFNode::asNode)
                .map(Node::getLiteralLexicalForm)
                .collect(toList());
        }
        return userNames;
    }

    /** Build a DatasetRef starting at Resource svc, having the services as described by the descriptions. */
    private static DataService buildDataService(Resource svc, DatasetDescriptionRegistry dsDescMap) {
        Resource datasetDesc = ((Resource)FusekiBuildLib.getOne(svc, "fu:dataset")) ;
        Dataset ds = getDataset(datasetDesc, dsDescMap);
 
        // In case the assembler included ja:contents
        DataService dataService = new DataService(ds.asDatasetGraph()) ;

        FusekiBuilder.addServiceEP(dataService, Operation.Query,  svc,    pServiceQueryEP) ;
        FusekiBuilder.addServiceEP(dataService, Operation.Update, svc,    pServiceUpdateEP) ;
        FusekiBuilder.addServiceEP(dataService, Operation.Upload, svc,    pServiceUploadEP);
        FusekiBuilder.addServiceEP(dataService, Operation.GSP_R,  svc,    pServiceReadGraphStoreEP) ;
        FusekiBuilder.addServiceEP(dataService, Operation.GSP_RW, svc,    pServiceReadWriteGraphStoreEP) ;

        FusekiBuilder.addServiceEP(dataService, Operation.Quads_R, svc,   pServiceReadQuadsEP) ;
        FusekiBuilder.addServiceEP(dataService, Operation.Quads_RW, svc,  pServiceReadWriteQuadsEP) ;
        
        // Quads - actions directly on the dataset URL are different.
        // In the config file they are also implicit when using GSP.
        
        if ( ! dataService.getEndpoints(Operation.GSP_RW).isEmpty() || ! dataService.getEndpoints(Operation.Quads_RW).isEmpty() ) {
            // ReadWrite available.
            // Dispatch needs introspecting on the HTTP request.
            dataService.addEndpoint(Operation.DatasetRequest_RW, "") ;
        } else if ( ! dataService.getEndpoints(Operation.GSP_R).isEmpty() || ! dataService.getEndpoints(Operation.Quads_R).isEmpty() ) {
            // Read-only available.
            // Dispatch needs introspecting on the HTTP request.
            dataService.addEndpoint(Operation.DatasetRequest_R, "") ;
        }
        
        // XXX 
        // This needs sorting out -- here, it is only on the whole server, not per dataset or even per service.
//        // Extract timeout overriding configuration if present.
//        if ( svc.hasProperty(FusekiVocab.pAllowTimeoutOverride) ) {
//            sDesc.allowTimeoutOverride = svc.getProperty(FusekiVocab.pAllowTimeoutOverride).getObject().asLiteral().getBoolean() ;
//            if ( svc.hasProperty(FusekiVocab.pMaximumTimeoutOverride) ) {
//                sDesc.maximumTimeoutOverride = (int)(svc.getProperty(FusekiVocab.pMaximumTimeoutOverride).getObject().asLiteral().getFloat() * 1000) ;
//            }
//        }
        return dataService ;
    }
    
    public static Dataset getDataset(Resource datasetDesc, DatasetDescriptionRegistry dsDescMap) {
        // check if this one already built
        Dataset ds = dsDescMap.get(datasetDesc);
        if (ds == null) {
            // Check if the description is in the model.
            if ( !datasetDesc.hasProperty(RDF.type) )
                throw new FusekiConfigException("No rdf:type for dataset " + FusekiBuildLib.nodeLabel(datasetDesc)) ;
            ds = (Dataset)Assembler.general.open(datasetDesc) ;
        }
        // Some kind of check that it is "the same" dataset.  
        // It can be different if two descriptions in different files have the same URI.
        dsDescMap.register(datasetDesc, ds);
        return ds;
    }


    // ---- System database
    /** Read the system database */
    public static List<DataAccessPoint> readSystemDatabase(Dataset ds) {
        DatasetDescriptionRegistry dsDescMap = new DatasetDescriptionRegistry() ;
        String qs = StrUtils.strjoinNL
            (FusekiConst.PREFIXES ,
             "SELECT * {" ,
             "  GRAPH ?g {",
             "     ?s fu:name ?name ;" ,
             "        fu:status ?status ." ,
             "  }",
             "}"
             ) ;
        
        List<DataAccessPoint> refs = new ArrayList<>() ;
        
        ds.begin(ReadWrite.WRITE) ;
        try {
            ResultSet rs = FusekiBuildLib.query(qs, ds) ;

    //        ResultSetFormatter.out(rs); 
    //        ((ResultSetRewindable)rs).reset();

            for ( ; rs.hasNext() ; ) {
                QuerySolution row = rs.next() ;
                Resource s = row.getResource("s") ;
                Resource g = row.getResource("g") ;
                Resource rStatus = row.getResource("status") ;
                //String name = row.getLiteral("name").getLexicalForm() ;
                DataServiceStatus status = DataServiceStatus.status(rStatus) ;

                Model m = ds.getNamedModel(g.getURI()) ;
                // Rebase the resource of the service description to the containing graph.
                Resource svc = m.wrapAsResource(s.asNode()) ;
                DataAccessPoint ref = buildDataAccessPoint(svc, dsDescMap) ;
                refs.add(ref) ;
            }
            ds.commit(); 
            return refs ;
        } finally { ds.end() ; }
    }
}
