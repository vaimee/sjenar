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
package org.apache.jena.acl;

/**
 *
 * @author Lorenzo
 */
public abstract class  DatasetACL {
    public static final String ADMIN_USER = "admin";
    public static final String ACL_HANDLER_NAME = "ACL";
    public static final String ACL_USER_NAME = "USER";
    
    public static final String ACL_GRAPH_NAME_ALL = "$$ALL$$";
    public static final String ACL_GRAPH_NAME_DEFAULT = "$$DEFAULT$$";
    
    public static final String DEF_GRAPH_NAME = "urn:x-arq:DefaultGraphNode";
    
    public static  enum aclId {
        aiDrop, 
        aiClear,
        aiCreate,
        aiInsertData,
        aiDeleteData,
        aiUpdate,
        aiQuery
    };
    //throwing accessors
    public void checkCreate(String graphName, String user) throws ACLException{
        checkGrap(aclId.aiCreate, graphName, user);
    }
    
    public void checkDrop(String graphName, String user) throws ACLException{
        checkGrap(aclId.aiDrop, graphName, user);
    }
    public void checkClear(String graphName, String user) throws ACLException {
        checkGrap(aclId.aiClear, graphName, user);
    }
    
    public void checkDeleteData(String graphName, String user) throws ACLException {
        checkGrap(aclId.aiDeleteData, graphName, user);
    }
    public void checkInsertData(String graphName, String user) throws ACLException{
        checkGrap(aclId.aiInsertData, graphName, user);
    }
    
    public void checkUpdate(String graphName, String user) throws ACLException{
        checkGrap(aclId.aiUpdate, graphName, user);
    }
 
    public void checkQuery(String graphName, String user) throws ACLException{
        checkGrap(aclId.aiQuery, graphName, user);
    }
    
    //not throwing accessors
    //real work is done here
    public void checkGrap(aclId id, String graphName, String user) throws ACLException {
        final boolean f = checkGrapBase(id, graphName, user);
        if (f == false) 
            throw new ACLException(graphName, user);
    }
    
    public abstract boolean checkGrapBase(aclId id, String graphName, String user);
}