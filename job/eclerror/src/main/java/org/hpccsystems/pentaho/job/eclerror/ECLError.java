/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclerror;

import java.util.ArrayList;
import java.util.List;
//import org.hpccsystems.javaecl.Output;
import org.hpccsystems.javaecl.EclDirect;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.javaecl.Column;
import java.io.*;
import org.hpccsystems.javaecl.ECLSoap;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.core.*;
import org.pentaho.di.core.gui.SpoonFactory;

import org.pentaho.di.plugins.perspectives.eclresults.*;

import org.hpccsystems.eclguifeatures.*;
import org.pentaho.di.job.JobMeta;
import org.hpccsystems.ecljobentrybase.*;
import org.hpccsystems.javaecl.EclDirect;

/**
 *
 * @author ChambersJ
 */
public class ECLError extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    


    
    public static boolean isReady = false;
    //boolean isValid = true;

    public static boolean isIsReady() {
        return isReady;
    }

    public static void setIsReady(boolean isReady) {
        ECLError.isReady = isReady;
    }

    

	@Override
	public Result execute(Result prevResult, int k) throws KettleException {
	
	
		Result result = modifyResults(prevResult);
		if(result.isStopped()){
		    logBasic("{Output Job is Stopped}");
		}else{
		    
			List list = result.getRows();
			
			String eclCode = "";
			String error = "";
			if (list == null) {
			    list = new ArrayList();
			} else {
				//System.out.println(result.getXML());
			    for (int i = 0; i < list.size(); i++) {
			        RowMetaAndData rowData = (RowMetaAndData) list.get(i);
			        String[] names = rowData.getRowMeta().getFieldNames();
			        boolean isError = false;
			        boolean isCode = false;
			        for(int j = 0; j < names.length; j++){
			        	if(names[j].equalsIgnoreCase("eclError")){
			        		isError = true;
			        	}
			        	if(names[j].equalsIgnoreCase("eclErrorCode")){
			        		isCode = true;
			        	}
			        }
			        if(isCode){
				        String code = rowData.getString("eclErrorCode", null);
					    if (code != null) {
					        eclCode += code;
					    }
			        }
			        if(isError){
					    String e = rowData.getString("eclError", null);
		                if (e != null) {
		                    error += e;
		                }
			        }
			    }
			    
			    
			}
			
			if (isGuiMode()) {
			    ErrorNotices en = new ErrorNotices();
			    //String error = result.getLogText();
			    error = "Please see eclwatch for more detailed information!\n\n\n" + error;
			    en.openDialog("Syntax Check Failed:", error,eclCode);
			}
		    
		}
		return result;
	}
    
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
             //System.out.println(" ------------ loadXML ------------- ");
            super.loadXML(node, list, list1);

        } catch (Exception e) {
            e.printStackTrace();
            throw new KettleXMLException("ECL Output Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        // System.out.println(" ------------ getXML ------------- ");
        retval += super.getXML();
        //System.out.println(" end getXML ");
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        //System.out.println(" ------------ loadRep " + id_jobentry + "------------- ");
        try {
          
        
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
            
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
       // System.out.println(" ------------ saveRep " + id_job + " ------------- ");
        try {
             
            ObjectId[] allIDs = rep.getPartitionSchemaIDs(true);
            for(int i = 0; i<allIDs.length; i++){
                logBasic("ObjectID["+i+"] = " + allIDs[i]);
              //  System.out.println("ObjectID["+i+"] = " + allIDs[i]);
            }

           
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    
    
    public static boolean isGuiMode() {
        boolean guiMode = false;
        try {
            guiMode = (org.pentaho.di.ui.spoon.Spoon.getInstance() != null);
        } catch (NoClassDefFoundError e) {
        }
        
        return guiMode;
    }
}
