/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclexecute;

import java.util.ArrayList;
import java.util.List;
//import org.hpccsystems.ecldirect.Output;
import org.hpccsystems.ecldirect.EclDirect;
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
import org.hpccsystems.ecldirect.Column;
import java.io.*;
import org.hpccsystems.ecldirect.ECLSoap;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.core.*;
import org.pentaho.di.core.gui.SpoonFactory;

import org.pentaho.di.plugins.perspectives.eclresults.*;

import org.hpccsystems.eclguifeatures.*;
import org.pentaho.di.job.JobMeta;

/**
 *
 * @author ChalaAX
 */
public class ECLExecute extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String attributeName = "";
    private String fileName = "";
    private String serverAddress = "";
    private String serverPort = "";
    private String debugLevel = "";
    
    public static boolean isReady = false;
    boolean isValid = true;


    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }
    
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
    public String getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public static boolean isIsReady() {
        return isReady;
    }

    public static void setIsReady(boolean isReady) {
        ECLExecute.isReady = isReady;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }    

    public String getDebugLevel() {
		return debugLevel;
	}

	public void setDebugLevel(String debugLevel) {
		this.debugLevel = debugLevel;
	}

	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
		String error = "";
        
      //Result result = null;
        
        Result result = prevResult;
        if(result.isStopped()){
            logBasic("{Output Job is Stopped}");

           if(!ECLExecute.isReady){
             //  result.setResult(false);
           } 
        }else{
        
	        ErrorNotices en = new ErrorNotices();
	        
	        //decide if we need to do a pre compile check before we send it off to the server
	        boolean validate = false;
	        if(!this.debugLevel.equalsIgnoreCase("None")){
	        	validate = true;
	        }
	        
	        //generates popup -- removed in lue of the error level
	        //en.openValidateCodeDialog();
	        //boolean validate = en.isValidateCode();
	
	        JobMeta jobMeta = super.parentJob.getJobMeta();
	                
	        String serverHost = "";
	        String serverPort = "";
	        String cluster = "";
	        String jobName = "";
	        String eclccInstallDir = "";
	        String mlPath = "";
	        String includeML = "";
	        
	        AutoPopulate ap = new AutoPopulate();
	        try{
	        //Object[] jec = this.jobMeta.getJobCopies().toArray();
	
	            serverHost = ap.getGlobalVariable(jobMeta.getJobCopies(),"server_ip");
	            serverPort = ap.getGlobalVariable(jobMeta.getJobCopies(),"server_port");
	
	            cluster = ap.getGlobalVariable(jobMeta.getJobCopies(),"cluster");
	            jobName = ap.getGlobalVariable(jobMeta.getJobCopies(),"jobName");
	
	            eclccInstallDir = ap.getGlobalVariable(jobMeta.getJobCopies(),"eclccInstallDir");
	            mlPath = ap.getGlobalVariable(jobMeta.getJobCopies(),"mlPath");
	            includeML = ap.getGlobalVariable(jobMeta.getJobCopies(),"includeML");
	            
	            //System.out.println("@@@@@@@@@@@@@@@@@@@" + includeML + "@@@@@@");
	
	        }catch (Exception e){
	            System.out.println("Error Parsing existing Global Variables ");
	            System.out.println(e.toString());
	            e.printStackTrace();
	
	        }
	        
	        //System.out.println("Output -- Finished setting up Global Variables");
	        this.setServerAddress(serverHost);
	        this.setServerPort(serverPort);
        
        
        
            ECLExecute.isReady=true;
            logBasic("not waiting: " + ECLExecute.isReady);
            result.setResult(true);


            List list = result.getRows();
            String eclCode = "";
            if (list == null) {
                list = new ArrayList();
            } else {

                for (int i = 0; i < list.size(); i++) {
                    RowMetaAndData rowData = (RowMetaAndData) list.get(i);
                    String code = rowData.getString("ecl", null);
                    if (code != null) {
                        eclCode += code;
                    }
                }
                logBasic("{Execute Job} Execute Code =" + eclCode);
            }
          
            
            
            EclDirect eclDirect = new EclDirect(this.serverAddress, cluster, this.serverPort);
            eclDirect.setEclccInstallDir(eclccInstallDir);
            eclDirect.setIncludeML(includeML);
            eclDirect.setJobName(jobName);
            eclDirect.setMlPath(mlPath);
            eclDirect.setOutputName(this.getName());
            ArrayList dsList = null;
            String outStr = "";
            //System.out.println("Output -- Finished setting up ECLDirect");
            try{
                String includes = "";
                includes += "IMPORT Std;\n";
                if(includeML.equals("true")){
                    includes += "IMPORT * FROM ML;\r\n\r\n";
                    includes += "IMPORT * FROM ML.Cluster;\r\n\r\n";
                    includes += "IMPORT * FROM ML.Types;\r\n\r\n";
                }
                System.out.println("Execute -- Finished Imports");
                eclCode = includes + eclCode;
                
                
                
                
                
                if(validate){
                	System.out.println("Execute -- Validate");
                   // System.out.println("Output -- Start Validate");
                    ECLSoap es = new ECLSoap();
                    es.setEclccInstallDir(eclccInstallDir);
                    es.setCluster(cluster);
                    System.out.println("Execute -- Validate 10");
                    es.setHostname(serverAddress);
                    es.setJobName(jobName);
                    es.setOutputName(this.getName());
                    System.out.println("Execute -- Validate 20");
                    if(includeML.equals("true")){
                        es.setIncludeML(true);
                        System.out.println("includML");
                    }else{
                        es.setIncludeML(false);
                        System.out.println("Dont includML");
                    }
                    es.setMlPath(mlPath);
                    es.setPort(Integer.parseInt(serverPort));
                    System.out.println("Execute -- Validate 30");
                    error = (es.syntaxCheck(eclCode)).trim();
                    System.out.println("Execute -- Validate 31");
                    boolean isError = false;
                    boolean isWarning = false;
                    
                   
                    
                    if(es.getErrorCount() > 0 &&  this.debugLevel.equalsIgnoreCase("Stop on Errors")){
                    	isError = true;
                    	System.out.println("Execute -- Validate isError");
                    }
                    if(es.getWarningCount() > 0 && this.debugLevel.equalsIgnoreCase("Stop on Errors or Warnings")){
                    	isWarning = true;
                    	System.out.println("Execute -- Validate isWarning");
                    }
                    
                    if((isError || isWarning) && !error.equals("")){
                    	
                    	
                    	//need to detect level here
                    	//int errorLevel = detectErrorLevel(error);
                        logError("Syntax Check Failed:\n\r"+error);
                        this.logRowlevel("Syntax Check Failed:\n\r"+error);
                        //en.openDialog("Syntax Check Failed:", error,eclCode);
                        isValid = false;
                        result.setResult(false);
                        result.setLogText(error);
                        
                        /*
                        RowMetaAndData data = new RowMetaAndData();
                        data.addValue("eclErrorCode", Value.VALUE_TYPE_STRING,eclCode+"\r\n");
                        list.add(data);
                        
                        data = new RowMetaAndData();
                        data.addValue("eclError", Value.VALUE_TYPE_STRING, error + "\r\n");
                        list.add(data);
                        */
                        //throw new KettleException("ECL failed Validation, please correct and re-run." + error);
                    }else{
                    	System.out.println("Execute -- No Validate");
                    	System.out.println("!!!!!!!!!!1 USE execute_noResults");
                        //isValid = eclDirect.execute_noResults(eclCode);
                    	
                    	
                        
                        isValid = es.executeECL(eclCode);
                        eclDirect.setWuid(es.getWuid());
                        
                        //if not isValid add error
                        if(!isValid){
                        	error += "\r\nServer Failed to compile code please refer to ECLWatch and verify your settings\r\n";
                        	logError("Server Failed to compile code please refer to ECLWatch and verify your settings");
                        }
                        
                    }
                   // System.out.println("Output -- Finished Validating");
                }else{
                	System.out.println("!!!!!!!!!!2 USE execute_noResults");
                    //isValid = eclDirect.execute_noResults(eclCode);
                	ECLSoap es = new ECLSoap();
                    es.setEclccInstallDir(eclccInstallDir);
                    es.setCluster(cluster);
                    es.setHostname(serverAddress);
                    es.setJobName(jobName);
                    es.setOutputName(this.getName());
                    System.out.println("@@@@@@@@@@@@@@@@@@@2" + includeML + "@@@@@@");
                    if(includeML.equals("true")){
                        es.setIncludeML(true);
                        System.out.println("includML");
                    }else{
                        es.setIncludeML(false);
                        System.out.println("Dont includML");
                    }
                    es.setMlPath(mlPath);
                    es.setPort(Integer.parseInt(serverPort));
                    isValid = es.executeECL(eclCode);
                    eclDirect.setWuid(es.getWuid());
                    
                    if(!isValid){
                    	error += "\r\nFailed to execute code on the cluster, please verify your settings\r\n";
                    	logError("Failed to execute code on the cluster, please verify your settings");
                    }
                }
                //logBasic("Start Log Results");
               
                    if(isValid){// && dsList != null){
                        ArrayList al = eclDirect.resultList();
                        
                        int alSize = al.size();
                        // System.out.println(al.toString());
                         //rows
                        for(int i = 0; i < alSize ; i++){
                            //System.out.println("-");
                            ArrayList al2 = (ArrayList)al.get(i);
                            int al2Size = al2.size();
                            //columns
                            int counter = 0;
                            for(int j = 0; j < al2Size ; j++){
                               // System.out.println("--");

                                ArrayList al3 = (ArrayList)al2.get(j);
                                int al3Size = al3.size();
                                
                                for(int r = 0; r < al3Size ; r++){

                                    if(((Column)al3.get(r)).getName().equals("Name")){
                                        //System.out.println("---");
                                        String resName = ((Column)al3.get(r)).getValue();
                                        //System.out.println( v2);
                                        ECLSoap es = new ECLSoap();
                                        es.setEclccInstallDir(eclccInstallDir);
                                        es.setCluster(cluster);
                                        es.setHostname(serverAddress);
                                        es.setJobName(jobName);
                                        es.setOutputName(this.getName());
                                        //System.out.println("@@@@@@@@@@@@@@@@@@@3" + includeML + "@@@@@@");
                                        if(includeML.equals("true")){
                                            es.setIncludeML(true);
                                            //System.out.println("includML");
                                        }else{
                                            es.setIncludeML(false);
                                            //System.out.println("Dont includML");
                                        }
                                        
                                        InputStream is = es.ResultsSoapCall(eclDirect.getWuid(), resName);
                                        ArrayList results = es.parseResults(is);
                                        //System.out.println(this.fileName + "\\" + resName + ".csv");
                                        resName = resName.replace(" ", "_");
                                        createOutputFile(results,this.fileName + "\\" + resName + ".csv",counter);
                                        counter++;
                                    }
                                }
                                
                            }
                            //set counter to memory
                                
                                //System.out.println(counter);
                                System.setProperty("fileCount", counter+"" );
                        }
                      //  this.createOutputFile(dsList,fileName);
                      //  result.setRows(list);
                    }else{
                    	logError("Tasked Failed");
                    	result.setResult(false);
                    	//error += "\r\nFailed to compile code please verify your settings\r\n";
                    
                    }
                    
                    
                    
             }catch (Exception e){
                logError("Failed to execute code on Cluster." + e);
                result.setResult(false);
                error += "Exception occured please verify all settings.";
                 e.printStackTrace();
            }
            
            result.clear();
            RowMetaAndData data = new RowMetaAndData();
            data.addValue("eclErrorCode", Value.VALUE_TYPE_STRING,eclCode+"\r\n");
            //list.add(data);
            
            //data = new RowMetaAndData();
            data.addValue("eclError", Value.VALUE_TYPE_STRING, error + "\r\n");
            list.add(data);
            result.setRows(list);
            //write the executed ecl code to file
            try {              
                System.getProperties().setProperty("eclCodeFile",fileName);
                
                BufferedWriter out = new BufferedWriter(new FileWriter(this.fileName + "\\eclcode.txt"));
                out.write(eclCode);
                out.close();
            } catch (IOException e) {
                 e.printStackTrace();
            }   
             
        
       }
        
        
       return result;
    }
	
	/*
	public int detectErrorLevel(String error){
		int errorLevel = 0;
		
		//does it contain error
		
		//does it contain warning
		
		return errorLevel;
	}*/
    
    public void createOutputFile(ArrayList dsList,String fileName, int count){
         String outStr = "";
         String header = "";
         if(dsList != null){
         String newline = System.getProperty("line.separator");
         
                        for (int iList = 0; iList < dsList.size(); iList++) {
                            //logBasic("----------Outer-------------");
                            ArrayList rowList = (ArrayList) dsList.get(iList);

                            for (int jRow = 0; jRow < rowList.size(); jRow++) {
                                //logBasic("----------Row-------------");
                                ArrayList columnList = (ArrayList) rowList.get(jRow);

                                for (int lCol = 0; lCol < columnList.size(); lCol++) {
                                 //   logBasic("----------Column-------------");
                                    Column column = (Column) columnList.get(lCol);
                                    logBasic(column.getName() + "=" + column.getValue() + "|");
                                    outStr += column.getValue();
                                    if(lCol< (columnList.size()-1)){
                                        outStr += ",";
                                    }
                                    if(jRow == 0){
                                        header += column.getName();
                                        if(lCol< (columnList.size()-1)){
                                            header += ",";
                                        }else{
                                            header += newline;
                                        }
                                    }
                                }
                                logBasic("newline");
                                outStr += newline;
                            }
                        }
             try {
                
                BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
                System.getProperties().getProperty("fileName");
                System.setProperty("fileName"+count, fileName);
                
                out.write(header+outStr);
                out.close();
           
            } catch (IOException e) {
               logError("Failed to write file: " + fileName);
               //error += "Failed to write ecl code file";
               //result.setResult(false);
                e.printStackTrace();
            }  
         }
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
             //System.out.println(" ------------ loadXML ------------- ");
            super.loadXML(node, list, list1);

            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "attribute_name")) != null)
            //    setAttributeName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "attribute_name")));
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "server_address")) != null)
                //setServerAddress(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "server_address")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_name")) != null)
                setFileName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "debugLevel")) != null)
                setDebugLevel(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "debugLevel")));

        } catch (Exception e) {
            e.printStackTrace();
            throw new KettleXMLException("ECL Output Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        // System.out.println(" ------------ getXML ------------- ");
        retval += super.getXML();

        //retval += "		<attribute_name>" + attributeName + "</attribute_name>" + Const.CR;
       // retval += "		<server_address>" + serverAddress + "</server_address>" + Const.CR;
        
      
        retval += "		<file_name><![CDATA[" + fileName + "]]></file_name>" + Const.CR;
        retval += "		<debugLevel><![CDATA[" + this.debugLevel + "]]></debugLevel>" + Const.CR;
        //System.out.println(" end getXML ");
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        //System.out.println(" ------------ loadRep " + id_jobentry + "------------- ");
        try {
          
            //if(rep.getStepAttributeString(id_jobentry, "attributeName") != null)
            //    attributeName = rep.getStepAttributeString(id_jobentry, "attributeName"); //$NON-NLS-1$
            //if(rep.getStepAttributeString(id_jobentry, "serverAddress") != null)
            //    serverAddress = rep.getStepAttributeString(id_jobentry, "serverAddress"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "fileName") != null)
                fileName = rep.getStepAttributeString(id_jobentry, "fileName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "debugLevel") != null)
                debugLevel = rep.getStepAttributeString(id_jobentry, "debugLevel"); //$NON-NLS-1$
        
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
            //rep.saveStepAttribute(id_job, getObjectId(), "attributeName", attributeName); //$NON-NLS-1$
            //rep.saveStepAttribute(id_job, getObjectId(), "serverAddress", serverAddress); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "fileName", fileName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "debugLevel", this.debugLevel); //$NON-NLS-1$
           
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    public boolean evaluates() {
    	return isValid;
        //return true;
    }

    public boolean isUnconditional() {
        return false;
    }
    
    
    
    
    
   
}
