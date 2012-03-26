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
    
    public static boolean isReady = false;


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

    
    
    

    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        
      //Result result = null;
        
        Result result = prevResult;
        if(result.isStopped()){
            logBasic("{Output Job is Stopped}");

           if(!ECLExecute.isReady){
             //  result.setResult(false);
           } 
        }else{
        
        ErrorNotices en = new ErrorNotices();
        
        
        //en.openDialog("Test Error, Click OK to continue");
        en.openValidateCodeDialog();
        boolean validate = en.isValidateCode();

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

        }catch (Exception e){
            System.out.println("Error Parsing existing Global Variables ");
            System.out.println(e.toString());
            e.printStackTrace();

        }
        
        System.out.println("Output -- Finished setting up Global Variables");
        this.setServerAddress(serverHost);
        this.setServerPort(serverPort);
        
        
        
            ECLExecute.isReady=true;
            logBasic("not waiting: " + ECLExecute.isReady);
            //Output op = new Output();
            //op.setDefinitionName(getAttributeName());

            //String opResults = op.ecl();

            //logBasic("{Output Job} Execute = " + opResults);

            logBasic("{Output Job} Previous =" + result.getLogText());

            result.setResult(true);

           // RowMetaAndData data = new RowMetaAndData();
            //data.addValue("ecl", Value.VALUE_TYPE_STRING, opResults);


            List list = result.getRows();
          //  list.add(data);
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
                logBasic("{Output Job} Output Code =" + eclCode);
            }
          
            
            
            EclDirect eclDirect = new EclDirect(this.serverAddress, cluster, this.serverPort);
            eclDirect.setEclccInstallDir(eclccInstallDir);
            eclDirect.setIncludeML(includeML);
            eclDirect.setJobName(jobName);
            eclDirect.setMlPath(mlPath);
            eclDirect.setOutputName(this.getName());
            ArrayList dsList = null;
            String outStr = "";
            System.out.println("Output -- Finished setting up ECLDirect");
            try{
                String includes = "";
                includes += "IMPORT Std;\n";
                if(includeML.equals("true")){
                    includes += "IMPORT * FROM ML;\r\n\r\n";
                    includes += "IMPORT * FROM ML.Cluster;\r\n\r\n";
                    includes += "IMPORT * FROM ML.Types;\r\n\r\n";
                }
                 System.out.println("Output -- Finished Imports");
                eclCode = includes + eclCode;
                String error = "";
                boolean isValid = true;
                if(validate){
                    System.out.println("Output -- Start Validate");
                    ECLSoap es = new ECLSoap();
                    es.setEclccInstallDir(eclccInstallDir);
                    es.setCluster(cluster);
                    es.setHostname(serverAddress);
                    es.setJobName(jobName);
                    es.setOutputName(this.getName());
                    
                    if(includeML.equals("false")){
                        es.setIncludeML(false);
                    }else{
                        es.setIncludeML(true);
                    }
                    es.setMlPath(mlPath);
                    es.setPort(Integer.parseInt(serverPort));
                     System.out.println("+++++");
                    error = (es.syntaxCheck(eclCode)).trim();
                    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    System.out.println("|"+error+"|");
                    if(!error.equals("")){
                        System.out.println("Throw Exception");
                        logError("Syntax Check Failed:\n\r"+error);
                        this.logRowlevel("Syntax Check Failed:\n\r"+error);
                        en.openDialog("Syntax Check Failed:", error);
                        isValid = false;
                        result.setResult(false);
                        throw new KettleException("ECL failed Validation, please correct and re-run." + error);
                    }else{
                        isValid = eclDirect.execute_noResults(eclCode);
                    }
                    System.out.println("Output -- Finished Validating");
                }else{
                    isValid = eclDirect.execute_noResults(eclCode);
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
                                        
                                        InputStream is = es.ResultsSoapCall(eclDirect.getWuid(), resName);
                                        ArrayList results = es.parseResults(is);
                                        System.out.println(this.fileName + "\\" + resName + ".csv");
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
                    }
             }catch (Exception e){
                logError("Failed to execute code on Cluster." + e);
                result.setResult(false);
                 e.printStackTrace();
            }
               
            
            
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
        retval += "		<file_name>" + fileName + "</file_name>" + Const.CR;
  
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
           
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    public boolean evaluates() {
        return true;
    }

    public boolean isUnconditional() {
        return true;
    }
    
    
    
    
    
   
}
