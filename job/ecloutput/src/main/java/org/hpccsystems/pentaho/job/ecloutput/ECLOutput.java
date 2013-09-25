/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecloutput;

import java.util.ArrayList;

import java.util.List;
import org.hpccsystems.javaecl.Output;
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
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author ChambersJ
 */
public class ECLOutput extends ECLJobEntry {
    

    private String recordset = "";
    //private String fileName = "";
    //private String serverAddress = "";
    //private String serverPort = "";
    
    //public static boolean isReady = false;

    
    //enhanced output fields
    private String isDef = ""; //true set output into attr using job entry name
    private String outputType = ""; //recordset,expression
    private String includeFormat = ""; //yes/no
    private String inputType = ""; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
    private String outputFormat = "";
    private String expression = "";
    private String file = "";
    private String typeOptions = ""; // used for CSV, XML, Pipe
    private String fileOptions = ""; // used for CSV, XML
    private String named = ""; //used for named
    private String extend = ""; // availiable for Named
    private String returnAll = ""; // availiable for Named
    private String thor = ""; // used in 
    private String cluster = "";
    private String encrypt = "";
    private String compressed = "";
    private String overwrite = "";
    private String expire = "";
    private String repeat = "";// piped
    private String pipeType = "";


    public String getRecordset() {
        return recordset;
    }
    public void setRecordset(String recordset) {
        this.recordset = recordset;
    }
    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getCompressed() {
        return compressed;
    }

    public void setCompressed(String compressed) {
        this.compressed = compressed;
    }

    public String getEncrypt() {
        return encrypt;
    }

    public void setEncrypt(String encrypt) {
        this.encrypt = encrypt;
    }

    public String getExpire() {
        return expire;
    }

    public void setExpire(String expire) {
        this.expire = expire;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getExtend() {
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getFileOptions() {
        return fileOptions;
    }

    public void setFileOptions(String fileOptions) {
        this.fileOptions = fileOptions;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String format) {
        this.outputFormat = format;
    }

    public String getIncludeFormat() {
        return includeFormat;
    }

    public void setIncludeFormat(String includeFormat) {
        this.includeFormat = includeFormat;
    }

    public String getIsDef() {
        //System.out.println("getIsDef() = " + isDef);
        return isDef;
    }

    public void setIsDef(String isDef) {
        this.isDef = isDef;
    }

    public String getTypeOptions() {
        return typeOptions;
    }

    public void setTypeOptions(String options) {
        this.typeOptions = options;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public String getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(String overwrite) {
        this.overwrite = overwrite;
    }

    public String getPipeType() {
        return pipeType;
    }

    public void setPipeType(String pipeType) {
        this.pipeType = pipeType;
    }

    public String getRepeat() {
        return repeat;
    }

    public void setRepeat(String repeat) {
        this.repeat = repeat;
    }

    public String getReturnAll() {
        return returnAll;
    }

    public void setReturnAll(String returnAll) {
        this.returnAll = returnAll;
    }

    public String getThor() {
        return thor;
    }

    public void setThor(String thor) {
        this.thor = thor;
    }

    public String getInputType() {
        return inputType;
    }

    public void setInputType(String inputType) {
        this.inputType = inputType;
    }

    public String getNamed() {
        //System.out.println("getNamed() - " + named + "}");
        return named;
    }

    public void setNamed(String named) {
        this.named = named;
    }

    
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
    	Result result = modifyResults(prevResult);
    	if(result.isStopped()){
        	return result;
 		}
        //ECLOutput.isReady=true;
        //logBasic("not waiting: " + ECLOutput.isReady);
        Output op = new Output();
        op.setRecordset(getRecordset());
        op.setIsDef(getIsDef());
        op.setOutputType(getOutputType());
        op.setIncludeFormat(getIncludeFormat());
        op.setInputType(getInputType());
        op.setOutputFormat(getOutputFormat());
        op.setExpression(getExpression());
        op.setFile(getFile());
        op.setTypeOptions(getTypeOptions());
        op.setFileOptions(getFileOptions());
        op.setNamed(getNamed());
        op.setExtend(getExtend());
        op.setReturnAll(getReturnAll());
        op.setThor(getThor());
        op.setCluster(getCluster());
        op.setEncrypt(getEncrypt());
        op.setCompressed(getCompressed());
        op.setOverwrite(getOverwrite());
        op.setExpire(getExpire());
        op.setRepeat(getRepeat());
        op.setPipeType(getPipeType());
        op.setName(this.getName());


        String opResults = op.ecl();

        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, opResults + "\r\n");
        
        
        List list = result.getRows();
        list.add(data);
        String eclCode = parseEclFromRowData(list);
        /*
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
            logBasic("{Iterate Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        
        
        return result;
        
    }
    
    public String loadXMLElement(Node node,String key){
        String rStr = "";
        if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, key)) != null){
                rStr = XMLHandler.getNodeValue(XMLHandler.getSubNode(node, key));
                //System.out.println("load XML Element " + key + " = " + XMLHandler.getNodeValue(XMLHandler.getSubNode(node, key)) + "|");
        }
        return rStr;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
             //System.out.println(" ------------ loadXML ------------- ");
            super.loadXML(node, list, list1);

            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset")) != null)
                setRecordset(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset")));
            
                //private String isDef; //true set output into attr using job entry name
            this.setIsDef(loadXMLElement(node,"isDef"));

            //private String outputType; //recordset,expression
            this.setOutputType(loadXMLElement(node,"outputType"));
                //private String includeFormat; //yes/no
            this.setIncludeFormat(loadXMLElement(node,"includeFormat"));
                //private String type; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
            this.setInputType(loadXMLElement(node,"inputType"));
                //private String format;
            this.setOutputFormat(loadXMLElement(node,"outputFormat"));
                //private String expression;
            this.setExpression(loadXMLElement(node,"expression"));
                //private String file;
            this.setFile(loadXMLElement(node,"file"));
                //private String options; // used for CSV, XML, Pipe
            this.setTypeOptions(loadXMLElement(node,"typeOptions"));
                //private String fileOptions; // used for CSV, XML
            this.setFileOptions(loadXMLElement(node,"fileOptions"));
                //private String name; //used for named
            this.setNamed(loadXMLElement(node,"named"));
                //private String extend; // availiable for Named
            this.setExtend(loadXMLElement(node,"extend"));
                //private String returnAll; // availiable for Named
            this.setReturnAll(loadXMLElement(node,"returnAll"));
                //private String thor; // used in 
            this.setThor(loadXMLElement(node,"thor"));
                //private String cluster;
            this.setCluster(loadXMLElement(node,"cluster"));
                //private String encrypt;
            this.setEncrypt(loadXMLElement(node,"encrypt"));
                //private String compressed;
            this.setCompressed(loadXMLElement(node,"compressed"));
                //private String overwrite;
            this.setOverwrite(loadXMLElement(node,"overwrite"));
                //private String expire;
            this.setExpire(loadXMLElement(node,"expire"));
                //private String repeat;// piped
            this.setRepeat(loadXMLElement(node,"repeat"));
                //private String pipeType;
            this.setPipeType(loadXMLElement(node,"pipeType"));
        
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "server_address")) != null)
                //setServerAddress(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "server_address")));
           // if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_name")) != null)
           //     setFileName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_name")));


        } catch (Exception e) {
            e.printStackTrace();
            throw new KettleXMLException("ECL Output Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXMLElement(String key, String value){
    	
        return "        <" + key + "><![CDATA["+value+"]]></"+key+">"+ Const.CR;
    }
    public String getXML() {
        String retval = "";
        // System.out.println(" ------------ getXML ------------- ");
        retval += super.getXML();
     
        retval += "		<recordset><![CDATA[" + recordset + "]]></recordset>" + Const.CR;
            //private String isDef; //true set output into attr using job entry name
    
        retval += getXMLElement("isDef", this.getIsDef());
            //private String outputType; //recordset,expression
         retval += getXMLElement("outputType", this.getOutputType());
            //private String includeFormat; //yes/no
         retval += getXMLElement("includeFormat", this.getIncludeFormat());
            //private String type; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
         retval += getXMLElement("inputType", this.getInputType());
            //private String format;
         retval += getXMLElement("outputFormat", this.getOutputFormat());
            //private String expression;
         retval += getXMLElement("expression", this.getExpression());
            //private String file;
         retval += getXMLElement("file", this.getFile());
            //private String options; // used for CSV, XML, Pipe
         retval += getXMLElement("typeOptions", this.getTypeOptions());
            //private String fileOptions; // used for CSV, XML
         retval += getXMLElement("fileOptions", this.getFileOptions());
            //private String name; //used for named
         retval += getXMLElement("named", this.getNamed());
            //private String extend; // availiable for Named
         retval += getXMLElement("extend", this.getExtend());
            //private String returnAll; // availiable for Named
         retval += getXMLElement("returnAll", this.getReturnAll());
            //private String thor; // used in 
         retval += getXMLElement("thor", this.getThor());
            //private String cluster;
         retval += getXMLElement("cluster", this.getCluster());
            //private String encrypt;
         retval += getXMLElement("encrypt", this.getEncrypt());
            //private String compressed;
         retval += getXMLElement("compressed", this.getCompressed());
            //private String overwrite;
         retval += getXMLElement("overwrite", this.getOverwrite());
            //private String expire;
         retval += getXMLElement("expire", this.getExpire());
            //private String repeat;// piped
         retval += getXMLElement("repeat", this.getRepeat());
            //private String pipeType;
        retval += getXMLElement("pipeType", this.pipeType);
        
        
       // retval += "		<server_address>" + serverAddress + "</server_address>" + Const.CR;
       // retval += "		<file_name>" + fileName + "</file_name>" + Const.CR;
  
        //System.out.println(" end getXML ");
        return retval;

    }

    public String getRepElement(Repository rep, ObjectId id_jobentry, String key) throws KettleException{
        String rStr = "";
        try{
        if(rep.getStepAttributeString(id_jobentry, key) != null)
                rStr = rep.getStepAttributeString(id_jobentry, key); //$NON-NLS-1$
        }catch (Exception e){
             e.printStackTrace();
            throw new KettleException("Unexpected Exception", e);
        }
        return rStr;
    }
    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        //System.out.println(" ------------ loadRep " + id_jobentry + "------------- ");
        try {
          
            if(rep.getStepAttributeString(id_jobentry, "recordset") != null)
                recordset = rep.getStepAttributeString(id_jobentry, "recordset"); //$NON-NLS-1$
            
        } catch (Exception e) {
             e.printStackTrace();
            throw new KettleException("Unexpected Exception", e);
            
        }
        
        //private String isDef; //true set output into attr using job entry name
        this.setIsDef(getRepElement(rep, id_jobentry, "isDef"));
            //private String outputType; //recordset,expression
        this.setOutputType(getRepElement(rep, id_jobentry, "outputType"));
            //private String includeFormat; //yes/no
        this.setIncludeFormat (getRepElement(rep, id_jobentry, "includeFormat"));
            //private String type; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
        this.setInputType(getRepElement(rep, id_jobentry, "inputType"));
            //private String format;
        this.setOutputFormat(getRepElement(rep, id_jobentry, "outputFormat"));
            //private String expression;
        this.setExpression(getRepElement(rep, id_jobentry, "expression"));
            //private String file;
        this.setFile(getRepElement(rep, id_jobentry, "file"));
            //private String options; // used for CSV, XML, Pipe
        this.setTypeOptions(getRepElement(rep, id_jobentry, "typeOptions"));
            //private String fileOptions; // used for CSV, XML
        this.setFileOptions(getRepElement(rep, id_jobentry, "fileOptions"));
            //private String name; //used for named
        this.setNamed(getRepElement(rep, id_jobentry, "named"));
            //private String extend; // availiable for Named
        this.setExtend(getRepElement(rep, id_jobentry, "extend"));
            //private String returnAll; // availiable for Named
        this.setReturnAll(getRepElement(rep, id_jobentry, "returnAll"));
            //private String thor; // used in 
        this.setThor(getRepElement(rep, id_jobentry, "thor"));
            //private String cluster;
        this.setCluster(getRepElement(rep, id_jobentry, "cluster"));
            //private String encrypt;
        this.setEncrypt(getRepElement(rep, id_jobentry, "encrypt"));
            //private String compressed;
        this.setCompressed(getRepElement(rep, id_jobentry, "compressed"));
            //private String overwrite;
        this.setOverwrite(getRepElement(rep, id_jobentry, "overwrite"));
            //private String expire;
        this.setExpire(getRepElement(rep, id_jobentry, "expire"));
            //private String repeat;// piped
        this.setRepeat(getRepElement(rep, id_jobentry, "repeat"));
            //private String pipeType;
        this.setPipeType(getRepElement(rep, id_jobentry, "pipeType"));
    }

    public void saveRepElement(Repository rep, ObjectId id_job,String key,String value) throws KettleException{
        try{
            rep.saveStepAttribute(id_job, getObjectId(), key, value); //$NON-NLS-1$
        }catch (Exception e){
             e.printStackTrace();
             throw new KettleException("Unable to save info into repository" + id_job, e);
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
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset); //$NON-NLS-1$
            
            //private String isDef; //true set output into attr using job entry name
            saveRepElement(rep,id_job,"isDef",this.getIsDef());
            //private String outputType; //recordset,expression
            saveRepElement(rep,id_job,"outputType",this.getOutputType());
            //private String includeFormat; //yes/no
            saveRepElement(rep,id_job,"includeFormat",this.getIncludeFormat());
            //private String type; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
            saveRepElement(rep,id_job,"inputType",this.getInputType());
            //private String format;
            saveRepElement(rep,id_job,"outputFormat",this.getOutputFormat());
            //private String expression;
            saveRepElement(rep,id_job,"expression",this.getExpression());
            //private String file;
            saveRepElement(rep,id_job,"file",this.getFile());
            //private String options; // used for CSV, XML, Pipe
            saveRepElement(rep,id_job,"typeOptions",this.getTypeOptions());
            //private String fileOptions; // used for CSV, XML
            saveRepElement(rep,id_job,"fileOptions",this.getFileOptions());
            //private String name; //used for named
            saveRepElement(rep,id_job,"named",this.getNamed());
            //private String extend; // availiable for Named
            saveRepElement(rep,id_job,"extend",this.getExtend());
            //private String returnAll; // availiable for Named
            saveRepElement(rep,id_job,"returnALl",this.getReturnAll());
            //private String thor; // used in 
            saveRepElement(rep,id_job,"thor",this.getThor());
            //private String cluster;
            saveRepElement(rep,id_job,"cluster",this.getCluster());
            //private String encrypt;
            saveRepElement(rep,id_job,"encrypt",this.getEncrypt());
            //private String compressed;
            saveRepElement(rep,id_job,"compressed",this.getCompressed());
            //private String overwrite;
            saveRepElement(rep,id_job,"overwrite",this.getOverwrite());
            //private String expire;
            saveRepElement(rep,id_job,"expire",this.getExpire());
            //private String repeat;// piped
            saveRepElement(rep,id_job,"repeat",this.getRepeat());
            //private String pipeType;
            saveRepElement(rep,id_job,"pipeType",this.getPipeType());
            
            //rep.saveStepAttribute(id_job, getObjectId(), "serverAddress", serverAddress); //$NON-NLS-1$
            //rep.saveStepAttribute(id_job, getObjectId(), "fileName", fileName); //$NON-NLS-1$
           
        } catch (Exception e) {
             e.printStackTrace();
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

   

    
    
    
    
    
    
    
}
