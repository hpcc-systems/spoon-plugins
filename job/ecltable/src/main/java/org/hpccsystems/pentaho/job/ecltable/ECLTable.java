/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecltable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Table;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
//import org.pentaho.di.job.entry.JobEntryBase;
//import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.ecljobentrybase.*;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;

/**
 *
 * @author ChambersJ
 */
public class ECLTable extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    
    private String recordsetName = "";
    private String recordset = "";
    private String format = "";
    private String expression = "";
    
    private String size = ""; //FEW,MANY
    private Boolean isUnsorted = false;
    private Boolean runLocal = false;
    private Boolean isKeyed = false;
    private Boolean isMerge = false;

    private RecordList recordList = new RecordList();

    public RecordList getRecordList() {
        return recordList;
    }

    public void setRecordList(RecordList recordList) {
        this.recordList = recordList;
    }

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Boolean getIsKeyed() {
        return isKeyed;
    }

    public void setIsKeyed(Boolean isKeyed) {
        this.isKeyed = isKeyed;
    }

    public Boolean getIsMerge() {
        return isMerge;
    }

    public void setIsMerge(Boolean isMerge) {
        this.isMerge = isMerge;
    }

    public Boolean getIsUnsorted() {
        return isUnsorted;
    }

    public void setIsUnsorted(Boolean isUnsorted) {
        this.isUnsorted = isUnsorted;
    }

    public String getRecordset() {
        return recordset;
    }

    public void setRecordset(String recordset) {
        this.recordset = recordset;
    }

    public Boolean getRunLocal() {
        return runLocal;
    }

    public void setRunLocal(Boolean runLocal) {
        this.runLocal = runLocal;
    }

    public void setIsUnsortedString(String isUnsorted) {
        
        if(isUnsorted.equals("true")){
            this.isUnsorted = true;
        }else{
            this.isUnsorted = false;
        }
        
    }
    
    public String getIsUnsortedString() {
        if(this.isUnsorted != null && this.isUnsorted){
            return "true";
        }else{
            return "false";
        }
    }
    
    public void setRunLocalString(String runLocal) {
        
        if(runLocal.equals("true")){
            this.runLocal = true;
        }else{
            this.runLocal = false;
        }
        
    }
    
    public String getRunLocalString() {
        if(this.runLocal != null && this.runLocal){
            return "true";
        }else{
            return "false";
        }
    }
    
     public void setIsKeyedString(String isKeyed) {
        
        if(isKeyed.equals("true")){
            this.isKeyed = true;
        }else{
            this.isKeyed = false;
        }
        
    }
    
    public String getIsKeyedString() {
        if(this.isKeyed != null && this.isKeyed){
            return "true";
        }else{
            return "false";
        }
    }
    
    public void setIsMergeString(String isMerge) {
        
        if(isMerge.equals("true")){
            this.isMerge = true;
        }else{
            this.isMerge = false;
        }
        
    }
    
    public String getIsMergeString() {
        if(this.isMerge != null && this.isMerge){
            return "true";
        }else{
            return "false";
        }
    }

    public String resultListToString(RecordList recordList){
        String out = "";
        
        if(recordList != null){
            if(recordList.getRecords() != null && recordList.getRecords().size() > 0) {
                    System.out.println("Size: "+recordList.getRecords().size());
                    for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO record = (RecordBO) iterator.next();
                        	String rLen = record.getColumnWidth();
        					if (rLen != null && rLen.trim().length() >0) {
                                if(record.getColumnName() != null && !record.getColumnName().equals("")){
                                    out += record.getColumnType()+rLen + " " + record.getColumnName();
                                    if(record.getDefaultValue() != ""){
                                        out += " := " + record.getDefaultValue();
                                    }
                                    out += ";\r\n";
                                 }
                            }else{
                                if(record.getColumnName() != null && !record.getColumnName().equals("")){
                                    out += record.getColumnType() + " " + record.getColumnName();
                                    if(record.getDefaultValue() != ""){
                                        out += " := " + record.getDefaultValue();
                                    }
                                    out += ";\r\n";
                                }
                            }
                            
                            
                    }
            }
        }
        
        return out;
    }
    
    public String saveRecordList(){
        String out = "";
        ArrayList list = recordList.getRecords();
        Iterator<RecordBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
    public void openRecordList(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            recordList = new RecordList();
            //System.out.println("Open Record List");
            for(int i =0; i<len; i++){
                //System.out.println("++++++++++++" + strLine[i]);
                //this.recordDef.addRecord(new RecordBO(strLine[i]));
                RecordBO rb = new RecordBO(strLine[i]);
                //System.out.println(rb.getColumnName());
                recordList.addRecordBO(rb);
            }
        }
    }
    public String resultListToString(){
    	return resultListToString(this.recordList);
    }
    
    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        

        
        Table table = new Table();
        table.setName(this.getRecordsetName());
        table.setRecordset(this.getRecordset());
        //table.setFormat(this.getFormat());
        table.setExpression(this.getExpression());
        table.setSize(this.getSize());
        
        table.setIsUnsorted(this.getIsUnsorted());
        table.setRunLocal(this.getRunLocal());
        table.setIsKeyed(this.getIsKeyed());
        table.setIsMerge(this.getIsMerge());
        table.setFormat(resultListToString());

        logBasic("{Table Job} Execute = " + table.ecl());
        
        logBasic("{Table Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, table.ecl());
        
        
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
            logBasic("{Table Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
                this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset")) != null)
                this.setRecordset(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset")));
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"format")) != null)
            //    this.setFormat(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"format")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"expression")) != null)
                this.setExpression(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"expression")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"size")) != null)
                this.setSize(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"size")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isUnsorted")) != null)
                this.setIsUnsortedString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isUnsorted")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")) != null)
                this.setRunLocalString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isKeyed")) != null)
                this.setIsKeyedString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isKeyed")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isMerge")) != null)
                this.setIsMergeString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isMerge")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
                openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      //<![CDATA["+this.model+"]]>
        retval += "             <recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
        
        retval += "             <recordset><![CDATA["+this.recordset+"]]></recordset>"+Const.CR;
        //retval += "             <format><![CDATA["+this.format+"]]></format>"+Const.CR;
        retval += "             <expression><![CDATA[" + this.expression + "]]></expression>"+Const.CR;
        retval += "             <size><![CDATA["+this.size+"]]></size>"+Const.CR;
        retval += "             <isUnsorted><![CDATA["+this.getIsUnsortedString()+"]]></isUnsorted>"+Const.CR;
        retval += "             <runLocal><![CDATA["+this.getRunLocalString()+"]]></runLocal>"+Const.CR;
        retval += "             <isKeyed><![CDATA["+this.getIsKeyedString()+"]]></isKeyed>"+Const.CR;
        retval += "             <isMerge><![CDATA["+this.getIsMergeString()+"]]></isMerge>"+Const.CR;
        
        retval += "		<recordList><![CDATA[" + this.saveRecordList() + "]]></recordList>" + Const.CR;
       
       
       
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

            //name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$
            recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            
            recordset = rep.getStepAttributeString(id_jobentry, "recordset");
            //format = rep.getStepAttributeString(id_jobentry, "format");
            expression = rep.getStepAttributeString(id_jobentry, "expression");
            size = rep.getStepAttributeString(id_jobentry, "size");
            this.setIsUnsortedString(rep.getStepAttributeString(id_jobentry, "isUnsorted"));
            this.setRunLocalString(rep.getStepAttributeString(id_jobentry, "runLocal"));
            this.setIsKeyedString(rep.getStepAttributeString(id_jobentry, "isKeyed"));
            this.setIsMergeString(rep.getStepAttributeString(id_jobentry, "isMerge"));
            
            if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
                this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

            
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset);
            //rep.saveStepAttribute(id_job, getObjectId(), "format", format);
            rep.saveStepAttribute(id_job, getObjectId(), "expression", expression);
            rep.saveStepAttribute(id_job, getObjectId(), "size", size);
            rep.saveStepAttribute(id_job, getObjectId(), "isUnsorted", this.getIsUnsortedString());
            rep.saveStepAttribute(id_job, getObjectId(), "runLocal", this.getRunLocalString());
            rep.saveStepAttribute(id_job, getObjectId(), "isKeyed", this.getIsKeyedString());
            rep.saveStepAttribute(id_job, getObjectId(), "isMerge", this.getIsMergeString());
            rep.saveStepAttribute(id_job, getObjectId(), "recordList", this.saveRecordList()); //$NON-NLS-1$
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
