/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecliterate;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.ecldirect.Iterate;
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

/**
 *
 * @author ChalaAX
 */
public class ECLIterate extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    private String transform = "";
    private String transformName = "";
    private String recordset = "";//Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    private Boolean runLocal = false;
    
    private String record = "";
    private String recordName = "";
    private String recordsetName = "";
    
    private String returnType = "";
    
    private String recordsetNameIterate = "";
    
    private String transformCall = "";

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    
    
    public String getTransformCall() {
        return transformCall;
    }

    public String getRecordsetNameIterate() {
        return recordsetNameIterate;
    }

    public void setRecordsetNameIterate(String recordsetNameIterate) {
        this.recordsetNameIterate = recordsetNameIterate;
    }

    public void setTransformCall(String transformCall) {
        this.transformCall = transformCall;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    public String getRecordName() {
        return recordName;
    }

    public void setRecordName(String recordName) {
        this.recordName = recordName;
    }

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }


    public String getTransformName() {
        return transformName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
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
    
    public void setRunLocalString(String runLocal) {
        
        if(runLocal.equals("true")){
            this.runLocal = true;
        }else{
            this.runLocal = false;
        }
        
    }
    
    public String getRunLocalString() {
        if(runLocal){
            return "true";
        }else{
            return "false";
        }
    }



    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }
    
   
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        

    
    
        Iterate iterate = new Iterate();
        iterate.setName(this.getRecordsetNameIterate());
        iterate.setRecordset(this.getRecordset());
        iterate.setRunLocal(this.getRunLocal());
        
        iterate.setTransform(this.getTransform());
        iterate.setTransformName(this.getTransformName());
        iterate.setRecord(this.getRecord());
        iterate.setRecordName(this.getRecordName());
        iterate.setRecordsetName(this.getRecordsetName());
        iterate.setTransformCall(this.getTransformCall());
        iterate.setReturnType(returnType);

        logBasic("{Iterate Job} Execute = " + iterate.ecl());
        
        logBasic("{Iterate Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, iterate.ecl());
        
        
        List list = result.getRows();
        list.add(data);
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
        
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            //this.setName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform_name")) != null)
                this.setTransformName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform")) != null)
                this.setTransform(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record")) != null)
                this.setRecord(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record_name")) != null)
                this.setRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
                this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name_iterate")) != null)
                this.setRecordsetNameIterate(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name_iterate")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transformCall")) != null)
                this.setTransformCall(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transformCall")));
           
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"returnType")) != null)
                this.setReturnType(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"returnType")));
            
            
            this.setRecordset(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset")));
            this.setRunLocalString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")));
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        //retval += "		<job_name>" + jobName + "</jobName>" + Const.CR;
        //retval += "		<name>" + name + "</name>" + Const.CR;
                                       /*
    private Text record;
    private Text recordName;
    private Text recordsetName;
    */
        
      
        retval += "             <record><![CDATA["+this.record+"]]></record>"+Const.CR;
        retval += "             <record_name eclIsDef=\"true\" eclType=\"record\"><![CDATA["+this.recordName+"]]></record_name>"+Const.CR;
        retval += "             <recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
        //resulting recordset
        retval += "             <recordset_name_iterate eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetNameIterate+"]]></recordset_name_iterate>"+Const.CR;
        
        
        retval += "             <transform_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.transformName+"]]></transform_name>"+Const.CR;
        retval += "             <transform><![CDATA["+this.transform+"]]></transform>"+Const.CR;
        retval += "             <transformCall><![CDATA["+this.transformCall+"]]></transformCall>"+Const.CR;
        retval += "             <recordset><![CDATA["+this.recordset+"]]></recordset>"+Const.CR;
        retval += "             <runLocal><![CDATA["+this.getRunLocalString()+"]]></runLocal>"+Const.CR;
        retval += "             <returnType><![CDATA["+this.getReturnType()+"]]></returnType>"+Const.CR;
       
       
       
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

            //name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "record") != null)
                record = rep.getStepAttributeString(id_jobentry, "record");
            if(rep.getStepAttributeString(id_jobentry, "recordName") != null)
                recordName = rep.getStepAttributeString(id_jobentry, "recordName");
            if(rep.getStepAttributeString(id_jobentry, "recordsetName") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordsetName");
            if(rep.getStepAttributeString(id_jobentry, "recordset_name_iterate") != null)
                recordsetNameIterate = rep.getStepAttributeString(id_jobentry, "recordset_name_iterate");
            
            if(rep.getStepAttributeString(id_jobentry, "transformName") != null)
                transformName = rep.getStepAttributeString(id_jobentry, "transformName");
            if(rep.getStepAttributeString(id_jobentry, "transform") != null)
                transform = rep.getStepAttributeString(id_jobentry, "transform");
            if(rep.getStepAttributeString(id_jobentry, "transformCall") != null)
                transformCall = rep.getStepAttributeString(id_jobentry, "transformCall");
            if(rep.getStepAttributeString(id_jobentry, "recordset") != null)
                recordset = rep.getStepAttributeString(id_jobentry, "recordset");
            if(rep.getStepAttributeString(id_jobentry, "runLocal") != null)
                this.setRunLocalString(rep.getStepAttributeString(id_jobentry, "runLocal"));
            if(rep.getStepAttributeString(id_jobentry, "returnType") != null)
                this.setReturnType(rep.getStepAttributeString(id_jobentry, "returnType"));

        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$

            rep.saveStepAttribute(id_job, getObjectId(), "record", record);
            rep.saveStepAttribute(id_job, getObjectId(), "recordName", recordName);
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetName", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name_iterate", recordsetNameIterate);
            
            
            //rep.saveStepAttribute(id_job, getObjectId(), "name", name); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "transformName", transformName);
            rep.saveStepAttribute(id_job, getObjectId(), "transform", transform);
            rep.saveStepAttribute(id_job, getObjectId(), "transformCall", transformCall);
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset);
            rep.saveStepAttribute(id_job, getObjectId(), "runLocal", this.getRunLocalString());
            rep.saveStepAttribute(id_job, getObjectId(), "returnType", this.getReturnType());
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
