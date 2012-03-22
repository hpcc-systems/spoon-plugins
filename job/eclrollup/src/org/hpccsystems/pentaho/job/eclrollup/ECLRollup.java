/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclrollup;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.ecldirect.Rollup;
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
public class ECLRollup extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";

    private String recordsetName = "";
    private String recordset = "";
    private String condition = "";
    private String transformName = "";
    private String transform = "";
    private String transformCall = "";

    private String fieldlist = "";
    private String group = "";
    private Boolean runLocal = false;//optional

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }

    public String getTransformCall() {
        return transformCall;
    }

    public void setTransformCall(String transformCall) {
        this.transformCall = transformCall;
    }

    public String getTransformName() {
        return transformName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String GROUP) {
        this.group = GROUP;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getFieldlist() {
        return fieldlist;
    }

    public void setFieldlist(String fieldlist) {
        this.fieldlist = fieldlist;
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

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
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



    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        

 

 
    
    
        Rollup rollup = new Rollup();
        rollup.setName(this.getRecordsetName());
        rollup.setRecordset(this.getRecordset());
        rollup.setRunLocal(this.getRunLocal());
        
        rollup.setCondition(this.getCondition());
        rollup.setFieldlist(this.getFieldlist());
        rollup.setGroup(this.getGroup());
        
        rollup.setTransformName(this.getTransformName());
        rollup.setTransform(this.getTransform());
        rollup.setTransformCall(this.getTransformCall());
        
        
        /*
         *     private String recordset;
    private String condition;
    private String transform;
    private String fieldlist;
    private String group;
    private Boolean runLocal;
         */

        logBasic("{rollup Job} Execute = " + rollup.ecl());
        
        logBasic("{rollup Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, rollup.ecl());
        
        
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
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
                this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset")) != null)
                this.setRecordset(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"condition")) != null)
                this.setCondition(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"condition")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transformName")) != null)
                this.setTransformName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transformName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform")) != null)
                this.setTransform(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transformCall")) != null)
                this.setTransformCall(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transformCall")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fieldlist")) != null)
                this.setFieldlist(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fieldlist")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"group")) != null)
                this.setGroup(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"group")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")) != null)
                this.setRunLocalString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        
        retval += "             <recordset_name>"+this.recordsetName+"</recordset_name>"+Const.CR;
        retval += "             <recordset>"+this.recordset+"</recordset>"+Const.CR;
        retval += "             <condition>"+this.condition+"</condition>"+Const.CR;
        retval += "             <transformName>"+this.transformName+"</transformName>"+Const.CR;
        retval += "             <transform>"+this.transform+"</transform>"+Const.CR;
        retval += "             <transformCall>"+this.transformCall+"</transformCall>"+Const.CR;
        retval += "             <fieldlist>"+this.fieldlist+"</fieldlist>"+Const.CR;
        retval += "             <group>"+this.group+"</group>"+Const.CR;
        retval += "             <runLocal>"+this.getRunLocalString()+"</runLocal>"+Const.CR;
       

       
       
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

            //name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$


            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            if(rep.getStepAttributeString(id_jobentry, "recordset") != null)
                recordset = rep.getStepAttributeString(id_jobentry, "recordset");
            if(rep.getStepAttributeString(id_jobentry, "condition") != null)
                condition = rep.getStepAttributeString(id_jobentry, "condition");
            if(rep.getStepAttributeString(id_jobentry, "transformName") != null)
                transformName = rep.getStepAttributeString(id_jobentry, "transformName");
            if(rep.getStepAttributeString(id_jobentry, "transform") != null)
                transform = rep.getStepAttributeString(id_jobentry, "transform");
            if(rep.getStepAttributeString(id_jobentry, "transformCall") != null)
                transformCall = rep.getStepAttributeString(id_jobentry, "transformCall");
            if(rep.getStepAttributeString(id_jobentry, "fieldlist") != null)
                fieldlist = rep.getStepAttributeString(id_jobentry, "fieldlist");
            if(rep.getStepAttributeString(id_jobentry, "group") != null)
                group = rep.getStepAttributeString(id_jobentry, "group");            
            if(rep.getStepAttributeString(id_jobentry, "runLocal") != null)
                this.setRunLocalString(rep.getStepAttributeString(id_jobentry, "runLocal"));

        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$

            //rep.saveStepAttribute(id_job, getObjectId(), "name", name); //$NON-NLS-1$

            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset);
            rep.saveStepAttribute(id_job, getObjectId(), "condition", condition);
            rep.saveStepAttribute(id_job, getObjectId(), "transformName", transformName);
            rep.saveStepAttribute(id_job, getObjectId(), "transform", transform);
            rep.saveStepAttribute(id_job, getObjectId(), "transformCall", transformCall);
            rep.saveStepAttribute(id_job, getObjectId(), "fieldlist", fieldlist);
            rep.saveStepAttribute(id_job, getObjectId(), "group", group);
            rep.saveStepAttribute(id_job, getObjectId(), "runLocal", this.getRunLocalString());
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
