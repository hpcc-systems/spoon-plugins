/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclml;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.ecldirect.ML_kmeans;
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
public class ECLML extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";

    
    private String record = "";
    private String recordName = "";
    
    private String record2 = "";
    private String record2Name = "";
    
    private String recordsetName = "";
    
    private String fieldType = "";
    private String fieldType2 = "";
    private String mlFunction = "";

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    public String getRecord2() {
        return record2;
    }

    public void setRecord2(String record2) {
        this.record2 = record2;
    }

    public String getRecord2Name() {
        return record2Name;
    }

    public void setRecord2Name(String record2Name) {
        this.record2Name = record2Name;
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

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldType2() {
        return fieldType2;
    }

    public void setFieldType2(String fieldType2) {
        this.fieldType2 = fieldType2;
    }

    public String getMlFunction() {
        return mlFunction;
    }

    public void setMlFunction(String mlFunction) {
        this.mlFunction = mlFunction;
    }

    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        
        
    
    
        ML_kmeans ml = new ML_kmeans();
        ml.setName(this.getRecordsetName());

        

        ml.setRecord(this.getRecord());
        ml.setRecordName(this.getRecordName());
        
        ml.setRecord2(this.getRecord2());
        ml.setRecord2Name(this.getRecord2Name());
        ml.setFieldType(this.fieldType);
        ml.setFieldType2(this.getFieldType2());
        ml.setMlFunction(this.getMlFunction());
        ml.setRecordsetName(this.getRecordsetName());


        logBasic("{Iterate Job} Execute = " + ml.ecl());
        
        logBasic("{Iterate Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, ml.ecl());
        
        
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
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record")) != null)
            this.setRecord(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record")));
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordName")) != null)
            this.setRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordName")));

            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record2")) != null)
                this.setRecord2(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record2")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record2Name")) != null)
                this.setRecord2Name(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record2Name")));
            
            
             if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fieldType")) != null)
                this.setFieldType(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fieldType")));
           if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fieldType2")) != null)
                this.setFieldType2(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fieldType")));
           if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"mlFunction")) != null)
                this.setMlFunction(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"mlFunction")));
          
           
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
                this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();

        retval += "             <record>"+this.record+"</record>"+Const.CR;
        retval += "             <recordName>"+this.recordName+"</recordName>"+Const.CR;
        retval += "             <record2>"+this.record2+"</record2>"+Const.CR;
        retval += "             <record2Name>"+this.record2Name+"</record2Name>"+Const.CR;
        
        retval += "             <fieldType>"+this.fieldType+"</fieldType>"+Const.CR;
        retval += "             <fieldType2>"+this.fieldType2+"</fieldType2>"+Const.CR;
        retval += "             <mlFunction>"+this.mlFunction+"</mlFunction>"+Const.CR;
        
        
        retval += "             <recordset_name>"+this.recordsetName+"</recordset_name>"+Const.CR;

       
       
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
            
            if(rep.getStepAttributeString(id_jobentry, "record2") != null)
                record2 = rep.getStepAttributeString(id_jobentry, "record2");
            if(rep.getStepAttributeString(id_jobentry, "record2Name") != null)
                record2Name = rep.getStepAttributeString(id_jobentry, "record2Name");
            
            
            if(rep.getStepAttributeString(id_jobentry, "fieldType") != null)
                fieldType = rep.getStepAttributeString(id_jobentry, "fieldType");
            if(rep.getStepAttributeString(id_jobentry, "fieldType2") != null)
                fieldType2 = rep.getStepAttributeString(id_jobentry, "fieldType2");
            if(rep.getStepAttributeString(id_jobentry, "mlFunction") != null)
                mlFunction = rep.getStepAttributeString(id_jobentry, "mlFUnction");
            
            
            
            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$

            rep.saveStepAttribute(id_job, getObjectId(), "record", record);
            rep.saveStepAttribute(id_job, getObjectId(), "recordName", recordName);
            
            rep.saveStepAttribute(id_job, getObjectId(), "record2", record2);
            rep.saveStepAttribute(id_job, getObjectId(), "record2Name", record2Name);
            
            rep.saveStepAttribute(id_job, getObjectId(), "fieldType", fieldType);
            rep.saveStepAttribute(id_job, getObjectId(), "fieldType2", fieldType2);
            rep.saveStepAttribute(id_job, getObjectId(), "mlFUnction", mlFunction);
            
            
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
           
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
