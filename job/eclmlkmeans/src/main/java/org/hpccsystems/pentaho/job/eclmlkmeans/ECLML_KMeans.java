/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclmlkmeans;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.ML_kmeans;
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
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author ChalaAX
 */
public class ECLML_KMeans extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    private String recordName = "";
    private String record2Name = "";
    private String recordsetName = "";
    private String iterations = "";
    private String threshold = "";

    public String getIterations() {
        return iterations;
    }

    public void setIterations(String iterations) {
        this.iterations = iterations;
    }

    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
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

    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        
        
    
    
        ML_kmeans ml = new ML_kmeans();
        ml.setName(this.getRecordsetName());
       
        ml.setRecordName(this.getRecordName());
        ml.setRecord2Name(this.getRecord2Name());
        ml.setRecordsetName(this.getRecordsetName());
        ml.setIterations(this.getIterations());
        ml.setThreshold(this.getThreshold());


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
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordName")) != null)
            this.setRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordName")));

          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record2Name")) != null)
            this.setRecord2Name(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"record2Name")));
            
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
            this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"iterations")) != null)
            this.setIterations(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"iterations")));
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"threshold")) != null)
            this.setThreshold(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"threshold")));
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "             <recordName><![CDATA["+this.recordName+"]]></recordName>"+Const.CR;
        retval += "             <record2Name><![CDATA["+this.record2Name+"]]></record2Name>"+Const.CR;  
        retval += "             <recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
        retval += "             <iterations><![CDATA["+this.iterations+"]]></iterations>"+Const.CR;
        retval += "             <threshold><![CDATA["+this.threshold+"]]></threshold>"+Const.CR;

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {

            if(rep.getStepAttributeString(id_jobentry, "recordName") != null)
                recordName = rep.getStepAttributeString(id_jobentry, "recordName");

            if(rep.getStepAttributeString(id_jobentry, "record2Name") != null)
                record2Name = rep.getStepAttributeString(id_jobentry, "record2Name");
  
            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            
            if(rep.getStepAttributeString(id_jobentry, "iterations") != null)
                iterations = rep.getStepAttributeString(id_jobentry, "iterations");
            
            if(rep.getStepAttributeString(id_jobentry, "threshold") != null)
                threshold = rep.getStepAttributeString(id_jobentry, "threshold");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

            rep.saveStepAttribute(id_job, getObjectId(), "recordName", recordName);

            rep.saveStepAttribute(id_job, getObjectId(), "record2Name", record2Name);

            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            
            rep.saveStepAttribute(id_job, getObjectId(), "iterations", iterations);
            rep.saveStepAttribute(id_job, getObjectId(), "threshold", threshold);
           
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
