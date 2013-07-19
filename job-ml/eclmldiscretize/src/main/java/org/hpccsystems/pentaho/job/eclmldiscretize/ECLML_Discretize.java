/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclmldiscretize;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.ML_Discretize;
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
 * @author ChambersJ
 */
public class ECLML_Discretize extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    private String recordName = "";
    private String cooccur = "";
    private String routine = "";
    private String recordsetName = "";

    public String getCooccur() {
        return cooccur;
    }

    public void setCooccur(String cooccur) {
        this.cooccur = cooccur;
    }

    public String getRoutine() {
        return routine;
    }

    public void setRoutine(String routine) {
        this.routine = routine;
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
        
        //create direct populate here
    
    
        ML_Discretize ml = new ML_Discretize();
        ml.setName(this.getRecordsetName());
       
        ml.setRecordName(this.getRecordName());
        ml.setCooccur(this.getCooccur());
        ml.setRoutine(this.routine);
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
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordName")) != null)
            this.setRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordName")));

          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"cooccur")) != null)
            this.setCooccur(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"cooccur")));
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"routine")) != null)
            this.setRoutine(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"routine")));
            
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
            this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
    
        retval += "             <recordName><![CDATA["+this.recordName+"]]></recordName>"+Const.CR;
        retval += "             <cooccur><![CDATA["+this.cooccur+"]]></cooccur>"+Const.CR;  
        retval += "             <routine><![CDATA["+this.routine+"]]></routine>"+Const.CR; 
        retval += "             <recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {

            if(rep.getStepAttributeString(id_jobentry, "recordName") != null)
                recordName = rep.getStepAttributeString(id_jobentry, "recordName");

            if(rep.getStepAttributeString(id_jobentry, "cooccur") != null)
                cooccur = rep.getStepAttributeString(id_jobentry, "cooccur");
            
            if(rep.getStepAttributeString(id_jobentry, "routine") != null)
                routine = rep.getStepAttributeString(id_jobentry, "routine");
  
            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

            rep.saveStepAttribute(id_job, getObjectId(), "recordName", recordName);

            rep.saveStepAttribute(id_job, getObjectId(), "cooccur", cooccur);
            rep.saveStepAttribute(id_job, getObjectId(), "routine", routine);

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
