/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclclassification;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.ecldirect.ML_Classification;
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
public class ECLML_Classification extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";

    private String recordsetName = "";
    
    private String algType = ""; //NaiveBayes, Logistic    
    private String dependentVar = ""; // 1
    private String independentVar = ""; // 2

    public String getAlgType() {
        return algType;
    }

    public void setAlgType(String algType) {
        this.algType = algType;
    }

    public String getDependentVar() {
        return dependentVar;
    }

    public void setDependentVar(String dependentVar) {
        this.dependentVar = dependentVar;
    }

    public String getIndependentVar() {
        return independentVar;
    }

    public void setIndependentVar(String independentVar) {
        this.independentVar = independentVar;
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
        
        
    
    
        ML_Classification c = new ML_Classification();
        c.setName(this.getRecordsetName());
       

        c.setRecordsetName(this.getRecordsetName());
        c.setAlgType(algType);
        c.setDependentVar(dependentVar);
        c.setIndependentVar(independentVar);
        
        //private Text algType; //NaiveBayes, Logistic    
        //private Text dependentVar; // 1
        //private Text independentVar; // 2
        
        //ml.setIterations(this.getIterations());
        //ml.setThreshold(this.getThreshold());
        //ml.setThreshold(this.getThreshold());


        logBasic("{Iterate Job} Execute = " + c.ecl());
        
        logBasic("{Iterate Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, c.ecl());
        
        
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
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
            this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));

          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"algType")) != null)
            this.setAlgType(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"algType")));
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"dependentVar")) != null)
            this.setDependentVar(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"dependentVar")));
          
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"independentVar")) != null)
            this.setIndependentVar(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"independentVar")));
          
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "             <recordset_name><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
                
        retval += "             <algType><![CDATA["+this.algType+"]]></algType>"+Const.CR;
        retval += "             <dependentVar><![CDATA["+this.dependentVar+"]]></dependentVar>"+Const.CR;
        retval += "             <independentVar><![CDATA["+this.independentVar+"]]></independentVar>"+Const.CR;
        
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {

  
            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            
            if(rep.getStepAttributeString(id_jobentry, "algType") != null)
                algType = rep.getStepAttributeString(id_jobentry, "algType");
            
            if(rep.getStepAttributeString(id_jobentry, "dependentVar") != null)
                dependentVar = rep.getStepAttributeString(id_jobentry, "dependentVar");
            
            if(rep.getStepAttributeString(id_jobentry, "independentVar") != null)
                independentVar = rep.getStepAttributeString(id_jobentry, "independentVar");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            
            //private Text algType; //NaiveBayes, Logistic    
        //private Text dependentVar; // 1
        //private Text independentVar; // 2
            
            rep.saveStepAttribute(id_job, getObjectId(), "algType", algType);
            rep.saveStepAttribute(id_job, getObjectId(), "dependentVar", dependentVar);
            rep.saveStepAttribute(id_job, getObjectId(), "independentVar", independentVar);
           
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
