/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecldistribute;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.Distribute;
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
public class ECLDistribute extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    

    private String recordsetName = "";
    private String datasetName = "";
    private String expression = "";
    private String index = "";
    private String joinCondition = "";
    private String skew = "";

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }
    
   // public String getJobName(){
   //     return jobName;
   // }
   // public void setJobName(String jobName){
   //     this.jobName = jobName;
   // }

    
    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }
    
    public String getExpression(){
        return expression;
    }
    public void setExpression(String expression){
        this.expression = expression;
    }
    
    public String getIndex(){
        return index;
    }
    public void setIndex(String index){
        this.index=index;
    }
    
    public String getJoinCondition(){
        return this.joinCondition;
    }
    
    public void setJoinCondition(String jc){
        this.joinCondition = jc;
    }
    
    public String getSkew(){
        return this.skew;
    }
    public void setSkew(String skew){
        this.skew = skew;
    }

   
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
    	Result result = modifyResults(prevResult);
    	if(result.isStopped()){
        	return result;
 		}
        Distribute distribute = new Distribute();
        distribute.setName(this.getRecordsetName());
        distribute.setDatasetName(this.getDatasetName());
        distribute.setExpression(this.getExpression());
        distribute.setIndex(this.getIndex());
        distribute.setJoinCondition(this.getJoinCondition());
        distribute.setSkew(this.getSkew());
        

        logBasic("{Distribute Job} Execute = " + distribute.ecl());
        
        logBasic("{Distribute Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, distribute.ecl());
        
        
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
            logBasic("{Distribute Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            //setJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "jobName")));
           
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")) != null)
            //    setName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")) != null)
                setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "expression")) != null)
                setExpression(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "expression")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "index")) != null)
                setIndex(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "index")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "join_condition")) != null)
                setJoinCondition(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "join_condition")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "skew")) != null)
                setSkew(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "skew")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        //retval += "		<job_name>" + jobName + "</jobName>" + Const.CR;
        //retval += "		<name>" + name + "</name>" + Const.CR;
      
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<dataset_name><![CDATA[" + datasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<expression><![CDATA[" + expression + "]]></expression>" + Const.CR;
        retval += "		<index><![CDATA[" + index + "]]></index>" + Const.CR;
        retval += "		<join_condition><![CDATA[" + joinCondition + "]]></join_condition>" + Const.CR;
        retval += "		<skew><![CDATA[" + skew + "]]></skew>" + Const.CR;
       
       
       
       
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$
           // if(rep.getStepAttributeString(id_jobentry, "name") != null)
            //    name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "recordsetName") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordsetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "expression") != null)
                expression = rep.getStepAttributeString(id_jobentry, "expression"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "index") != null)
                index = rep.getStepAttributeString(id_jobentry, "index"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "joinCondition") != null)
                joinCondition = rep.getStepAttributeString(id_jobentry, "joinCondition"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "skew") != null)
                skew = rep.getStepAttributeString(id_jobentry, "skew"); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$
           
           // rep.saveStepAttribute(id_job, getObjectId(), "name", name); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetName", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "expression", expression); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "index", index); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "joinCondition", joinCondition); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "skew", skew); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    
}
