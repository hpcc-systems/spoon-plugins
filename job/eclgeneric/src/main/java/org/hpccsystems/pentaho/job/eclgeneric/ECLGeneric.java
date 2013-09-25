/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclgeneric;

import java.util.ArrayList;
import java.util.List;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
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
public class ECLGeneric extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    
    private String recordsetName = "";
    
    private String ECLCode = "";

    public String getECLCode() {
        return ECLCode;
    }

    public void setECLCode(String ECLCode) {
        this.ECLCode = ECLCode;
    }

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }
    
    
    
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
    	Result result = modifyResults(prevResult);
    	if(result.isStopped()){
        	return result;
 		}

        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, this.ECLCode + "\r\n");
        
        
        List list = result.getRows();
        list.add(data);
        
        //String eclCode = parseEclFromRowData(list);
        
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
        if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"ECLCode")) != null)
                this.setECLCode(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"ECLCode")));
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();

      
        retval += "             <recordset_name><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
        retval += "             <ECLCode><![CDATA["+this.ECLCode+"]]></ECLCode>"+Const.CR;

       
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

            //name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$
            
                  
            
            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            
            if(rep.getStepAttributeString(id_jobentry, "ECLCode") != null)
                ECLCode = rep.getStepAttributeString(id_jobentry, "ECLCode");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "ECLCode", ECLCode);
           
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }


}
