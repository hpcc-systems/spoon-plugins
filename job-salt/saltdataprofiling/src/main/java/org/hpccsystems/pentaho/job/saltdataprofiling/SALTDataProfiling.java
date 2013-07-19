/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltdataprofiling;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.SaltDataProfilingAPI;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
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
public class SALTDataProfiling extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
	
    private String datasetName;
    private String layout;
	
   
	public String getDatasetName() {
		return datasetName;
	}
	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}
	public String getLayout() {
		return layout;
	}
	public void setLayout(String layout) {
		this.layout = layout;
	}


    
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        
        AutoPopulate ap = new AutoPopulate();
        String jobNameNoSpace = "";
        JobMeta jobMeta = super.parentJob.getJobMeta();
        
        System.out.println("Fetch AP for saltDP");
        try{
        //Object[] jec = this.jobMeta.getJobCopies().toArray();

            String jobName = ap.getGlobalVariable(jobMeta.getJobCopies(),"jobName");
            jobNameNoSpace = jobName.replace(" ", ""); 
            
            this.setLayout(ap.getDatasetsField("record_name", this.getDatasetName(),jobMeta.getJobCopies()));
           // System.out.println("----------------- Layout: " + this.layout);
           // System.out.println(ap.getDatasetsField("record_name", this.getDatasetName(),jobMeta.getJobCopies()));
        }catch (Exception e){
            System.out.println("Error Parsing existing Global Variables ");
            System.out.println(e.toString());
            e.printStackTrace();

        }
        
        SaltDataProfilingAPI sdp = new SaltDataProfilingAPI();
        
        sdp.setDatasetName(this.getDatasetName());
        sdp.setLayout(this.getLayout());
        sdp.setName(this.getName());
        sdp.setSaltLib(jobNameNoSpace + "module");
        logBasic("{----DATAPROFILING----}");
        logBasic("{Dataset Job} Execute = " + sdp.ecl());
        
        logBasic("{Dataset Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, sdp.ecl());
        
        
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
            logBasic("{Dataset Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        result.setLogText("ECLDataset executed, ECL code added");
        
        return result;
    }
 
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "datasetName")) != null)
            	setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "datasetName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layout")) != null)
            	setLayout(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layout")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<datasetName><![CDATA[" + datasetName + "]]></datasetName>" + Const.CR;
        retval += "		<layout><![CDATA[" + layout + "]]></layout>" + Const.CR;
        
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "layout") != null)
                layout = rep.getStepAttributeString(id_jobentry, "layout"); //$NON-NLS-1$
        
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "layout", layout); //$NON-NLS-1$
            
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
