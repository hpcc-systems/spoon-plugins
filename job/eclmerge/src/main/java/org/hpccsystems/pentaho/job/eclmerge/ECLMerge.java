package org.hpccsystems.pentaho.job.eclmerge;

import java.util.ArrayList;
import java.util.List;

import org.hpccsystems.javaecl.Merge;
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

public class ECLMerge extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {

	private String result; // Output Recordset
	private String recordsetName; // Output Recordset Name
	private String recordsetList;
	private String recordsetSet;
	private String fieldList;
	private Boolean dedup = false;
	private Boolean runLocal = false;
	
	public String getResult() {
		return result;
	}
	
	public void setResult(String result) {
		this.result = result;
	}
	
	public String getRecordsetName() {
		return recordsetName;
	}
	
	public void setRecordsetName(String recordsetName) {
		this.recordsetName = recordsetName;
	}
	
	public String getRecordsetList() {
		return recordsetList;
	}
	
	public void setRecordsetList(String recordsetList) {
		this.recordsetList = recordsetList;
	}
	
	public String getRecordsetSet() {
		return recordsetSet;
	}
	
	public void setRecordsetSet(String recordsetSet) {
		this.recordsetSet = recordsetSet;
	}
	
	public String getFieldList() {
		return fieldList;
	}
	
	public void setFieldList(String fieldList) {
		this.fieldList = fieldList;
	}
	
	public boolean isDedup() {
		return dedup;
	}
	
	public void setDedup(boolean dedup) {
		this.dedup = dedup;
	}
	
	public String isDedupString() {
		if (dedup)
			return "true";
		return "false";
	}
	
	public void setDedupString(String dedup) {
		if (dedup.equals("true")) {
			this.dedup = true;
		} else {
			this.dedup = false;
		}
	}
	
	public boolean runLocal() {
		return runLocal;
	}
	
	public void setRunLocal(boolean runLocal) {
		this.runLocal = runLocal;
	}
	
	public String runLocalString() {
		if (runLocal)
			return "true";
		return "false";
	}
	
	public void setRunLocalString(String runLocal) {
		if (runLocal.equals("true")) {
			this.runLocal = true;
		} else {
			this.runLocal = false;
		}
	}
	
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
		Result result = modifyResults(prevResult);
		if(result.isStopped()){
        	return result;
 		}
		Merge merge = new Merge();
		
		merge.setResult(this.getRecordsetName());
		merge.setRecordsetList(this.getRecordsetList());
		merge.setRecordsetSet(this.getRecordsetSet());
		merge.setFieldList(this.getFieldList());
		merge.setDedup(this.isDedup());
		merge.setRunLocal(this.runLocal());
		
		logBasic("{Merge job} Execute = " + merge.ecl());
		
		logBasic("{Merge job} Previous = " + result.getLogText());
		
		result.setResult(true);
		
		RowMetaAndData data = new RowMetaAndData();
		data.addValue("ecl", Value.VALUE_TYPE_STRING, merge.ecl());
		
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
				if (code != null)
					eclCode += code;
			}
			logBasic("{Merge job} ECL Code = " + eclCode);
		}
		*/
		result.setRows(list);
		
		return result;
    }
	
	@Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")));
            this.setRecordsetList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordsetList")));
            this.setRecordsetSet(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordsetSet")));
            this.setFieldList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fieldList")));
            this.setDedupString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dedup")));
            this.setRunLocalString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "runLocal")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Merge Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + this.recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<recordsetList><![CDATA[" + this.recordsetList + "]]></recordsetList>" + Const.CR;
        retval += "		<recordsetSet><![CDATA[" + this.recordsetSet + "]]></recordsetSet>" + Const.CR;
        retval += "		<fieldList><![CDATA[" + this.fieldList + "]]></fieldList>" + Const.CR;
        retval += "		<dedup><![CDATA[" + this.dedup + "]]></dedup>" + Const.CR;
        retval += "		<runLocal><![CDATA[" + this.runLocal + "]]></runLocal>" + Const.CR;
  
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	this.recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name"); //$NON-NLS-1$
            this.recordsetList = rep.getStepAttributeString(id_jobentry, "recordsetList"); //$NON-NLS-1$
            this.recordsetSet = rep.getStepAttributeString(id_jobentry, "recordsetSet"); //$NON-NLS-1$
            this.fieldList = rep.getStepAttributeString(id_jobentry, "fieldList"); //$NON-NLS-1$
            this.setDedupString(rep.getStepAttributeString(id_jobentry, "dedup")); //$NON-NLS-1$
            this.setRunLocalString(rep.getStepAttributeString(id_jobentry, "runLocal")); //$NON-NLS-1$
                
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetList", recordsetList); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetSet", recordsetSet); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "fieldList", fieldList); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "dedup", dedup); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "runLocal", runLocal); //$NON-NLS-1$
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

  
}
