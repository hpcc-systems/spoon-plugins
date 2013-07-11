package org.hpccsystems.pentaho.job.ecllimit;

import java.util.ArrayList;
import java.util.List;

import org.hpccsystems.javaecl.Limit;
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

public class ECLLimit extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	private String result;
	private String recordsetName;
	private String recordset;
	private String maxRecs;
	private String failClause;
	private Boolean keyed = false;
	private Boolean count = false;
	private Boolean skip = false;
	private String onFailTransform;
	
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
	
	public String getRecordset() {
		return recordset;
	}
	
	public void setRecordset(String recordset) {
		this.recordset = recordset;
	}
	
	public String getMaxRecs() {
		return maxRecs;
	}
	
	public void setMaxRecs(String maxRecs) {
		this.maxRecs = maxRecs;
	}
	
	public String getFailClause() {
		return failClause;
	}
	
	public void setFailClause(String failClause) {
		this.failClause = failClause;
	}
	
	public boolean getKeyed() {
		return keyed;
	}
	
	public void setKeyed(boolean keyed) {
		this.keyed = keyed;
	}
	
	public String getKeyedString() {
		if (this.keyed) {
			return "true";
		} else {
			return "false";
		}
	}
	
	public void setKeyedString(String keyed) {
		if (keyed.equals("true")) {
			this.keyed = true;
		} else {
			this.keyed = false;
		}
	}
	
	public boolean getCount() {
		return count;
	}
	
	public void setCount(boolean count) {
		this.count = count;
	}
	
	public String getCountString() {
		if (this.count) {
			return "true";
		} else {
			return "false";
		}
	}
	
	public void setCountString(String count) {
		if (count.equals("true")) {
			this.count = true;
		} else {
			this.count = false;
		}
	}
	
	public boolean getSkip() {
		return skip;
	}
	
	public void setSkip(boolean skip) {
		this.skip = skip;
	}
	
	public String getSkipString() {
		if (this.skip) {
			return "true";
		} else {
			return "false";
		}
	}
	
	public void setSkipString(String skip) {
		if (skip.equals("true")) {
			this.skip = true;
		} else {
			this.skip = false;
		}
	}
	
	public String getOnFailTransform() {
		return onFailTransform;
	}
	
	public void setOnFailTransform(String onFailTransform) {
		this.onFailTransform = onFailTransform;
	}
	
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
		Result result = prevResult;
		
		Limit limit = new Limit();
		
		limit.setResult(this.getRecordsetName());
		limit.setRecordset(this.getRecordset());
		limit.setMaxRecs(this.getMaxRecs());
		limit.setFailClause(this.getFailClause());
		limit.setKeyed(this.getKeyed());
		limit.setCount(this.getCount());
		limit.setSkip(this.getSkip());
		limit.setOnFailTransform(this.getOnFailTransform());
		
		logBasic("{Limit job} Execute = " + limit.ecl());
		
		logBasic("{Limit job} Previous = " + result.getLogText());
		
		result.setResult(true);
		
		RowMetaAndData data = new RowMetaAndData();
		data.addValue("ecl", Value.VALUE_TYPE_STRING, limit.ecl());
		
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
			logBasic("{Limit job} ECL Code = " + eclCode);
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
            this.setRecordset(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset")));
            this.setMaxRecs(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "maxRecs")));
            this.setFailClause(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "failClause")));
            this.setKeyedString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "keyed")));
            this.setCountString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "count")));
            this.setSkipString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "skip")));
            this.setOnFailTransform(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "onFailTransform")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Limit Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + this.recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<recordset><![CDATA[" + this.recordset + "]]></recordset>" + Const.CR;
        retval += "		<maxRecs><![CDATA[" + this.maxRecs + "]]></maxRecs>" + Const.CR;
        retval += "		<failClause><![CDATA[" + this.failClause + "]]></failClause>" + Const.CR;
        retval += "		<keyed><![CDATA[" + this.keyed + "]]></keyed>" + Const.CR;
        retval += "		<count><![CDATA[" + this.count + "]]></count>" + Const.CR;
        retval += "		<skip><![CDATA[" + this.skip + "]]></skip>" + Const.CR;
        retval += "		<onFailTransform><![CDATA[" + this.onFailTransform + "]]></onFailTransform>" + Const.CR;
  
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	this.recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            this.recordset = rep.getStepAttributeString(id_jobentry, "recordset"); //$NON-NLS-1$
            this.maxRecs = rep.getStepAttributeString(id_jobentry, "maxRecs"); //$NON-NLS-1$
            this.failClause = rep.getStepAttributeString(id_jobentry, "failClause"); //$NON-NLS-1$
            this.setKeyedString(rep.getStepAttributeString(id_jobentry, "keyed")); //$NON-NLS-1$
            this.setCountString(rep.getStepAttributeString(id_jobentry, "count")); //$NON-NLS-1$
            this.setSkipString(rep.getStepAttributeString(id_jobentry, "skip")); //$NON-NLS-1$
            this.onFailTransform = rep.getStepAttributeString(id_jobentry, "onFailTransform"); //$NON-NLS-1$
                
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "maxRecs", maxRecs); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "failClause", failClause); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "keyed", keyed); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "count", count); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "skip", skip); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "onFailTransform", onFailTransform); //$NON-NLS-1$
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
