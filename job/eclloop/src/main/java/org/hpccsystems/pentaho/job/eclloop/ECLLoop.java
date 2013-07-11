package org.hpccsystems.pentaho.job.eclloop;

import java.util.ArrayList;
import java.util.List;

import org.hpccsystems.javaecl.Loop;
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

public class ECLLoop extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {

	private String result;
	private String recordsetName;
	private String recordset;
	private String loopCount;
	private String loopBody;
	private String iterations;
	private String iterationList;
	private String dfault;
	private String loopFilter;
	private String loopCondition;
	private String rowFilter;
	
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
	
	public String getLoopCount() {
		return loopCount;
	}
	
	public void setLoopCount(String loopCount) {
		this.loopCount = loopCount;
	}
	
	public String getLoopBody() {
		return loopBody;
	}
	
	public void setLoopBody(String loopBody) {
		this.loopBody = loopBody;
	}
	
	public String getIterations() {
		return iterations;
	}
	
	public void setIterations(String iterations) {
		this.iterations = iterations;
	}
	
	public String getIterationList() {
		return iterationList;
	}
	
	public void setIterationList(String iterationList) {
		this.iterationList = iterationList;
	}
	
	public String getDefault() {
		return dfault;
	}
	
	public void setDefault(String dfault) {
		this.dfault = dfault;
	}
	
	public String getLoopFilter() {
		return loopFilter;
	}
	
	public void setLoopFilter(String loopFilter) {
		this.loopFilter = loopFilter;
	}
	
	public String getLoopCondition() {
		return loopCondition;
	}
	
	public void setLoopCondition(String loopCondition) {
		this.loopCondition = loopCondition;
	}
	
	public String getRowFilter() {
		return rowFilter;
	}
	
	public void setRowFilter(String rowFilter) {
		this.rowFilter = rowFilter;
	}
	
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
		Result result = prevResult;
		
		Loop loop = new Loop();
		
		loop.setResult(this.getRecordsetName());
		loop.setRecordset(this.getRecordset());
		loop.setLoopCount(this.getLoopCount());
		loop.setLoopBody(this.getLoopBody());
		loop.setIterations(this.getIterations());
		loop.setIterationList(this.getIterationList());
		loop.setDefault(this.getDefault());
		loop.setLoopFilter(this.getLoopFilter());
		loop.setLoopCondition(this.getLoopCondition());
		loop.setRowFilter(this.getRowFilter());
		
		logBasic("{Loop job} Execute = " + loop.ecl());
		
		logBasic("{Loop job} Previous = " + result.getLogText());
		
		result.setResult(true);
		
		RowMetaAndData data = new RowMetaAndData();
		data.addValue("ecl", Value.VALUE_TYPE_STRING, loop.ecl());
		
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
			logBasic("{Loop job} ECL Code = " + eclCode);
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
            this.setLoopCount(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "loopCount")));
            this.setLoopBody(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "loopBody")));
            this.setIterations(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "iterations")));
            this.setIterationList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "iterationList")));
            this.setDefault(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "default")));
            this.setLoopFilter(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "loopFilter")));
            this.setLoopCondition(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "loopCondition")));
            this.setRowFilter(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rowFilter")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Loop Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += " 	<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + this.recordsetName + "]]></recordset_name>";
        retval += "		<recordset><![CDATA[" + this.recordset + "]]></recordset>" + Const.CR;
        retval += "		<loopCount><![CDATA[" + this.loopCount + "]]></loopCount>" + Const.CR;
        retval += "		<loopBody><![CDATA[" + this.loopBody + "]]></loopBody>" + Const.CR;
        retval += "		<iterations><![CDATA[" + this.iterations + "]]></iterations>" + Const.CR;
        retval += "		<iterationList><![CDATA[" + this.iterationList + "]]></iterationList>" + Const.CR;
        retval += "		<default><![CDATA[" + this.dfault + "]]></default>" + Const.CR;
        retval += "		<loopFilter><![CDATA[" + this.loopFilter + "]]></loopFilter>" + Const.CR;
        retval += "		<loopCondition><![CDATA[" + this.loopCondition + "]]></loopCondition>" + Const.CR;
        retval += "		<rowFilter><![CDATA[" + this.rowFilter + "]]></rowFilter>" + Const.CR;
  
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	this.recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name"); //$NON-NLS-1$
            this.recordset = rep.getStepAttributeString(id_jobentry, "recordset"); //$NON-NLS-1$
            this.loopCount = rep.getStepAttributeString(id_jobentry, "loopCount"); //$NON-NLS-1$
            this.loopBody = rep.getStepAttributeString(id_jobentry, "loopBody"); //$NON-NLS-1$
            this.iterations = rep.getStepAttributeString(id_jobentry, "iterations"); //$NON-NLS-1$
            this.iterationList = rep.getStepAttributeString(id_jobentry, "iterationList"); //$NON-NLS-1$
            this.dfault = rep.getStepAttributeString(id_jobentry, "default"); //$NON-NLS-1$
            this.loopFilter = rep.getStepAttributeString(id_jobentry, "loopFilter"); //$NON-NLS-1$
            this.loopCondition = rep.getStepAttributeString(id_jobentry, "loopCondition"); //$NON-NLS-1$
            this.rowFilter = rep.getStepAttributeString(id_jobentry, "rowFilter"); //$NON-NLS-1$
                
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "loopCount", loopCount); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "loopBody", loopBody); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "iterations", iterations); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "iterationList", iterationList); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "default", dfault); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "loopFilter", loopFilter); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "loopCondition", loopCondition); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "rowFilter", rowFilter); //$NON-NLS-1$
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
