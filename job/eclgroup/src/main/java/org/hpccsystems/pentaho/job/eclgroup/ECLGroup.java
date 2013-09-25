/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclgroup;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.Group;
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
 * @author SimmonsJA
 */
public class ECLGroup extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	
	private String recordsetName;
	private String recordset;
	private String breakCriteria;
	private Boolean isAll = false;
	private Boolean runLocal = false;
	
	
	
	
	public String getRecordSetName() {
		return recordsetName;
	}
	
	public void setRecordSetName(String recordsetName) {
		this.recordsetName = recordsetName;
	}
	
	public String getRecordSet() {
		return recordset;
	}
	
	public void setRecordSet(String recordset) {
		this.recordset = recordset;
	}
	
	public String getBreakCriteria() {
		return breakCriteria;
	}
	
	public void setBreakCriteria(String breakCriteria) {
		this.breakCriteria = breakCriteria;
	}
	
	public boolean getIsAll() {
		return isAll;
	}
	
	public String getIsAllString() {
		if (this.isAll)
			return "true";
		return "false";
	}
	
	public void setIsAll(boolean isAll) {
		this.isAll = isAll;
	}
	
	public void setIsAllString(String isAll) {
		if (isAll.equals("true"))
			this.isAll = true;
		else
			this.isAll = false;
	}
	
	public boolean getIsRunLocal() {
		return runLocal;
	}
	
	public String getIsRunLocalString() {
		if (this.runLocal)
			return "true";
		return "false";
	}
	
	public void setRunLocal(boolean runLocal) {
		this.runLocal = runLocal;
	}
	
	public void setRunLocalString(String runLocal) {
		if (runLocal.equals("true"))
			this.runLocal = true;
		else
			this.runLocal = false;
	}
	
	
	@Override
	public Result execute(Result prevResult, int k) throws KettleException {
		
		Result result = modifyResults(prevResult);
		if(result.isStopped()){
        	return result;
 		}
		logBasic("{Group job} Creating Group object");
		
		Group group = new Group();
		
		logBasic("{Group job} Group object created");
		
		group.setName(this.getRecordSetName());
		group.setRecordSet(this.getRecordSet());
		group.setBreakCriteria(this.getBreakCriteria());
		group.setIsAll(this.getIsAll());
		group.setRunLocal(this.getIsRunLocal());
		
		logBasic("{Group job} Execute = " + group.ecl());
		
		logBasic("{Group job} Previous = " + result.getLogText());
		
		result.setResult(true);
		
		RowMetaAndData data = new RowMetaAndData();
		data.addValue("ecl", Value.VALUE_TYPE_STRING, group.ecl());
		
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
			logBasic("{Group job} ECL Code = " + eclCode);
		}
		*/
		result.setRows(list);
		
		return result;
	}
	
	@Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);

            this.setRecordSetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
            this.setRecordSet(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset")));
            this.setBreakCriteria(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"breakCriteria")));
            this.setIsAllString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"isAll")));
            this.setRunLocalString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Group Job Plugin is unable to read step info from XML node", e);
        }

    }
	
	public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "             <recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
        retval += "             <recordset><![CDATA["+this.recordset+"]]></recordset>"+Const.CR;
        retval += "             <breakCriteria><![CDATA["+this.breakCriteria+"]]></breakCriteria>"+Const.CR;
        retval += "             <isAll><![CDATA["+this.getIsAllString()+"]]></isAll>"+Const.CR;
        retval += "             <runLocal><![CDATA["+this.getIsRunLocalString()+"]]></runLocal>"+Const.CR;

        return retval;

    }
	
	public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {

            recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name");
            recordset = rep.getStepAttributeString(id_jobentry, "recordset");
            breakCriteria = rep.getStepAttributeString(id_jobentry, "breakCriteria");
            this.setIsAllString(rep.getStepAttributeString(id_jobentry, "isAll"));
            this.setRunLocalString(rep.getStepAttributeString(id_jobentry, "runLocal"));
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }
	
	public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset);
            rep.saveStepAttribute(id_job, getObjectId(), "breakCriteria", breakCriteria);
            rep.saveStepAttribute(id_job, getObjectId(), "isAll", this.getIsAllString());
            rep.saveStepAttribute(id_job, getObjectId(), "runLocal", this.getIsRunLocalString());
        
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

	
}