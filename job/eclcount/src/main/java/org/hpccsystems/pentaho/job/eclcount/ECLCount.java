/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclcount;

import java.util.ArrayList;

import java.util.List;
import org.hpccsystems.javaecl.Count;
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
public class ECLCount extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {

	
	private String recordsetName = "";
	private String recordset = "";
	private String expression = "";
	private String keyed = "";
	private String valuelist = "";
	
	public String getRecordsetName() {
		return recordsetName;
	}
	
	public void setRecordsetName(String recordsetName) {
		this.recordsetName = recordsetName;
	}
	
	public String getRecordSet() {
		return recordset;
	}
	
	public void setRecordSet(String recordset) {
		this.recordset = recordset;
	}
	
	public String getExpression() {
		return expression;
	}
	
	public void setExpression(String expression) {
		this.expression = expression;
	}
	
	public String getKeyed() {
		return keyed;
	}
	
	public void setKeyed(String keyed) {
		this.keyed = keyed;
	}
	
	public String getValueList() {
		return valuelist;
	}
	
	public void setValueList(String valuelist) {
		this.valuelist = valuelist;
	}
	
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
		Result result = prevResult;
		
		logBasic("{Count job} Creating Count object");
		
		Count count = new Count();
		
		count.setName(this.getRecordsetName());
		count.setRecordSet(this.getRecordSet());
		count.setExpression(this.getExpression());
		count.setKeyed(this.getKeyed());
		count.setValueList(this.getValueList());
		
		logBasic("{Count job} Execute = " + count.ecl());
		
		logBasic("{Count job} Previous = " + result.getLogText());
		
		result.setResult(true);
		
		RowMetaAndData data = new RowMetaAndData();
		data.addValue("ecl", Value.VALUE_TYPE_STRING, count.ecl());
		
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
			logBasic("{Count job} ECL Code = " + eclCode);
		}
		*/
		result.setRows(list);
		
		return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")) != null)
            	setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset")) != null)
            	setRecordSet(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "expression")) != null)
            	setExpression(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "expression")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "keyed")) != null)
            	setKeyed(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "keyed")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "valuelist")) != null)
            	setValueList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "valuelist")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Count Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + this.recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<recordset><![CDATA[" + this.recordset + "]]></recordset>" + Const.CR;
        retval += "		<expression><![CDATA[" + this.expression + "]]></expression>" + Const.CR;
        retval += "		<keyed><![CDATA[" + this.keyed + "]]></keyed>" + Const.CR;
        retval += "		<valuelist><![CDATA[" + this.valuelist + "]]></valuelist>" + Const.CR;
  
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
        		this.recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "recordset") != null)
        		this.recordset = rep.getStepAttributeString(id_jobentry, "recordset"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "expression") != null)
        		this.expression = rep.getStepAttributeString(id_jobentry, "expression"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "keyed") != null)
        		this.keyed = rep.getStepAttributeString(id_jobentry, "keyed"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "valuelist") != null)
        		this.valuelist = rep.getStepAttributeString(id_jobentry, "valuelist"); //$NON-NLS-1$
                
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordset", recordset); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "expression", expression); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "keyed", keyed); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "valuelist", valuelist); //$NON-NLS-1$
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
