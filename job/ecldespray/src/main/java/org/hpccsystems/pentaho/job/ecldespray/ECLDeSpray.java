/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecldespray;

import java.util.ArrayList;
import java.util.List;

import org.hpccsystems.javaecl.DeSpray;
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
public class ECLDeSpray extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {

	
	private String logicalName;
	private String destinationIP;
	private String destinationPath;
	private String timeout;
	private String espserverISPport;
	private String maxConnections;
	private Boolean allowOverwrite = false;
	
	
	
	public String getLogicalName() {
		return logicalName;
	}
	
	public void setLogicalName(String logicalName) {
		this.logicalName = logicalName;
	}
	
	public String getDestinationIP() {
		return destinationIP;
	}
	
	public void setDestinationIP(String destinationIP) {
		this.destinationIP = destinationIP;
	}
	
	public String getDestinationPath() {
		return destinationPath;
	}
	
	public void setDestinationPath(String destinationPath) {
		this.destinationPath = destinationPath;
	}
	
	public String getTimeout() {
		return timeout;
	}
	
	public void setTimeout(String timeout) {
		this.timeout = timeout;
	}
	
	public String getEspserverISPport() {
		return espserverISPport;
	}
	
	public void setEspserverISPport(String espserverISPport) {
		this.espserverISPport = espserverISPport;
	}
	
	public String getMaxConnections() {
		return maxConnections;
	}
	
	public void setMaxConnections(String maxConnections) {
		this.maxConnections = maxConnections;
	}
	
	public Boolean isAllowOverwrite() {
		return allowOverwrite;
	}
	
	public void setAllowOverwrite(boolean allowOverwrite) {
		this.allowOverwrite = allowOverwrite;
	}
	
	public String isAllowOverwriteString() {
		if (this.allowOverwrite)
			return "true";
		return "false";
	}
	
	public void setAllowOverwriteString(String allowOverwrite) {
		if(allowOverwrite.equals("true"))
			this.allowOverwrite = true;
		else
			this.allowOverwrite = false;
	}
	
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
		Result result = prevResult;
		
		logBasic("{DeSpray job} Creating Count object");
		
		DeSpray dspray = new DeSpray();
		
		dspray.setName(this.getName());
		dspray.setLogicalName(this.getLogicalName());
		dspray.setDestinationIP(this.getDestinationIP());
		dspray.setDestinationPath(this.getDestinationPath());
		dspray.setTimeout(this.getTimeout());
		dspray.setEspserverISPport(this.getEspserverISPport());
		dspray.setMaxConnections(this.getMaxConnections());
		dspray.setAllowOverwrite(this.isAllowOverwrite());
		
		logBasic("{DeSpray job} Execute = " + dspray.ecl());
		
		logBasic("{DeSpray job} Previous = " + result.getLogText());
		
		result.setResult(true);
		
		RowMetaAndData data = new RowMetaAndData();
		data.addValue("ecl", Value.VALUE_TYPE_STRING, dspray.ecl());
		
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
			logBasic("{DeSpray job} ECL Code = " + eclCode);
		}
		*/
		result.setRows(list);
		
		return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            this.setLogicalName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "logicalName")));
            this.setDestinationIP(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "destinationIp")));
            this.setDestinationPath(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "destinationPath")));
            this.setTimeout(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "timeout")));
            this.setEspserverISPport(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "espserverISPport")));
            this.setMaxConnections(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "maxConnections")));
            this.setAllowOverwriteString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "allowOverwrite")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL DeSpray Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        
        retval += "		<logicalName><![CDATA[" + this.logicalName + "]]></logicalName>" + Const.CR;
        retval += "		<destinationIp><![CDATA[" + this.destinationIP + "]]></destinationIp>" + Const.CR;
        retval += "		<destinationPath><![CDATA[" + this.destinationPath + "]]></destinationPath>" + Const.CR;
        retval += "		<timeout><![CDATA[" + this.timeout + "]]></timeout>" + Const.CR;
        retval += "		<espserverISPport><![CDATA[" + this.espserverISPport + "]]></espserverISPport>" + Const.CR;
        retval += "		<maxConnections><![CDATA[" + this.maxConnections + "]]></maxConnections>" + Const.CR;
        retval += "		<allowOverwrite><![CDATA[" + this.allowOverwrite + "]]></allowOverwrite>" + Const.CR;
  
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            this.logicalName = rep.getStepAttributeString(id_jobentry, "logicalName"); //$NON-NLS-1$
            this.destinationIP = rep.getStepAttributeString(id_jobentry, "destinationIP"); //$NON-NLS-1$
            this.destinationPath = rep.getStepAttributeString(id_jobentry, "destinationPath"); //$NON-NLS-1$
            this.timeout = rep.getStepAttributeString(id_jobentry, "timeout"); //$NON-NLS-1$
            this.espserverISPport = rep.getStepAttributeString(id_jobentry, "espserverISPport"); //$NON-NLS-1$
            this.maxConnections = rep.getStepAttributeString(id_jobentry, "maxConnections"); //$NON-NLS-1$
            this.setAllowOverwriteString(rep.getStepAttributeString(id_jobentry, "allowOverwrite")); //$NON-NLS-1$
                
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "logicalName", logicalName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "destinationIP", destinationIP); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "destinationPath", destinationPath); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "timeout", timeout); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "espserverISPport", espserverISPport); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "maxConnections", maxConnections); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "allowOverwrite", this.isAllowOverwriteString()); //$NON-NLS-1$
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
