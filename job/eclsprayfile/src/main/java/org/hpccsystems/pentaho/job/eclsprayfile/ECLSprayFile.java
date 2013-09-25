/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclsprayfile;

import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Spray;
import org.hpccsystems.javaecl.EclDirect;
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

import org.pentaho.di.job.JobMeta;
import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author ChalaAX
 */
public class ECLSprayFile extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String outputField = "";
    //private String ipAddress = "";
    private String filePath = "";
    private String logicalFileName = "";
    private String fileType = "";
    private String csvSeparator = "";
    private String csvTerminator = "";
    private String csvQuote = "";
    private String fixedRecordSize = "";
    private String allowOverwrite = "True";
    private String groupName = "";
    
    private String landingZoneIP = "";
    
    private boolean isValid = true;
    
    //private RecordList recordList = new RecordList();

   /* public RecordList getRecordList() {
        return recordList;
    }

    public void setRecordList(RecordList recordList) {
        this.recordList = recordList;
    }*/
    
    

    
    
    public String getOutputField() {
        return outputField;
    }

    public String getLandingZoneIP() {
		return landingZoneIP;
	}

	public void setLandingZoneIP(String landingZoneIP) {
		this.landingZoneIP = landingZoneIP;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getAllowOverwrite() {
		return allowOverwrite;
	}

	public void setAllowOverwrite(String allowOverwrite) {
		this.allowOverwrite = allowOverwrite;
	}

	public void setOutputField(String outputField) {
        this.outputField = outputField;
    }

    public String getCsvQuote() {
        return csvQuote;
    }

    public void setCsvQuote(String csvQuote) {
        this.csvQuote = csvQuote;
    }

    public String getCsvSeparator() {
        return csvSeparator;
    }

    public void setCsvSeparator(String csvSeparator) {
        this.csvSeparator = csvSeparator;
    }

    public String getCsvTerminator() {
        return csvTerminator;
    }

    public void setCsvTerminator(String csvTerminator) {
        this.csvTerminator = csvTerminator;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getLogicalFileName() {
        return logicalFileName;
    }

    public void setLogicalFileName(String logicalFileName) {
        this.logicalFileName = logicalFileName;
    }
    
    public String getFixedRecordSize() {
        return fixedRecordSize;
    }

    public void setFixedRecordSize(String fixedRecordSize) {
        this.fixedRecordSize = fixedRecordSize;
    }



    @Override
    public Result execute(Result prevResult, int i) throws KettleException {
    	Result result = modifyResults(prevResult);
        if(result.isStopped()){
        	return result;
 		}
        JobMeta jobMeta = super.parentJob.getJobMeta();
        String serverHost = "";
        String serverPort = "";
        String landingZone = "";
        
        String cluster = "";
        String jobName = "";
        String eclccInstallDir = "";
        String mlPath = "";
        String includeML = "";
        String user = "";
        String pass = "";
        
        AutoPopulate ap = new AutoPopulate();
            try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
                
                serverHost = ap.getGlobalVariable(jobMeta.getJobCopies(),"server_ip");
                serverPort = ap.getGlobalVariable(jobMeta.getJobCopies(),"server_port");
                landingZone = ap.getGlobalVariable(jobMeta.getJobCopies(),"landing_zone");
                
                cluster = ap.getGlobalVariable(jobMeta.getJobCopies(),"cluster");
                jobName = ap.getGlobalVariable(jobMeta.getJobCopies(),"jobName");
                
                eclccInstallDir = ap.getGlobalVariable(jobMeta.getJobCopies(),"eclccInstallDir");
                mlPath = ap.getGlobalVariable(jobMeta.getJobCopies(),"mlPath");
                includeML = ap.getGlobalVariable(jobMeta.getJobCopies(),"includeML");
                
                user = ap.getGlobalVariable(jobMeta.getJobCopies(),"user_name");
                pass = ap.getGlobalVariableEncrypted(jobMeta.getJobCopies(),"password");
                
            }catch (Exception e){
                System.out.println("Error Parsing existing Global Variables ");
                System.out.println(e.toString());
                
            }
            

        
        
            
        
        Spray spray = new Spray();
        spray.setClusterName("mythor");
        spray.setFilePath(landingZone + getFilePath());
        spray.setIpAddress(serverHost);
        spray.setLogicalFileName(getLogicalFileName());
        spray.setFileType(getFileType());
        spray.setCsvQuote(getCsvQuote());
        spray.setCsvSeparator(getCsvSeparator());
        spray.setCsvTerminator(getCsvTerminator());
        spray.setRecordSize(getFixedRecordSize());
        spray.setAllowOverWrite(getAllowOverwrite());
        spray.setGroupName(getGroupName());
        spray.setLandingZoneOverrideIP(getLandingZoneIP());
         
        //logBasic(spray.ecl());
        logBasic("{Spray Job} Execute = " + spray.ecl());
        
        
        result.setLogText(spray.ecl());//Will do for now. This is to pass the information forward
        result.setResult(true);

        logBasic("{Spray Job} Called IP = " + (String)spray.getIpAddress());
        
        EclDirect eclDirect = new EclDirect(((String)spray.getIpAddress()).trim(), cluster, serverPort);
        eclDirect.setEclccInstallDir(eclccInstallDir);
        eclDirect.setIncludeML(includeML);
        eclDirect.setJobName(jobName);
        eclDirect.setMlPath(mlPath);
        eclDirect.setOutputName(this.getName());
        eclDirect.setUserName(user);
        eclDirect.setPassword(pass);
        ArrayList dsList = eclDirect.execute(spray.ecl());
       
        isValid = eclDirect.isValid();
        if(!isValid){
        	System.out.println("Not Valid Spray");
        	logError("Failed to execute spray on the cluster, please verify your settings");
        	result.setResult(false);
        	result.setStopped(true);
        }
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, spray.ecl());
        
        
        
        List list = result.getRows();
        if (list == null) {
            list = new ArrayList();
        }
        //list.add(data);
        result.setRows(list);
        
        return result;
    }
  /*
    public String saveRecordList(){
        String out = "";
        ArrayList list = recordList.getRecords();
        Iterator<RecordBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
  
    public void openRecordList(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            recordList = new RecordList();
            System.out.println("Open Record List");
            for(int i =0; i<len; i++){
                System.out.println("++++++++++++" + strLine[i]);
                //this.recordDef.addRecord(new RecordBO(strLine[i]));
                RecordBO rb = new RecordBO(strLine[i]);
                System.out.println(rb.getColumnName());
                recordList.addRecordBO(rb);
            }
        }
    }
     * */

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outputfield")) != null)
                setOutputField(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outputfield")));
           // if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "ip_address")) != null)
               // setIpAddress(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "ip_address")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_path")) != null)
                setFilePath(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_path")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "logical_file_name")) != null)
                setLogicalFileName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "logical_file_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_type")) != null)
                setFileType(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "file_type")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "csv_separator")) != null)
                setCsvSeparator(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "csv_separator")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "csv_terminator")) != null)
                setCsvTerminator(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "csv_terminator")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "csv_quote")) != null)
                setCsvQuote(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "csv_quote")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fixed_record_size")) != null)
                setFixedRecordSize(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fixed_record_size")));   
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "allowOverwrite")) != null)
            	setAllowOverwrite(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "allowOverwrite")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "groupName")) != null)
            	setGroupName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "groupName")));
           
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "landingZoneIP")) != null)
            	setLandingZoneIP(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "landingZoneIP")));

          //  if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
           //     openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Spray Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<outputfield><![CDATA[" + outputField + "]]></outputfield>" + Const.CR;
        //retval += "		<ip_address>" + ipAddress + "</ip_address>" + Const.CR;
        retval += "		<file_path><![CDATA[" + filePath + "]]></file_path>" + Const.CR;
        retval += "		<file_type><![CDATA[" + fileType + "]]></file_type>" + Const.CR;
        retval += "		<csv_separator><![CDATA[" + csvSeparator + "]]></csv_separator>" + Const.CR;
        retval += "		<csv_terminator><![CDATA[" + csvTerminator + "]]></csv_terminator>" + Const.CR;
        retval += "		<csv_quote><![CDATA[" + csvQuote + "]]></csv_quote>" + Const.CR;
        retval += "		<csv_quote><![CDATA[" + csvQuote + "]]></csv_quote>" + Const.CR;
        retval += "		<fixed_record_size><![CDATA[" + fixedRecordSize + "]]></fixed_record_size>" + Const.CR;
        retval += "		<logical_file_name><![CDATA[" + logicalFileName + "]]></logical_file_name>" + Const.CR;
        retval += "		<allowOverwrite><![CDATA[" + allowOverwrite + "]]></allowOverwrite>" + Const.CR;
        retval += "		<groupName><![CDATA[" + groupName + "]]></groupName>" + Const.CR;
        retval += "		<landingZoneIP><![CDATA[" + landingZoneIP + "]]></landingZoneIP>" + Const.CR;
       // retval += "		<recordList>" + this.saveRecordList() + "</recordList>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "otuputField") != null)
                outputField = rep.getStepAttributeString(id_jobentry, "otuputField"); //$NON-NLS-1$
           // if(rep.getStepAttributeString(id_jobentry, "ipAddress") != null)
               // ipAddress = rep.getStepAttributeString(id_jobentry, "ipAddress"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "filePath") != null)
                filePath = rep.getStepAttributeString(id_jobentry, "filePath"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "fileType") != null)
                fileType = rep.getStepAttributeString(id_jobentry, "fileType"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "csvSeparator") != null)
                csvSeparator = rep.getStepAttributeString(id_jobentry, "csvSeparator"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "csvTerminator") != null)
                csvTerminator = rep.getStepAttributeString(id_jobentry, "csvTerminator"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "csvQuote") != null)
                csvQuote = rep.getStepAttributeString(id_jobentry, "csvQuote"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "fixedRecordSize") != null)
                fixedRecordSize = rep.getStepAttributeString(id_jobentry, "fixedRecordSize"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "logicalFileName") != null)
                logicalFileName = rep.getStepAttributeString(id_jobentry, "logicalFileName"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "allowOverwrite") != null)
            	allowOverwrite = rep.getStepAttributeString(id_jobentry, "allowOverwrite"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "groupName") != null)
            	groupName = rep.getStepAttributeString(id_jobentry, "groupName"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "landingZoneIP") != null)
            	landingZoneIP = rep.getStepAttributeString(id_jobentry, "landingZoneIP"); //$NON-NLS-1$
          //  if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
          //      this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "outputField", outputField); //$NON-NLS-1$
           // rep.saveStepAttribute(id_job, getObjectId(), "ipAddress", ipAddress); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "filePath", filePath); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "fileType", fileType); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "csvSeparator", csvSeparator); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "csvTerminator", csvTerminator); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "csvQuote", csvQuote); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "fixedRecordSize", fixedRecordSize); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "logicalFileName", logicalFileName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "allowOverwrite", allowOverwrite); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "groupName", groupName); //$NON-NLS-1$
           
            rep.saveStepAttribute(id_job, getObjectId(), "landingZoneIP",landingZoneIP);
          //  rep.saveStepAttribute(id_job, getObjectId(), "recordList", this.saveRecordList()); //$NON-NLS-1$
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    
    public boolean evaluates() {
        //return isValid;
    	//boolean isValid = true;
    	if(validMap.containsKey(this.getName())){
    		isValid = validMap.get(this.getName());
    	}
    	return isValid;
    }

    
}
