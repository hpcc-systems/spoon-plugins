/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclglobalvariables;


import java.util.List;



import org.pentaho.di.cluster.SlaveServer;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;

import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.ecljobentrybase.*;

import org.pentaho.di.core.encryption.*;

/**
 *
 * @author ChambersJ
 */
public class ECLGlobalVariables extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    //private String name = "";
	private String maxReturn = "";
    private String serverIP = "";
    private String serverPort = "";
    
    private String landingZone = "";
    
    private String mlPath = "";
    private String eclccInstallDir = "";
    private String jobName = "Spoon-job";
    private String cluster = "";
    private boolean includeML = false;
    private String compileFlags = "";
    private String user = "";
    private String pass = "";
    
    private String SALTPath = "";
    private boolean includeSALT = false;

    private String keyStr = "saqwvdf023rkjas7ku,df9e4kt`q234rtuqrtadfads.faufrae";
    
    
    
    public String getCompileFlags() {
		return compileFlags;
	}

	public void setCompileFlags(String compileFlags) {
		this.compileFlags = compileFlags;
	}

	public String getMaxReturn() {
		return maxReturn;
	}

	public void setMaxReturn(String maxReturn) {
		this.maxReturn = maxReturn;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		if(pass.equalsIgnoreCase("")){
			return "";
		}else{
			return Encr.decryptPassword(pass);
		}
	}

	public void setPass(String pass) {
		if(!pass.equalsIgnoreCase("")){
			this.pass = Encr.encryptPassword(pass);
		}
	}

	public String getServerIP() {
        return serverIP;
    }

    public void setServerIP(String serverIP) {
        this.serverIP = serverIP;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public String getLandingZone() {
        return landingZone;
    }

    public void setLandingZone(String landingZone) {
        this.landingZone = landingZone;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getEclccInstallDir() {
        return eclccInstallDir;
    }

    public void setEclccInstallDir(String eclccInstallDir) {
        this.eclccInstallDir = eclccInstallDir;
    }

    public boolean isIncludeML() {
        return includeML;
    }
    public String getIncludeML() {
        if(includeML){
            return "true";
        }else{
            return "false";
        }
    }
    public void setIncludeML(boolean includeML) {
        this.includeML = includeML;
    }
    
    public void setIncludeML(String includeML) {
        if(includeML.equals("true")){
            this.includeML = true;
        }else{
            this.includeML = false;
        }
    }

    
    public String getSALTPath() {
		return SALTPath;
	}

	public void setSALTPath(String sALTPath) {
		SALTPath = sALTPath;
	}

	public boolean isIncludeSALT() {
		return includeSALT;
	}

	public void setIncludeSALT(boolean includeSALT) {
		this.includeSALT = includeSALT;
	}
	public void setIncludeSALT(String includeSALT) {
        if(includeSALT.equals("true")){
            this.includeSALT = true;
        }else{
            this.includeSALT = false;
        }
    }
	
	 public String getIncludeSALT() {
	        if(includeSALT){
	            return "true";
	        }else{
	            return "false";
	        }
	    }


    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getMlPath() {
        return mlPath;
    }

    public void setMlPath(String mlPath) {
        this.mlPath = mlPath;
    }
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        

        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"maxReturn")) != null)
                this.setMaxReturn(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"maxReturn")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"server_ip")) != null)
                this.setServerIP(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"server_ip")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"server_port")) != null)
                this.setServerPort(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"server_port")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"landing_zone")) != null)
                this.setLandingZone(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"landing_zone")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"mlPath")) != null)
                this.setMlPath(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"mlPath")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"eclccInstallDir")) != null)
                this.setEclccInstallDir(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"eclccInstallDir")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"jobName")) != null)
                this.setJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"jobName")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"cluster")) != null)
                this.setCluster(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"cluster")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"includeML")) != null)
                this.setIncludeML(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"includeML")));
            
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"includeSALT")) != null)
                this.setIncludeSALT(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"includeSALT")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"SALTPath")) != null)
                this.setSALTPath(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"SALTPath")));
           
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"user_name")) != null)
                this.setUser(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"user_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"password")) != null)
            	pass = (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"password")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"compileFlags")) != null)
            	compileFlags = (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"compileFlags")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }



    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "             <maxReturn><![CDATA["+this.maxReturn+"]]></maxReturn>"+Const.CR;
        retval += "             <server_ip><![CDATA["+this.serverIP+"]]></server_ip>"+Const.CR;
        
        retval += "             <server_port><![CDATA["+this.serverPort+"]]></server_port>"+Const.CR;
        
        retval += "             <landing_zone><![CDATA["+this.landingZone+"]]></landing_zone>"+Const.CR;
        
        retval += "             <mlPath><![CDATA["+this.getMlPath()+"]]></mlPath>"+Const.CR;
        
        retval += "             <eclccInstallDir><![CDATA["+this.getEclccInstallDir()+"]]></eclccInstallDir>"+Const.CR;
       
        retval += "             <jobName><![CDATA["+this.getJobName()+"]]></jobName>"+Const.CR;
       
        retval += "             <cluster><![CDATA["+this.getCluster()+"]]></cluster>"+Const.CR;
        
        retval += "             <includeML><![CDATA["+this.getIncludeML() + "]]></includeML>"+Const.CR;
        
        retval += "             <user_name><![CDATA["+this.getUser() + "]]></user_name>"+Const.CR;
        
        retval += "             <password><![CDATA["+ pass + "]]></password>"+Const.CR;
        
        retval += "             <includeSALT><![CDATA["+this.getIncludeSALT() + "]]></includeSALT>"+Const.CR;
        retval += "             <SALTPath><![CDATA["+this.getSALTPath()+"]]></SALTPath>"+Const.CR;
        retval += "             <compileFlags><![CDATA["+this.getCompileFlags()+"]]></compileFlags>"+Const.CR;
        
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

            //name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$
        	maxReturn = rep.getStepAttributeString(id_jobentry, "maxReturn");
            serverIP = rep.getStepAttributeString(id_jobentry, "server_ip");
            
            serverPort = rep.getStepAttributeString(id_jobentry, "server_port");
            landingZone = rep.getStepAttributeString(id_jobentry, "landing_zone");
            
            this.setMlPath( rep.getStepAttributeString(id_jobentry, "mlPath"));
            this.setEclccInstallDir( rep.getStepAttributeString(id_jobentry, "eclccInstallDir"));
            this.setJobName( rep.getStepAttributeString(id_jobentry, "jobName"));
            this.setCluster( rep.getStepAttributeString(id_jobentry, "cluster"));
            this.setIncludeML( rep.getStepAttributeString(id_jobentry, "includeML"));
            
            this.setUser( rep.getStepAttributeString(id_jobentry, "user_name"));
            pass = ( rep.getStepAttributeString(id_jobentry, "password"));
            
            this.setIncludeSALT( rep.getStepAttributeString(id_jobentry, "includeSALT"));
            this.setSALTPath( rep.getStepAttributeString(id_jobentry, "SALTPath"));
            this.setCompileFlags( rep.getStepAttributeString(id_jobentry, "compileFlags"));
            
                    
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

        	rep.saveStepAttribute(id_job, getObjectId(), "maxReturn", maxReturn);
            rep.saveStepAttribute(id_job, getObjectId(), "server_ip", serverIP);
            rep.saveStepAttribute(id_job, getObjectId(), "server_port", serverPort);
            rep.saveStepAttribute(id_job, getObjectId(), "landing_zone", landingZone);
            
            rep.saveStepAttribute(id_job, getObjectId(), "mlPath", this.getMlPath());
            rep.saveStepAttribute(id_job, getObjectId(), "eclccInstallDir", this.getEclccInstallDir());
            rep.saveStepAttribute(id_job, getObjectId(), "jobName", this.getJobName());
            rep.saveStepAttribute(id_job, getObjectId(), "cluster", this.getCluster());
            rep.saveStepAttribute(id_job, getObjectId(), "includeML", this.getIncludeML());
            
            rep.saveStepAttribute(id_job, getObjectId(), "user_name", this.getUser());
            rep.saveStepAttribute(id_job, getObjectId(), "password", pass);
            rep.saveStepAttribute(id_job, getObjectId(), "includeSALT", this.getIncludeSALT());
            rep.saveStepAttribute(id_job, getObjectId(), "SALTPath", this.getSALTPath());
            rep.saveStepAttribute(id_job, getObjectId(), "compileFlags", this.getCompileFlags());
            
        
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

   
    
    
   
}
