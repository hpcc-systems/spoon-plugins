/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclkscoefficient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.ecljobentrybase.*;
import org.hpccsystems.javaecl.Filter;

/**
 *
 * @author KeshavS
 */
public class ECLKSCoefficient extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String dataSetName = "";
    private String columnName = "";
    
    private String output = "";

    private java.util.List fields = new ArrayList();
      
    public String getOutputName() {
		return output;
	}

	public void setOutputName(String output) {
		
		this.output = output;
	}
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}	
	
	public String getDataSetName() {
        return dataSetName;
    }
	
	
    public void setDataSetName(String dataSetName) {
    	
        this.dataSetName = dataSetName;
    }
    
    
	
    	public void setFields(java.util.List fields){
		this.fields = fields;
	}
	
    	
	public java.util.List getFields(){
		return fields;
	}

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
    	Result result = modifyResults(prevResult);
        if(result.isStopped()){
        return result;
       }

        else{	

        	logBasic("DataSetName********** " + getDataSetName() );
        	logBasic("columnName********** " + getColumnName() );
        	logBasic("output********** " + getOutputName() );
        	
        	
        	KSCoefficient coeff = new KSCoefficient();
        	coeff.setDatasetName(getDataSetName());
        	coeff.setColumnName(getColumnName());
        	coeff.setOutputName(getOutputName());
        	
        	
        	    	
        	logBasic("PValueJob =" + coeff.ecl());//{Dataset Job} 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, coeff.ecl());
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	                
	        result.setLogText("ECLGiniCoefficient executed, ECL code added");
        	
        }
		return result;
    }
    
    public String saveFields(){
    	String out = "";
    	
    	Iterator it = fields.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		//Player p = (Player) it.next();
    		//out +=  p.getFirstName()+","+p.getDependance()+","+p.getIndex();
            isFirst = false;
    	}
    	return out;
    }

    public void openFields(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	fields = new ArrayList();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		
        	}
        }
    }
    
      
    
    
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataSetName")) != null)
            	setDataSetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataSetName")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "columnName")) != null)
            	setColumnName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "columnName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output")) != null)
            	setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fields")) != null)
                openFields(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fields")));
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<fields><![CDATA[" + this.saveFields() + "]]></fields>" + Const.CR;
        retval += "		<dataSetName><![CDATA[" + dataSetName + "]]></dataSetName>" + Const.CR;      
        retval += "		<columnName><![CDATA[" + columnName + "]]></columnName>" + Const.CR;           
        retval += "		<output><![CDATA[" + output + "]]></output>" + Const.CR;
        
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "dataSetName") != null)
            	dataSetName = rep.getStepAttributeString(id_jobentry, "dataSetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "columnName") != null)
            	columnName = rep.getStepAttributeString(id_jobentry, "columnName"); //$NON-NLS-1$            
            if(rep.getStepAttributeString(id_jobentry, "output") != null)
            	output = rep.getStepAttributeString(id_jobentry, "output"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "fields") != null)
                this.openFields(rep.getStepAttributeString(id_jobentry, "fields")); //$NON-NLS-1$  
            
            
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "dataSetName", dataSetName); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "columnName", columnName); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "fields", this.saveFields()); //$NON-NLS-1$        
                         
            rep.saveStepAttribute(id_job, getObjectId(), "output", output); //$NON-NLS-1$
            
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
    
    public String varName(String dep, String tablename,String module,String stat,int i){
    	String ret = "";
    	ret += tablename+"_Table := "+module+"."+stat+";\n";
    	ret += tablename+"Rec := RECORD\n	STRING Dependant;\n	"+tablename+"_Table;\nEND;\n";
    	ret += tablename+"Rec Trans"+i+"("+tablename+"_Table L) := TRANSFORM\n	SELF.Dependant := '"+dep+"';\n	SELF := L;\nEND;\n";
    	ret += tablename+" := PROJECT("+tablename+"_Table, Trans"+i+"(LEFT));\nOUTPUT("+tablename+",NAMED('"+tablename+"Table'));\n";
    	return ret;
    }
    
}
