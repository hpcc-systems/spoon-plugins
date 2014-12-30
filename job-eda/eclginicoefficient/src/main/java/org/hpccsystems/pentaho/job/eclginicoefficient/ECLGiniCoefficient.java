/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclginicoefficient;

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
public class ECLGiniCoefficient extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String datasetName = "";
    private String ColumnName = "";
    private String Output = "";

    private java.util.List fields = new ArrayList();
      
    public String getOutput() {
		return Output;
	}

	public void setOutput(String output) {
		Output = output;
	}

	public String getField() {
		return ColumnName;
	}

	public void setField(String ColumnName) {
		this.ColumnName = ColumnName;
	}	
	

	public String getDatasetName() {
        return datasetName;
    }
    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
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

        	
        	GiniCoefficient gcoef = new GiniCoefficient();
        	gcoef.setDatasetName(getDatasetName());
        	gcoef.setField(getField());
        	
        	gcoef.setOutput(getOutput());
        	
        	    	
        	logBasic("PValueJob =" + gcoef.ecl());//{Dataset Job} 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, gcoef.ecl());
	        
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
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Output")) != null)
            	setOutput(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Output")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "ColumnName")) != null)
            	setField(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "ColumnName")));
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
        retval += "		<dataset_name><![CDATA[" + datasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<Output><![CDATA[" + Output + "]]></Output>" + Const.CR;
        retval += "		<ColumnName><![CDATA[" + ColumnName + "]]></ColumnName>" + Const.CR;
        
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "Output") != null)
            	Output = rep.getStepAttributeString(id_jobentry, "Output"); //$NON-NLS-1$ 
            if(rep.getStepAttributeString(id_jobentry, "ColumnName") != null)
            	Output = rep.getStepAttributeString(id_jobentry, "ColumnName"); //$NON-NLS-1$             
            if(rep.getStepAttributeString(id_jobentry, "fields") != null)
                this.openFields(rep.getStepAttributeString(id_jobentry, "fields")); //$NON-NLS-1$  
            
            
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "fields", this.saveFields()); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "Output", Output); //$NON-NLS-1$            
            
            rep.saveStepAttribute(id_job, getObjectId(), "ColumnName", ColumnName); //$NON-NLS-1$
            
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
