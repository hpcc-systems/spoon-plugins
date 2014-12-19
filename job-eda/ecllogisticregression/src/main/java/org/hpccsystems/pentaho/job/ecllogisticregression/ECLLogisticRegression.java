/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecllogisticregression;

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
public class ECLLogisticRegression extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String datasetName = "";
    private String ModelName = "";
    private String dependantField;
    private ArrayList<String> independantFields = new ArrayList<String>();
    private java.util.List fields = new ArrayList();
      
    	
	
	public String getModelName() {
        return ModelName;
    }
	
	public void setModelName(String ModelName) {
        this.ModelName = ModelName;
    }

	public String getDatasetName() {
        return datasetName;
    }
    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }
    
    public String getdependantField() {
        return dependantField;
    }
    public void setdependantField(String dependantField) {
        this.dependantField = dependantField;
    }
    
    public ArrayList<String> getIndependantFields() {
        return independantFields;
    }
    public void setIndependantFields(ArrayList<String> independantFields) {
        this.independantFields = independantFields;
    }

    	public void setFields(java.util.List fields){
		this.fields = fields;
	}
	
	public java.util.List getFields(){
		return fields;
	}

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
    	Result result = prevResult;
        if(result.isStopped()){
        	return result;
        }
        else{	

        	
        	LogisticRegression logres = new LogisticRegression();
        	logres.setDatasetName(getDatasetName());
        	logres.setModelName(getModelName());
        	logres.setDependantField(getdependantField());
        	logres.setIndependantFields(getIndependantFields());    
        	/*logBasic ("Gagan *********" + getDatasetName());
        	logBasic ("Gagan *********" + getModelName());
        	    	
        	logBasic ("Gagan *********" + getdependantField());
        	logBasic ("Gagan *********" + getIndependantFields());*/
        	    	
        	logBasic("LogisticRegressionJob =" + logres.ecl());//{Dataset Job} 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, logres.ecl());
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	                
	        result.setLogText("ECLLogisticRegression executed, ECL code added");
        	return result;
        }
    }
    
    public String saveFields(){
    	String out = "";
    	
    	Iterator it = fields.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getDependance()+","+p.getIndex();
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
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		P.setDependance(Integer.parseInt(S[1]));
        		P.setIndex(S[2]);
        		fields.add(P);
        	}
        }
    }
    
    public String saveIndependantFields(){
    	String out = "";
    	boolean isFirst = true;
    	Iterator<String> it = independantFields.iterator();
    	
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		out += it.next();
    		isFirst = false;
    	}
    	return out;
    }

    public void openIndependantFields(String in){
        String[] strLine = in.split("[|]");       
        int len = strLine.length;
        independantFields = new ArrayList<String>();
        if(len > 0){
	       	for(int i = 0; i<strLine.length; i++){      		
	       		  		
	       		independantFields.add(strLine[i]);
	       	
	       	}
        }
    }

    
    
    
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "ModelName")) != null)
                setModelName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "ModelName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dependantField")) != null)
                setdependantField((XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dependantField"))));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "independantFields")) != null)
            	openIndependantFields((XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "independantFields"))));
            
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
        retval += "		<ModelName><![CDATA[" + ModelName + "]]></ModelName>" + Const.CR;
        retval += "		<dependantField><![CDATA[" + dependantField + "]]></dependantField>" + Const.CR;
        retval += "		<independantFields><![CDATA[" + this.saveIndependantFields() + "]]></independantFields>" + Const.CR;
                return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "ModelName") != null)
            	ModelName = rep.getStepAttributeString(id_jobentry, "ModelName"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "fields") != null)
                this.openFields(rep.getStepAttributeString(id_jobentry, "fields")); //$NON-NLS-1$  
            
            if(rep.getStepAttributeString(id_jobentry, "dependantField") != null)
                dependantField= rep.getStepAttributeString(id_jobentry, "dependantField"); //$NON-NLS-1$
            
            
            if(rep.getStepAttributeString(id_jobentry, "independantFields") != null)
            	this.openIndependantFields(rep.getStepAttributeString(id_jobentry, "independantFields")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "ModelName", ModelName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "dependantField", dependantField); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "fields", this.saveFields()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "independantFields", this.saveIndependantFields()); //$NON-NLS-1$
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
