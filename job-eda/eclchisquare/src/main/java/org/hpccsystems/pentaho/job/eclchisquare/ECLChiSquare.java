/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclchisquare;

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
public class ECLChiSquare extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String dataSetName = "";
    private String sampleDataSetName = "";
    private String population_column = "";
    private String sample_column = "";
    private String columnDatatypeInPopulation = "";
    private String columnDatatypeInSample = "";
    private String output = "";

    private java.util.List fields = new ArrayList();
      
    public String getOutputName() {
		return output;
	}

	public void setOutputName(String output) {
		
		this.output = output;
	}

	public String getPopulationField() {
		return population_column;
	}
	
	public String getSampleField() {
		return sample_column;
	}


	public void setPopulationField(String ColumnName) {
		this.population_column = ColumnName;
	}	
	
	public void setSampleField(String ColumnName) {
		this.sample_column = ColumnName;
	}	
	
	public String getDataSetName() {
        return dataSetName;
    }
	
	public String getSampleDataSetName() {
        return sampleDataSetName;
    }
    public void setDataSetName(String dataSetName) {
    	
        this.dataSetName = dataSetName;
    }
    
    public void setSampleDataSetName(String sampleDataSetName) {
    	
        this.sampleDataSetName = sampleDataSetName;
    }
    
    public String getColumnDatatypeInPopulation() {
		return columnDatatypeInPopulation;
	}
	public void setColumnDatatypeInPopulation(String columnDatatypeInPopulation) {
		
		this.columnDatatypeInPopulation = columnDatatypeInPopulation;
	}

	public String getColumnDatatypeInSample() {
		return columnDatatypeInSample;
	}
	public void setColumnDatatypeInSample(String columnDatatypeInSample) {
		
		this.columnDatatypeInSample = columnDatatypeInSample;
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

        	logBasic("sampleDataSetName********** " + getSampleDataSetName() );
        	logBasic("dataSetName********** " + getDataSetName() );
        	logBasic("columnDatatypeInPopulation********** " + getColumnDatatypeInPopulation() );
        	logBasic("ColumnDatatypeInSample********** " + getColumnDatatypeInSample() );
        	logBasic("output********** " + getOutputName() );
        	
        	logBasic("getPopulationField********** " + getPopulationField() );
        	logBasic("getSampleField********** " + getSampleField() );
        	
        	
        	ChiSquare csquare = new ChiSquare();
        	csquare.setDataSetName(getDataSetName());
        	csquare.setSampleDataSetName(getSampleDataSetName());
        	csquare.setColumnNameInPopulation(getPopulationField());
        	csquare.setColumnNameInSample(getSampleField());        	
        	csquare.setOutputName(getOutputName());
        	csquare.setColumnDatatypeInPopulation(getColumnDatatypeInPopulation());
        	csquare.setColumnDatatypeInSample(getColumnDatatypeInSample());
        	
        	    	
        	logBasic("PValueJob =" + csquare.ecl());//{Dataset Job} 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, csquare.ecl());
	        
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
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "sampleDataSetName")) != null)
            	setSampleDataSetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "sampleDataSetName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "population_column")) != null)
            	setPopulationField(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "population_column")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "sample_column")) != null)
            	setSampleField(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "sample_column")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output")) != null)
            	setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output")));
            
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "columnDatatypeInPopulation")) != null)
            	setColumnDatatypeInPopulation(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "columnDatatypeInPopulation")));
            
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "columnDatatypeInSample")) != null)
            	setColumnDatatypeInSample(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "columnDatatypeInSample")));
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
        retval += "		<sampleDataSetName><![CDATA[" + sampleDataSetName + "]]></sampleDataSetName>" + Const.CR;
        retval += "		<population_column><![CDATA[" + population_column + "]]></population_column>" + Const.CR;
        retval += "		<sample_column><![CDATA[" + sample_column + "]]></sample_column>" + Const.CR;
        retval += "		<columnDatatypeInPopulation><![CDATA[" + columnDatatypeInPopulation + "]]></columnDatatypeInPopulation>" + Const.CR;
        retval += "		<columnDatatypeInSample><![CDATA[" + columnDatatypeInSample + "]]></columnDatatypeInSample>" + Const.CR;
        retval += "		<output><![CDATA[" + output + "]]></output>" + Const.CR;
        
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "dataSetName") != null)
            	dataSetName = rep.getStepAttributeString(id_jobentry, "dataSetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "sampleDataSetName") != null)
            	sampleDataSetName = rep.getStepAttributeString(id_jobentry, "sampleDataSetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "population_column") != null)
            	population_column = rep.getStepAttributeString(id_jobentry, "population_column"); //$NON-NLS-1$   
            if(rep.getStepAttributeString(id_jobentry, "sample_column") != null)
            	sample_column = rep.getStepAttributeString(id_jobentry, "sample_column"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "columnDatatypeInPopulation") != null)
            	columnDatatypeInPopulation = rep.getStepAttributeString(id_jobentry, "columnDatatypeInPopulation"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "columnDatatypeInSample") != null)
            	columnDatatypeInSample = rep.getStepAttributeString(id_jobentry, "columnDatatypeInSample"); //$NON-NLS-1$
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
            
            rep.saveStepAttribute(id_job, getObjectId(), "sampleDataSetName", sampleDataSetName); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "fields", this.saveFields()); //$NON-NLS-1$        
               
            rep.saveStepAttribute(id_job, getObjectId(), "population_column", population_column); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "sample_column", sample_column); //$NON-NLS-1$
            
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
