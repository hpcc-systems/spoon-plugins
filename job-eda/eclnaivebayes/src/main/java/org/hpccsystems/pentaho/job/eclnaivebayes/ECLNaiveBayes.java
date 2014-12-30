/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclnaivebayes;

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
public class ECLNaiveBayes extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String datasetName = "";
    private java.util.List fields = new ArrayList();
    private String accuracy = "";
    private String precision = "";
    
	
    
    public String getAccuracy() {
		return accuracy;
	}

	public void setAccuracy(String accuracy) {
		this.accuracy = accuracy;
	}

	public String getPrecision() {
		return precision;
	}

	public void setPrecision(String precision) {
		this.precision = precision;
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
        	String frequency = "IMPORT ML;\n";boolean flag = true;String record = "";int cnt = 1;String dep_num = "";
        	String indep_num = "";String dep = "";
        	for(Iterator<Player> it = fields.iterator(); it.hasNext();){
        		Player P = (Player) it.next();
        		if(P.getDependance() == 0){        			
        			
        			record += getDatasetName()+"."+P.getFirstName()+";\n";
        			indep_num += "number="+cnt+ " OR ";
        			cnt++;        			
        		}
        		else{
        			dep = P.getFirstName();
        			record += getDatasetName()+"."+P.getFirstName()+";\n";
        			dep_num += "number="+cnt;
        			cnt++;
        		}
        		//frequency += "'"+P.getFirstName()+"',";
        		
        	}
        	//frequency = frequency.substring(0,frequency.length()-1);
        	indep_num = indep_num.substring(0,indep_num.length()-3);
        	//frequency += "];\n";
        	frequency += "NBRec := RECORD\nUNSIGNED id;\n"+record+"END;\n";
        	frequency += "NBRec idTrans("+getDatasetName()+" L, INTEGER C) := TRANSFORM\n	SELF.id := C;\n	SELF := L;\nEND;\n";
        	frequency += "d := PROJECT("+getDatasetName()+",idTrans(LEFT,COUNTER));\n";        	
        	frequency += "ML.ToField(d,o1);\n";
        	frequency += "o := ML.Discretize.ByRounding(o1);\n";
        	frequency += "X := o("+indep_num+");\n";
        	frequency += "Y := o("+dep_num+");\n";
        	frequency += "BayesModule := ML.Classify.NaiveBayes;\n";
        	frequency += "TestModule := BayesModule.TestD(X,Y);\n";
        	frequency += varName(dep,"Raw","TestModule","Raw",1);
        	frequency += varName(dep,"Cross","TestModule","CrossAssignments",2);
        	if(precision.equalsIgnoreCase("YES")){
        		frequency += varName(dep,"Precision","TestModule","PrecisionByClass",3);
        	}
        	if(accuracy.equalsIgnoreCase("YES")){
        		frequency += varName(dep,"Accuracy","TestModule","Headline",4);
        	}
        	
        	
        	
        	
        	
        	
        	logBasic("NaiveBayes Job =" + frequency);//{Dataset Job} 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, frequency);
	        
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	        result.setLogText("ECLFrequency executed, ECL code added");
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

    
    
    
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "precision")) != null)
                setPrecision(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "precision")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "accuracy")) != null)
                setAccuracy(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "accuracy")));
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
        retval += "		<precision><![CDATA[" + precision + "]]></precision>" + Const.CR;
        retval += "		<accuracy><![CDATA[" + accuracy + "]]></accuracy>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "precision") != null)
            	precision = rep.getStepAttributeString(id_jobentry, "precision"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "accuracy") != null)
                accuracy = rep.getStepAttributeString(id_jobentry, "accuracy"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "fields") != null)
                this.openFields(rep.getStepAttributeString(id_jobentry, "fields")); //$NON-NLS-1$
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "precision", precision); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "accuracy", accuracy); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "fields", this.saveFields()); //$NON-NLS-1$
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
