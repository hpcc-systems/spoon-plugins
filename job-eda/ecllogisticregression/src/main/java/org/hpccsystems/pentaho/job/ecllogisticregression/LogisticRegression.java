package org.hpccsystems.pentaho.job.ecllogisticregression;

import java.util.ArrayList;
import java.util.Iterator;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */



/**
 *
 * @author Siddharth
 */
public class LogisticRegression {
	

    private String name;
    private String datasetName;
    private ArrayList<String> independantFields;
    private String dependantField;
    //Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    
    public String getDependantField() {
		return dependantField;
	}

	public void setDependantField(String dependantField) {
		this.dependantField = dependantField;
	}
    
    public LogisticRegression() {
		// TODO Auto-generated constructor stub
    	Storage.noOfVariables++;
	}

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public ArrayList<String> getIndependantFields() {
        return independantFields;
    }

    public void setIndependantFields(ArrayList<String> independantFields) {
        this.independantFields = independantFields;
    }

    public String getModelName() {
        return name;
    }

    public void setModelName(String name) {
        this.name = name;
    }
    public String ecl() {
    	
    	int noOfVar = Storage.noOfVariables;
        String ecl ="IMPORT ML;\n";
        
    	String record = "";
    	int cnt = 2;
    	String dep_num = "";
    	String indep_num = "";
    	String indep = "";
    	dep_num += "number=1";
        for(Iterator<String> it = getIndependantFields().iterator(); it.hasNext();){
               String P = (String) it.next();
                      indep += "'"+P+"', ";
                      record += getDatasetName()+"."+P+";\n";
                      indep_num += "number="+cnt+ " OR ";
                      cnt++;                            
        }
    	
    	indep_num = indep_num.substring(0,indep_num.length()-3);
    	indep = indep.substring(0, indep.length()-2);
    	String NBRec = "NBRec"+noOfVar;
    	ecl += NBRec+" := RECORD\nUNSIGNED id;\n"+record+"END;\n";
    	String idTrans = "idTrans"+noOfVar;
    	ecl += NBRec+" "+idTrans+"("+getDatasetName()+" L, INTEGER C) := TRANSFORM\n	SELF.id := C;\n	SELF := L;\nEND;\n";
    	String newdataSetLogis = "newdataSetLogis" + noOfVar;
    	ecl += newdataSetLogis+" := PROJECT("+getDatasetName()+","+idTrans+"(LEFT,COUNTER));\n";        	
   	
    	
    	
    	
    	
    	String mlVariable = "MLVariable"+noOfVar;
        String indVarName = "indVarName"+noOfVar;
        String depVarName = "depVarName"+noOfVar;
        
        ecl += "ML.ToField("+newdataSetLogis+","+mlVariable+");\n";
        
        ecl += depVarName + " := " + mlVariable+ "("+dep_num+");\n";
        ecl += indVarName + " := "+mlVariable+"("+indep_num+");\n";
        
        
        
        
        String depVarNameDisc = "depVarNameDisc"+noOfVar;
        ecl += depVarNameDisc +":= ML.Discretize.ByRounding("+depVarName+");\n";
        String count = "CountMlLogistic"+noOfVar;
        ecl += count +":= COUNT("+newdataSetLogis+");\n";
        String LogisticModule = "LogisticModule"+noOfVar;
        ecl += LogisticModule +" := ML.Classify.Logistic(,,"+count+");\n";
        ecl += this.name+" := "+LogisticModule+".LearnC("+indVarName+","+depVarNameDisc+");\n";
        String clas = "OutputClassifierLogistic"+noOfVar;
        ecl += clas+" :="+LogisticModule+".ClassifyC("+indVarName+","+this.name+");\n";
        ecl += "OUTPUT("+clas+",named('"+this.name+"'));\n";

        
        return ecl;

    }
}
