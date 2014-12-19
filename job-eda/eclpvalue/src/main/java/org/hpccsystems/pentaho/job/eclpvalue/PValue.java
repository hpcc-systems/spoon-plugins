/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.hpccsystems.pentaho.job.eclpvalue;
/**
 *
 * @author Siddharth
 */
public class PValue {
	

//    private String name;
    private String datasetName;
    private String field;
    private String valueOfX;
    private String Output;
    public String getOutput() {
		return Output;
	}

	public void setOutput(String output) {
		Output = output;
	}

	public String getValueOfX() {
		return valueOfX;
	}

	public void setValueOfX(String valueOfX) {
		this.valueOfX = valueOfX;
	}
	private int distribution;
    //Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    
    public int getDistribution() {
		return distribution;
	}

	public void setDistribution(int distribution) {
		this.distribution = distribution;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}
    
    public PValue() {
		// TODO Auto-generated constructor stub
    	Storage.noOfVariables++;
	}

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

//    public String getName() {
//        return name;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
    public String ecl() {
    	int noOfVar = Storage.noOfVariables;
        String ecl ="IMPORT ML;\n";
        
        
    	String record = "";
		record += getDatasetName()+"."+this.field+";\n";

		
    	String NBRec = "NBRec"+noOfVar;

    	ecl += NBRec+" := RECORD\nUNSIGNED id;\n"+record+"END;\n";
    	String idTrans = "idTrans"+noOfVar;
    	ecl += NBRec+" "+idTrans+"("+getDatasetName()+" L, INTEGER C) := TRANSFORM\n	SELF.id := C;\n	SELF := L;\nEND;\n";
    	String newdataSetLogis = "newdataSetLogis" + noOfVar;
    	ecl += newdataSetLogis+" := PROJECT("+getDatasetName()+","+idTrans+"(LEFT,COUNTER));\n";        	
   	
    	
    	
    	
    	
    	String mlVariable = "MLVariablepValue"+noOfVar;
        String indVarName = "indVarNamePvalue"+noOfVar;

        ecl += "ML.ToField("+newdataSetLogis+","+mlVariable+");\n";
        
        ecl += indVarName + " := "+mlVariable+"(Number=1);\n";
        
        String meanValueForP = "pValueForP"+noOfVar; 
        ecl += meanValueForP + " := ave("+indVarName+",value);\n";
        
        String varPvalue = "varPvalue"+noOfVar;
        ecl += varPvalue+" := variance("+indVarName+",value);\n";
        
        String standardDev = "standardDevPvalue"+noOfVar;
        ecl += standardDev+" := SQRT("+varPvalue+");\n";
        
        String NormalDistribution = "NormalDistributionForPvalue"+noOfVar;
        ecl += NormalDistribution + " := ML.Distribution.Normal("+meanValueForP+","+standardDev+",10000);\n";
        
        ecl += this.Output+" := "+NormalDistribution+".Cumulative("+this.valueOfX+");\n";
        ecl += "OUTPUT("+this.Output+",named('"+this.Output+"'));";
        
        return ecl;

    }
}
