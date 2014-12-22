package org.hpccsystems.pentaho.job.eclkscoefficient;

public class KSCoefficient {
	
	private String datasetName = "";
	private String columnName = "";
	private int distributionToBeCompared = 0;
	private String outputName = "";
	public String getDatasetName() {
		return datasetName;
	}
	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public int getDistributionToBeCompared() {
		return distributionToBeCompared;
	}
	public void setDistributionToBeCompared(int distributionToBeCompared) {
		this.distributionToBeCompared = distributionToBeCompared;
	}
	public String getOutputName() {
		return outputName;
	}
	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}

	public String ecl() {
    	int noOfVar = Storage.noOfVariables;
        String ecl ="IMPORT ML;\n";
        
        String sortedDatasetName = "KSCoeffSortDataset"+noOfVar; 
        ecl += sortedDatasetName +" := SORT("+this.getDatasetName()+",'"+this.columnName+"');\n";      
        
    	String count = "COuntKSValuesinDataset"+noOfVar;
    	ecl += count+" := COUNT("+sortedDatasetName+");\n";
    	
    	
    	String record = "REAL8 valueX;\n";
    	String NBRec = "NBRecKSCOeff"+noOfVar;
    	ecl += NBRec+" := RECORD\nUNSIGNED id;\n"+record+"END;\n";
    	
    	String idTrans = "idTransKSCoeff"+noOfVar;
    	ecl += NBRec+" "+idTrans+"("+getDatasetName()+" L, INTEGER C) := TRANSFORM\n	SELF.id := C;\n	" +
    			"SELF.valueX := L."+this.columnName+";\nEND;\n";
    	String newdataSetKSCoeff = "newdataSetKSCoeff" + noOfVar;
    	ecl += newdataSetKSCoeff+" := PROJECT("+getDatasetName()+","+idTrans+"(LEFT,COUNTER));\n";        	
   	
    	String mlVariable = "MLVariableKSCoeff"+noOfVar;
        ecl += "ML.ToField("+newdataSetKSCoeff+","+mlVariable+");\n";    	
    	
        String colVarName = "colVarNameKSCoeff"+noOfVar;
        ecl += colVarName +" := "+mlVariable+"(NUMBER=1);\n";
        
        
        String meanValueForP = "meanValueForKSCoeff"+noOfVar; 
        ecl += meanValueForP + " := ave("+colVarName+",value);\n";
        
        String varPvalue = "varPvalueKSCoeff"+noOfVar;
        ecl += varPvalue+" := variance("+colVarName+",value);\n";
        
        String standardDev = "standardDevPvalueKSCoeff"+noOfVar;
        ecl += standardDev+" := SQRT("+varPvalue+");\n";
        
        String NormalDistribution = "NormalDistributionForKSCoefficient"+noOfVar;
        ecl += NormalDistribution + " := ML.Distribution.Normal("+meanValueForP+","+standardDev+",10000);\n";
 
    	
        String RecordForStoringDvalues = "RecordForStoringDvalues"+noOfVar;
        ecl += RecordForStoringDvalues +" := RECORD\nREAL8 valueOfD;\nEND;\n";
        
        String transFormForKSCoeff = "transFormForKSCoeff"+noOfVar;
        ecl += RecordForStoringDvalues+"  "+transFormForKSCoeff+"("+sortedDatasetName+" L, INTEGER C,INTEGER N) := TRANSFORM\n" +
        		"	SELF.ValueOfD := MAX(ABS("+NormalDistribution+".cumulative(L."+this.columnName+")-(C/N))," +
        		"ABS("+NormalDistribution+".cumulative(L."+this.columnName+")-((C-1)/N)));\n" +
        				"END;\n";
        String DvaluesinTable = "DvaluesinTableFOrKSCoeff"+noOfVar;
        
        ecl += DvaluesinTable+" := PROJECT("+sortedDatasetName+","+transFormForKSCoeff+"(LEFT,COUNTER,"+count+"));\n";
        ecl += this.outputName+" := MAX("+DvaluesinTable+","+DvaluesinTable+".ValueOfD);\n";
        ecl += "OUTPUT("+this.outputName+",named('"+this.outputName+"'));\n";
        
    	return ecl;

	}
}
