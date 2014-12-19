/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclchisquare;
import java.util.ArrayList;


public class ChiSquare {
	
	static int noOfStorageVariables =0;
	
	private String dataSetName= "";
	private String sampleDataSetName= "";
	private String columnNameInPopulation="";
	private String columnDatatypeInPopulation = "";
	private String columnNameInSample="";
	private String columnDatatypeInSample = "";
	private String output = "";
	
	public ChiSquare() {
		// TODO Auto-generated constructor stub
		ChiSquare.noOfStorageVariables++;
	}
	
	public String getOutputName() {
		return output;
	}
	public void setOutputName(String output) {
		this.output = output;
	}
	public String getDataSetName() {
		return dataSetName;
	}
	public void setDataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
	}
	public String getSampleDataSetName() {
		return sampleDataSetName;
	}
	public void setSampleDataSetName(String sampleDataSetName) {
		this.sampleDataSetName = sampleDataSetName;
	}
	public String getColumnNameInPopulation() {
		return columnNameInPopulation;
	}
	public void setColumnNameInPopulation(String columnNameInPopulation) {
		this.columnNameInPopulation = columnNameInPopulation;
	}
	public String getColumnDatatypeInPopulation() {
		return columnDatatypeInPopulation;
	}
	public void setColumnDatatypeInPopulation(String columnDatatypeInPopulation) {
		this.columnDatatypeInPopulation = columnDatatypeInPopulation;
	}
	public String getColumnNameInSample() {
		return columnNameInSample;
	}
	public void setColumnNameInSample(String columnNameInSample) {
		this.columnNameInSample = columnNameInSample;
	}
	public String getColumnDatatypeInSample() {
		return columnDatatypeInSample;
	}
	public void setColumnDatatypeInSample(String columnDatatypeInSample) {
		this.columnDatatypeInSample = columnDatatypeInSample;
	}
	
	public String ecl() {
		
		String ecl = "";
		int noOfVariable = ChiSquare.noOfStorageVariables;
		
		
		String normList = "";
		String nameOfFreqTableP = "";


		Frequency eclFrequency = new Frequency();
		eclFrequency.setDatasetName(this.dataSetName);
		eclFrequency.setNumber(noOfVariable+"");
		normList = this.columnNameInPopulation+",DESC,FREQUENCY,NO-";
		eclFrequency.setnormList(normList);
		eclFrequency.setDataType(this.columnDatatypeInPopulation);
		nameOfFreqTableP = "NameofFreqTableChisquarePopulation"+noOfVariable;
		eclFrequency.setOutputName(nameOfFreqTableP);
		String[] x = {this.columnNameInPopulation+"_Frequency"};
		eclFrequency.setoutTables(x);
		ecl += eclFrequency.ecl();
		
		eclFrequency = new Frequency();
		eclFrequency.setDatasetName(this.sampleDataSetName);
		eclFrequency.setNumber((noOfVariable+1)+"");
		normList = this.columnNameInSample+",DESC,FREQUENCY,NO-";
		eclFrequency.setnormList(normList);
		eclFrequency.setDataType(this.columnDatatypeInSample);
		String nameOfFreqTableSample = "NameofFreqTableChisquareSample"+noOfVariable;
		eclFrequency.setOutputName(nameOfFreqTableSample);
		x[0] = this.columnNameInSample+"_Frequency";
		eclFrequency.setoutTables(x);
		
		ecl += eclFrequency.ecl();

		
		String MainJoinedDatasetRecordChiSquare = " MainJoinedDatasetRecordChiSquare"+noOfVariable;
		ecl += MainJoinedDatasetRecordChiSquare+" := RECORD\n REAL8 freqExisting :=0; REAL8 freqExpected;\n " +
				"REAL8 chisquareTerm;\n END;\n";
		
		String JoinTemforChisquare = "JoinTemforChisquare"+noOfVariable;
		
		ecl += MainJoinedDatasetRecordChiSquare + " "+JoinTemforChisquare+"("+nameOfFreqTableP+" A, "+nameOfFreqTableSample+" B" +
				",INTEGER PopCOunt, INTEGER SampleCount) := TRANSFORM\n" +
				"SELF.freqExisting := B.frequency;\n" +
				"SELF.freqExpected := SampleCount/PopCount*A.frequency;\n" +
				"SELF.chisquareTerm := " +
				"(SELF.freqExisting-SELF.freqExpected)*(SELF.freqExisting-SELF.freqExpected)/SELF.freqExpected;\n" +
				"END;\n";
		
		String countPopulation = "countPopulationChisquare"+noOfVariable;
		String countSample = "countSampleChisquare"+noOfVariable;
		ecl += countPopulation+" := COUNT("+this.dataSetName+");\n";
		ecl += countSample+" := COUNT("+this.sampleDataSetName+");\n";
		String LOutJoinedRecsChis2quare = "LOutJoinedRecsChis2quare"+noOfVariable;
		ecl += LOutJoinedRecsChis2quare+" := JOIN("+nameOfFreqTableP+","+nameOfFreqTableSample+",LEFT."+
				this.columnNameInPopulation+
				"= RIGHT."+this.columnNameInSample+","+JoinTemforChisquare+"(LEFT,RIGHT,"+countPopulation+","+countSample+")," +
						"LEFT OUTER);\n";
        
		Mean eclUnvariate = new Mean();
        eclUnvariate.setDatasetName(LOutJoinedRecsChis2quare);
        eclUnvariate.setCheckList("true,fa,fa,fa,af,af,af");
        eclUnvariate.setFieldList("chisquareTerm");
        ArrayList<String> fields = new ArrayList<String>();
        fields.add("chisquareTerm");
        eclUnvariate.setPeople(fields);
        String MeanNameChiSquare = "MeanNameChiSquare"+noOfVariable;
        eclUnvariate.setSingle(MeanNameChiSquare);
        
        ecl += eclUnvariate.ecl();
        
        String countOfAllColumnsChisquare = "sumOfAllColumnsChisquare"+noOfVariable; 
        ecl += countOfAllColumnsChisquare +" := COUNT("+LOutJoinedRecsChis2quare+");\n";
        ecl += this.output+" := "+countOfAllColumnsChisquare+"*"+MeanNameChiSquare+"[1].mean;\n";
		
        ecl += "OUTPUT("+this.output+",named('"+this.output+"'));\n";
		return ecl;
		
	}

}
