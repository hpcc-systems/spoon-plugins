package org.hpccsystems.pentaho.job.eclchisquare;

public class Tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
    	ChiSquare csquare = new ChiSquare();
    	csquare.setDataSetName("DS");
    	csquare.setSampleDataSetName("DS2");
    	csquare.setColumnNameInPopulation("Dayofmonth");
    	csquare.setColumnNameInSample("dayofmonth");        	
    	csquare.setOutputName("hhhhh");
    	csquare.setColumnDatatypeInPopulation("REAL");
    	csquare.setColumnDatatypeInSample("REAL");
    	System.out.println(csquare.ecl());

	}

}
