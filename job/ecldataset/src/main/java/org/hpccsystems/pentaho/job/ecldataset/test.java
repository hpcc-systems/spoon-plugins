package org.hpccsystems.pentaho.job.ecldataset;

public class test {

	public static void main(String[] args){
		System.out.println("test");
		String x = "asf-asdf8_2";
		System.out.println(x.replaceAll("\\d+",""));
		System.out.println("Letters: " + x.replaceAll("\\d+[_]*",""));
		
		System.out.println(x.replaceAll("\\D+",""));
		System.out.println("Numbers: " + x.replaceAll("[^0-9_]+",""));
	}
}
