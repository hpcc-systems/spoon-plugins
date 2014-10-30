package org.hpccsystems.pentaho.job.eclpercentile;

class Test{
	public static void main(String[] args){
		String S = "(avgmos<>99999)";
		System.out.println(S.replaceFirst("\\(", "(DS."));    
	}
}