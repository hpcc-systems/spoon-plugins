package org.hpccsystems.salt.dataprofiling;

public class StringBuilderEx {

	private StringBuilder builder;
	
	public StringBuilderEx() 

	{
		builder = new StringBuilder();
		
	}
	
	public void append(String s) {
		builder.append(s);
	}
	
	public void appendLine(String s) {
		builder.append(s + "\r\n");
	}
	
	public String toString() {
		return builder.toString();
	}
	
}
