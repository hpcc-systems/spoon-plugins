package org.hpccsystems.salt.hygiene;

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

    public void insert(int offset, String value) {
        builder.insert(offset, value);
    }
	
	public String toString() {
		return builder.toString();
	}

    public int length() {
        return builder.length();
    }
	
}
