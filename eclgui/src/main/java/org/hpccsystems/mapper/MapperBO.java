package org.hpccsystems.mapper;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

public class MapperBO {
	
	public MapperBO(){
		super();
	}
	
	public MapperBO(String in){
        super();
        fromCSV(in);
    }
	
	private String opVariable;
	
	private String expression;
	
	public String getOpVariable() {
		return opVariable;
	}

	public void setOpVariable(String opVariable) {
		this.opVariable = opVariable;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}
	
	public String toCSV(){
		String str = "";
		if(opVariable.contains(",")){
			str = "\"" + opVariable + "\"";
		}else{
			str = opVariable;
		}
		str += ",";
		if(expression.contains(",")){
			str += "\"" + expression + "\"";
		}else{
			str += expression;
		}
        return str;
    }
	
    public void fromCSV(String in){
        //String[] strArr = in.split("[,]");//"\\,"
        //String regex = "(?:(?<=\")([^\"]*)(?=\"))|(?<=,|^)([^,]*)(?=,|$)";
       // String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        //String[] strArr = in.split(regex);//"\\,
        CSVParser csvP = new CSVParser();
        String[] strArr = null;
		try {
			strArr = csvP.parseLine(in);
			if(strArr.length >= 1)
				opVariable = strArr[0];
			else
				opVariable = "";
			if(strArr.length >= 2)
				expression = strArr[1];
			else
				expression = "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
    }
}
