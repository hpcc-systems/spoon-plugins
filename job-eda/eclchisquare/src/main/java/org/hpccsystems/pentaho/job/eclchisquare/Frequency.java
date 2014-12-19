
/* * To change this template, choose Tools | Templates
 * and open the template in the editor.*/
 
package org.hpccsystems.pentaho.job.eclchisquare;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/**
 *
 * @author KeshavS
 */
public class Frequency {//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	private String Sort = "";
	private String DatasetName = "";
	private String normList = "";
	private String data_type = "";
	private String outTables[] = null;
    private String Number = "";
	private String outputName ="";
	private String persist = "";
	private String defJobName = "";
	
	
	public String getOutputName() {
		return outputName;
	}

	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}
	
	public void setoutTables(String[] outTables){
		this.outTables = outTables;
	}
	
	public String[] getoutTables(){
		return outTables;
	}
	
    
	public String getNumber() {
        return Number;
    }

    public void setNumber(String Number) {
        this.Number = Number;
    }


	public String getDataType(){
		return data_type;
	}
    
	public void setDataType(String data_type){
		this.data_type = data_type;
	}

	
	public String getSort(){
		return Sort;
	}
    
	public void setSort(String Sort){
		this.Sort = Sort;
	}
	
	
	public String getDatasetName(){
		return DatasetName;
	}
    
	public void setDatasetName(String DatasetName){
		this.DatasetName = DatasetName;
	}
	
	public String getnormList(){
		return normList;
	}
    
	public void setnormList(String normList){
		this.normList = normList;
	}
    
    public String ecl() {
    	
    	String fieldStr = ""; 
    	String frequency = "";
    	String[] norm = this.normList.split("-");
    	String valueStr = "";
    	String[] dataT = data_type.toLowerCase().split(",");
        String fieldNum = "";
        String valueNum = "";
        int str = 0;
        int notstr = 0;
        outTables = new String[norm.length];
        for(int i = 0; i < norm.length; i++){
        	String[] cols = norm[i].split(",");
        	if(dataT[i].startsWith("integer") || dataT[i].startsWith("decimal") || dataT[i].startsWith("real") || dataT[i].startsWith("unicode")){ 
        		valueNum += "LEFT."+cols[0]+",";
    			fieldNum += "\'"+cols[0]+"\',";
    			notstr++;
    		}
    		else{
    			valueStr += "LEFT."+cols[0]+",";
    			fieldStr += "\'"+cols[0]+"\',";
    			str++;
    		}
        }
        if(valueStr.length()>0){
        	valueStr = valueStr.substring(0, valueStr.length()-1);
        	fieldStr = fieldStr.substring(0, fieldStr.length()-1);
        	frequency += "NumFieldStr"+getNumber()+":=RECORD\nSTRING field;\nSTRING value;\nEND;\n";
        	frequency += "OutDSStr"+getNumber()+" := NORMALIZE("+this.getDatasetName()+","+str+", TRANSFORM(NumFieldStr"+getNumber()+",SELF.field:=CHOOSE(COUNTER,"+fieldStr+");SELF.value:=CHOOSE" +
        			"(COUNTER,"+valueStr+")));\n";
        	 frequency += "FreqRecStr"+getNumber()+":=RECORD\nOutDSStr"+getNumber()+".field;\nOutDSStr"+getNumber()+".value;\nINTEGER frequency:=COUNT(GROUP);\n" +
        		     "REAL8 Percent:=(COUNT(GROUP)/COUNT("+this.DatasetName+"))*100;\n" +
        		     "END;\n";
        	 
        	 frequency += "Frequency1"+getNumber()+" := TABLE(OutDSStr"+getNumber()+",FreqRecStr"+getNumber()+",field,value,MERGE);\n";
        }
        if(valueNum.length()>0){
        	valueNum = valueNum.substring(0, valueNum.length()-1);		        
	        fieldNum = fieldNum.substring(0, fieldNum.length()-1);
	        frequency += "NumField"+getNumber()+getNumber()+":=RECORD\nSTRING field;\nREAL value;\nEND;\n";
	        frequency += "OutDSNum"+getNumber()+getNumber()+" := NORMALIZE("+this.getDatasetName()+","+notstr+", TRANSFORM(NumField"+getNumber()+getNumber()+",SELF.field:=CHOOSE(COUNTER,"+fieldNum+");SELF.value:=CHOOSE" +
        			"(COUNTER,"+valueNum+")));\n";
	        frequency += "FreqRecNum"+getNumber()+getNumber()+":=RECORD\nOutDSNum"+getNumber()+getNumber()+".field;\nOutDSNum"+getNumber()+getNumber()+".value;\nINTEGER frequency:=COUNT(GROUP);\n" +
       		     "REAL8 Percent:=(COUNT(GROUP)/COUNT("+this.DatasetName+"))*100;\n" +
       		     "END;\n";
	        frequency += "Frequency2"+getNumber()+" := TABLE(OutDSNum"+getNumber()+getNumber()+",FreqRecNum"+getNumber()+getNumber()+",field,value,MERGE);\n";

        }
        
        
        for(int j = 0; j<norm.length; j++){
        	String[] cols = norm[j].split(",");
        	if(getSort().equals("NO") || getSort().equals("")){
        		if(dataT[j].startsWith("string")) {
        			frequency += this.outputName+":= TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent});\n";
        			//frequency += "OUTPUT("+this.outputName+",THOR);\n";
        			if(persist.equalsIgnoreCase("true")){
        	    		if(outputName != null && !(outputName.trim().equals(""))){
        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+outputName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
        	    		}else{
        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+defJobName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
        	    		}
        	    	}
        	    	else{
//        	    		frequency += "OUTPUT("+this.outputName+",NAMED('Frequency_"+cols[0]+"'));\n";
        	    	}
        			
        		}
        		else{
        			frequency += this.outputName+":=TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent});\n";
        			//frequency += "OUTPUT("+this.outputName+",THOR);\n";
        			if(persist.equalsIgnoreCase("true")){
        	    		if(outputName != null && !(outputName.trim().equals(""))){
        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+outputName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
        	    		}else{
        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+defJobName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
        	    		}
        	    	}
        	    	else{
//        	    		frequency += "OUTPUT("+this.outputName+",NAMED('Frequency_"+cols[0]+"'));\n";
        	    	}
        			
        		}
        	}
        	else{
        		if(cols[1].equals("ASC")){
        			if(cols[2].equals("NAME")){
        				if(cols[3].equals("YES")){
        					if(dataT[j].startsWith("string"))
        						frequency += this.outputName+":=SORT(TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),(REAL)"+cols[0]+");\n";
        					else	        						
        						frequency += this.outputName+":=SORT(TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),(REAL)"+cols[0]+");\n";
        				}
        				else{
        					if(dataT[j].startsWith("string"))
        						frequency += this.outputName+":=SORT(TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),"+cols[0]+");\n";
        					else
        						frequency += this.outputName+":=SORT(TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),"+cols[0]+");\n";
        				}
        					
        				//frequency += "OUTPUT("+this.outputName+",THOR);\n";
        				if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+outputName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+defJobName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
//	        	    		frequency += "OUTPUT("+this.outputName+",NAMED('Frequency_"+cols[0]+"'));\n";
	        	    	}
        				
        			}
        			else{
        				if(dataT[j].startsWith("string"))
        					frequency += this.outputName+":=SORT(TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),frequency);\n";
        				else
        					frequency += this.outputName+":=SORT(TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),frequency);\n";
        				//frequency += "OUTPUT("+this.outputName+",THOR);\n";
        				if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+outputName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+defJobName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
//	        	    		frequency += "OUTPUT("+this.outputName+",NAMED('Frequency_"+cols[0]+"'));\n";
	        	    	}


        			}
        		}
        		else if(cols[1].equals("DESC")){
        			if(cols[2].equals("NAME")){
        				if(cols[3].equals("YES")){
        					if(dataT[j].startsWith("string"))
        						frequency += this.outputName+":=SORT(TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),-(REAL)"+cols[0]+");\n";
        					else
        						frequency += this.outputName+":=SORT(TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),-(REAL)"+cols[0]+");\n";
        				}
        				else{
        					if(dataT[j].startsWith("string"))
        						frequency += this.outputName+":=SORT(TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),-"+cols[0]+");\n";
        					else
        						frequency += this.outputName+":=SORT(TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),-"+cols[0]+");\n";
        				}
        				//frequency += "OUTPUT("+this.outputName+",THOR);\n";
        				if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+outputName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+defJobName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
//	        	    		frequency += "OUTPUT("+this.outputName+",NAMED('Frequency_"+cols[0]+"'));\n";
	        	    	}
        				
        			}
        			else{
        				if(dataT[j].startsWith("string"))
        					frequency += this.outputName+":=SORT(TABLE(Frequency1"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),-frequency);\n";
        				else
        					frequency += this.outputName+":=SORT(TABLE(Frequency2"+getNumber()+"(field = \'"+cols[0]+"\'),{"+dataT[j]+" "+cols[0]+":=value;frequency;Percent}),-frequency);\n";
        				//frequency += "OUTPUT("+this.outputName+",THOR);\n";
        				if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+outputName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+this.outputName+",,'~eda::"+defJobName+cols[0]+"::frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
//	        	    		frequency += "OUTPUT("+this.outputName+",NAMED('Frequency_"+cols[0]+"'));\n";
	        	    	}
        				
        			}
        		}
        	}
        	
        }

    return frequency;
    }
    
}
