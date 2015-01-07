/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclfrequency;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.hpccsystems.ecljobentrybase.ECLJobEntry;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author KeshavS
 */
public class ECLFrequency extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	private String Name = "";
	private String Sort = "";
	private String DatasetName = "";
	private String normList = "";
	private String data_type = "";
	private java.util.List people = new ArrayList();
	private String flag = "";
    private String label ="";
	private String outputName ="";
	private String persist = "";
	private String defJobName = "";
	private RecordList recordList_number = new RecordList();
	private RecordList recordList_string = new RecordList();
	private ArrayList<String> ds_num;
	private ArrayList<String> ds_string;
	private String histogram = "";
	private String filePath;
	private String Cumulative;
	
	public String getCumulative() {
		return Cumulative;
	}

	public void setCumulative(String cumulative) {
		Cumulative = cumulative;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getHistogram() {
		return histogram;
	}

	public void setHistogram(String histogram) {
		this.histogram = histogram;
	}

	public ArrayList<String> getDs_num() {
		return ds_num;
	}

	public void setDs_num(ArrayList<String> ds_num) {
		this.ds_num = ds_num;
	}

	public ArrayList<String> getDs_string() {
		return ds_string;
	}

	public void setDs_string(ArrayList<String> ds_string) {
		this.ds_string = ds_string;
	}

	public RecordList getRecordList_number() {
		return recordList_number;
	}

	public void setRecordList_number(RecordList recordList_number) {
		this.recordList_number = recordList_number;
	}

	public RecordList getRecordList_string() {
		return recordList_string;
	}

	public void setRecordList_string(RecordList recordList_string) {
		this.recordList_string = recordList_string;
	}

	public String getDefJobName() {
		return defJobName;
	}

	public void setDefJobName(String defJobName) {
		this.defJobName = defJobName;
	}
	
	public String getPersistOutputChecked() {
		return persist;
	}

	public void setPersistOutputChecked(String persist) {
		this.persist = persist;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getOutputName() {
		return outputName;
	}

	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}
    
	public void setPeople(java.util.List people){
		this.people = people;
	}
	
	public java.util.List getPeople(){
		return people;
	}

	public String getName(){
		return Name;
	}
    
    public String getflag() {
        return flag;
    }

    public void setflag(String flag) {
        this.flag = flag;
    }
	public void setName(String Name){
		this.Name = Name;
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
    
    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
    	
    	
    	Result result = modifyResults(prevResult);
        if(result.isStopped()){
        	return result;
        }
        else{
        	logBasic(filePath);
        	for(String S : ds_string)
        		logBasic(S+"from string");
        	for(String S : ds_num)
        		logBasic(S);
        	//String sort = Sort;
	        String fieldStr = ""; String frequency = "";String[] norm = this.normList.split("-");String valueStr = "";String[] dataT = data_type.toLowerCase().split(",");
	        String fieldNum = "";String valueNum = "";int str = 0;int notstr = 0;String name = getName().replaceAll("\\s", "");String hist = "";
	        boolean cumulative;
	        if(getCumulative().equalsIgnoreCase("Yes"))
	        	cumulative = true;
	        else
	        	cumulative = false;
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
	        if(getHistogram().equalsIgnoreCase("Yes")){
	        	hist = "ColumnChart_";
	        	try {
	        		String[] path = getFilePath().split("\"");
	        		logBasic(path[1].replaceAll("manifest.xml", "")); 	        		
					Change("ColumnChart",path[1].replaceAll("manifest.xml", ""));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TransformerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        else{
	        	hist = "";
	        }
	        if(valueStr.length()>0){
	        	valueStr = valueStr.substring(0, valueStr.length()-1);
	        	fieldStr = fieldStr.substring(0, fieldStr.length()-1);
	        	frequency += "NumFieldStr"+name+":=RECORD\nSTRING field;\nSTRING value;\nEND;\n";
	        	frequency += "OutDSStr"+name+" := NORMALIZE("+this.getDatasetName()+","+str+", TRANSFORM(NumFieldStr"+name+",SELF.field:=CHOOSE(COUNTER,"+fieldStr+");SELF.value:=CHOOSE" +
	        			"(COUNTER,"+valueStr+")));\n";
	        	 frequency += "FreqRecStr"+name+":=RECORD\nOutDSStr"+name+".field;\nOutDSStr"+name+".value;\nINTEGER frequency:=COUNT(GROUP);\n" +
	        		     "REAL8 Percent:=(COUNT(GROUP)/COUNT("+this.DatasetName+"))*100;REAL8 Cumulative:=0;\n" +
	        		     "END;\n";
	        	 
	        	 frequency += "Frequency1"+name+" := TABLE(OutDSStr"+name+",FreqRecStr"+name+",field,value,MERGE);\n";
	        }
	        if(valueNum.length()>0){
	        	valueNum = valueNum.substring(0, valueNum.length()-1);		        
		        fieldNum = fieldNum.substring(0, fieldNum.length()-1);
		        frequency += "NumField"+name+":=RECORD\nSTRING field;\nREAL value;\nEND;\n";
		        frequency += "OutDSNum"+name+" := NORMALIZE("+this.getDatasetName()+","+notstr+", TRANSFORM(NumField"+name+",SELF.field:=CHOOSE(COUNTER,"+fieldNum+");SELF.value:=CHOOSE" +
	        			"(COUNTER,"+valueNum+")));\n";
		        frequency += "FreqRecNum"+name+":=RECORD\nOutDSNum"+name+".field;\nOutDSNum"+name+".value;\nINTEGER frequency:=COUNT(GROUP);\n" +
	       		     "REAL8 Percent:=(COUNT(GROUP)/COUNT("+this.DatasetName+"))*100;REAL8 Cumulative:=0;\n" +
	       		     "END;\n";
		        frequency += "Frequency2"+name+" := TABLE(OutDSNum"+name+",FreqRecNum"+name+",field,value,MERGE);\n";

	        }
	        
	        
	        for(int j = 0; j<norm.length; j++){
	        	String[] cols = norm[j].split(",");
	        	if(getSort().equals("NO") || getSort().equals("")){
	        		if(dataT[j].startsWith("string")) {
	        			if(cols.length == 5){
	        				if(cumulative){
	        					frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(Frequency1"+name+" L, Frequency1"+name+" R) := TRANSFORM\n";
	        					frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	        					frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(Frequency1"+name+"(field='"+cols[0]+"'), Trans"+cols[0]+"(LEFT,RIGHT));\n";
	        					frequency += cols[0]+"_Frequency:= TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        				}
	        				else
	        					frequency += cols[0]+"_Frequency:= TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent});\n";//"+dataT[j]+" "+cols[0]+":=
	        			}
	        			else{
	        				if(cumulative){
	        					frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(Frequency1"+name+" L, Frequency1"+name+" R) := TRANSFORM\n";
	        					frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	        					frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(Frequency1"+name+"(field='"+cols[0]+"'), Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					
	        					frequency += cols[0]+"_Frequency:= TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        				}
	        				else
	        					frequency += cols[0]+"_Frequency:= TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent});\n";
	        			}	        			
	        			if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
	        	    		frequency += "OUTPUT("+cols[0]+"_Frequency,NAMED('"+hist+"Frequency_"+cols[0]+"'));\n";
	        	    		
	        	    	}
	        			
	        		}
	        		else{
	        			if(cols.length == 5){
	        				if(cumulative){
		        				frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(Frequency2"+name+" L, Frequency2"+name+" R) := TRANSFORM\n";
	        					frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	        					frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(Frequency2"+name+"(field='"+cols[0]+"'), Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        					
		        				frequency += cols[0]+"_Frequency:= TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND"+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";//"+dataT[j]+" "+cols[0]+":=
	        				}
	        				else
	        					frequency += cols[0]+"_Frequency:= TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND"+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent});\n";//"+dataT[j]+" "+cols[0]+":=
	        			}
	        			else{
	        				if(cumulative){
	        					frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(Frequency2"+name+" L, Frequency2"+name+" R) := TRANSFORM\n";
	        					frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	        					frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(Frequency2"+name+"(field='"+cols[0]+"'), Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        					
	        					frequency += cols[0]+"_Frequency:= TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        				}
	        				else
	        					frequency += cols[0]+"_Frequency:= TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent});\n";
	        			}
	        			if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
	        	    		frequency += "OUTPUT("+cols[0]+"_Frequency,NAMED('"+hist+"Frequency_"+cols[0]+"'));\n";
	        	    	}
	        			
	        		}
	        	}
	        	else{
	        		if(cols[1].equals("ASC")){
	        			if(cols[2].equals("NAME")){
	        				if(cols[3].equals("YES")){
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),(REAL) value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),(REAL) value);\n";
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),(REAL) value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),(REAL) value);\n";//"+dataT[j]+" "+cols[0]+":=
	        						}
	        					}
	        					else{	        						
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),(REAL) value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),(REAL) value);\n";//"+dataT[j]+" "+cols[0]+":=
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),(REAL) value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),(REAL) value);\n";//"+dataT[j]+" "+cols[0]+":=
	        						}
	        					}
	        				}
	        				else{
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),value);\n";
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),(REAL) value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),value);\n";
	        						}
	        					}
	        					else{
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),value);\n";
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),value);\n";
	        						}
	        					}
	        				}	
	        				//frequency += "OUTPUT("+cols[0]+"_Frequency"+getNumber()+",THOR);\n";
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_Frequency,NAMED('"+hist+"Frequency_"+cols[0]+"'));\n";
		        	    	}
	        				
	        			}
	        			else{
	        				if(dataT[j].startsWith("string")){
	        					if(cols.length == 5){
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),frequency);\n";
        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),frequency);\n";
	        					}
	        					else{
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),frequency);\n";
        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),frequency);\n";
	        					}
	        				}else{
	        					if(cols.length == 5){
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),frequency);\n";
        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),frequency);\n";	
	        					}
	        					else{
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),frequency);\n";
        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),frequency);\n";
	        					}
	        				}
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_Frequency,NAMED('"+hist+"Frequency_"+cols[0]+"'));\n";
		        	    	}


	        			}
	        		}
	        		else if(cols[1].equals("DESC")){
	        			if(cols[2].equals("NAME")){
	        				if(cols[3].equals("YES")){
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),-(REAL) value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),-(REAL) value);\n";	        							
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),-(REAL) value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),-(REAL) value);\n";
	        						}
	        					}else{
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),-(REAL) value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),-(REAL) value);\n";
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),-(REAL) value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else	        								
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),-(REAL) value);\n";
	        						}
	        					}
	        				}
	        				else{
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),-value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),-value);\n";
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),-value);\n";
	        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),-value);\n";
	        						}
	        					}else{
	        						if(cols.length == 5){
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),-value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),-value);\n";
	        						}
	        						else{
	        							if(cumulative){
	        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),-value);\n";
	        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
	        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
	    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
	    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
	        							}
	        							else
	        								frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),-value);\n";
	        						}
	        					}
	        				}
	        				//frequency += "OUTPUT("+cols[0]+"_Frequency"+getNumber()+",THOR);\n";
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_Frequency,NAMED('"+hist+"Frequency_"+cols[0]+"'));\n";
		        	    	}
	        				
	        			}
	        			else{
	        				if(dataT[j].startsWith("string")){
	        					if(cols.length == 5){
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),-frequency);\n";
        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),-frequency);\n";
	        					}
	        					else{
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency1"+name+"(field='"+cols[0]+"'),-frequency);\n";
        								frequency += "FreqRecStr"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\'),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),-frequency);\n";
	        					}
	        				}else{
	        					if(cols.length == 5){
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),-frequency);\n";
        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{value;frequency;Percent}),-frequency);\n";
	        					}
	        					else{
	        						if(cumulative){
        								frequency += "sort"+cols[0]+":=SORT(Frequency2"+name+"(field='"+cols[0]+"'),-frequency);\n";
        								frequency += "FreqRecNum"+name+" Trans"+cols[0]+"(sort"+cols[0]+" L, sort"+cols[0]+" R) := TRANSFORM\n";
        								frequency += "SELF.Cumulative := L.Cumulative + R.Percent;\n	SELF := R;\nEND;\n";
    	        						frequency += "Cumulative_Freq"+cols[0]+":=ITERATE(sort"+cols[0]+", Trans"+cols[0]+"(LEFT,RIGHT));\n";	        					        						    	        					
    	        						frequency += cols[0]+"_Frequency:=TABLE(Cumulative_Freq"+cols[0]+"(field = \'"+cols[0]+"\' ),{value;frequency;Percent;Cumulative});\n";
        							}
	        						else
	        							frequency += cols[0]+"_Frequency:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{value;frequency;Percent}),-frequency);\n";
	        					}
	        				}
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_Frequency,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('"+hist+"Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_Frequency,NAMED('"+hist+"Frequency_"+cols[0]+"'));\n";
		        	    	}
	        				
	        			}
	        		}
	        	}
	        	
	        }
	
	        logBasic("Frequency Job =" + frequency);//{Dataset Job} 
	        
	        result.setResult(true); 
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, frequency);
	        
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	        result.setLogText("ECLFrequency executed, ECL code added");
        }
        return result;
    }
    
    public String savePeople(){
    	String out = "";
    	
    	Iterator it = people.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getSort().toString()+","+p.getColumns().toString()+","+p.getType()+","+p.getSortNumeric().toString()+","+p.getRule();
            isFirst = false;
    	}
    	return out;
    }

    public void openPeople(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	people = new ArrayList();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(","); 
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		P.setSort(Integer.parseInt(S[1]));
        		P.setColumns(Integer.parseInt(S[2]));
        		P.setType(S[3]);
        		P.setSortNumeric(Integer.parseInt(S[4]));
        		if(S.length>5)
        			P.setRule(S[5]); 
        		else
        			P.setRule("");
        		people.add(P);
        	}
        }
    }
    
    public String saveRecordList_number(){
        String out = "";
        ArrayList list = recordList_number.getRecords();
        Iterator<RecordBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
    public void openRecordList_number(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            recordList_number = new RecordList();
            //System.out.println("Open Record List");
            for(int i =0; i<len; i++){
                //System.out.println("++++++++++++" + strLine[i]);
                //this.recordDef.addRecord(new RecordBO(strLine[i]));
                RecordBO rb = new RecordBO(strLine[i]);
                //System.out.println(rb.getColumnName());
                recordList_number.addRecordBO(rb);
            }
        }
    }

    public String saveRecordList_string(){
        String out = "";
        ArrayList list = recordList_string.getRecords();
        Iterator<RecordBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
    public void openRecordList_string(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            recordList_string = new RecordList();
            //System.out.println("Open Record List");
            for(int i =0; i<len; i++){
                //System.out.println("++++++++++++" + strLine[i]);
                //this.recordDef.addRecord(new RecordBO(strLine[i]));
                RecordBO rb = new RecordBO(strLine[i]);
                //System.out.println(rb.getColumnName());
                recordList_string.addRecordBO(rb);
            }
        }
    }
    
/*    public String saveArrayList(ArrayList<String> ar){
        String out = "";
        //ArrayList list = ds_num;
        Iterator<String> itr = ar.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next();
            isFirst = false;
        }
        return out;
    }
    
    public void openArrayList_number(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
        	ds_num = new ArrayList<String>();
            for(int i =0; i<len; i++){
                ds_num.add(strLine[i]);
            }
        }
    }
    
    public void openArrayList_string(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
        	ds_string = new ArrayList<String>(); 
            for(int i =0; i<len; i++){
                ds_num.add(strLine[i]);
            }
        }
    }
*/
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "flag")) != null)
                setflag(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "flag")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));

            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Sort")) != null)
                setSort(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Sort")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Histogram")) != null)
                setHistogram(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Histogram")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Cumulative")) != null)
                setCumulative(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Cumulative")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "FilePath")) != null)
                setFilePath(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "FilePath")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "normList")) != null)
                setnormList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "normList")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")) != null)
                openPeople(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "data_type")) != null)
            	data_type = (XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "data_type")));
           
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")) != null)
                setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")) != null)
                setLabel(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")) != null)
                setPersistOutputChecked(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")));
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outTables")) != null)
              //  openOutTables(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outTables")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")) != null)
                setDefJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList_number")) != null)
                openRecordList_number(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList_number")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList_string")) != null)
                openRecordList_string(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList_string")));
            
            String[] S = normList.split("-");
            ds_string = new ArrayList<String>();
            ds_num = new ArrayList<String>();
        	for(int i = 0; i<S.length; i++){
        		String[] s = S[i].split(",");
        		if(data_type.toLowerCase().split(",")[i].contains("string"))
        			ds_string.add(s[0]+"_Frequency");        			        		
        		else
        			ds_num.add(s[0]+"_Frequency");
        	}
            /*if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_string")) != null){
            	ds_string = new ArrayList<String>();int i = 0;
            	while(i < XMLHandler.getNodeElements(node).length){
            		String S = XMLHandler.getNodeElements(node)[i];
            		ds_string.add(S);
            	}
            }
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_number")) != null){
            	ds_num = new ArrayList<String>();int i = 0;
            	while(i < XMLHandler.getNodeElements(node).length){
            		String S = XMLHandler.getNodeElements(node)[i];
            		ds_num.add(S);
            	}
            }*/
            	
/*            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_number")) != null){
                
            }
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_string")) != null)
                openArrayList_string(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outpur_string")));
*/           
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "		<name><![CDATA[" + Name + "]]></name>" + Const.CR;
        retval += "		<people><![CDATA[" + this.savePeople() + "]]></people>" + Const.CR;
        retval += "		<data_type><![CDATA[" + data_type + "]]></data_type>" + Const.CR;
        retval += "		<Sort ><![CDATA[" + Sort + "]]></Sort>" + Const.CR;
        retval += "		<Histogram ><![CDATA[" + histogram + "]]></Histogram>" + Const.CR;
        retval += "		<Cumulative><![CDATA[" + Cumulative + "]]></Cumulative>" + Const.CR;
        retval += "		<FilePath ><![CDATA[" + filePath + "]]></FilePath>" + Const.CR;
        retval += "		<normList><![CDATA[" + this.getnormList() + "]]></normList>" + Const.CR;
        retval += "		<dataset_name><![CDATA[" + DatasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<flag><![CDATA[" + flag + "]]></flag>" + Const.CR;
        retval += "		<label><![CDATA[" + label + "]]></label>" + Const.CR;
        retval += "		<output_name><![CDATA[" + outputName + "]]></output_name>" + Const.CR;
        retval += "		<persist_Output_Checked><![CDATA[" + persist + "]]></persist_Output_Checked>" + Const.CR;
        retval += "		<defJobName><![CDATA[" + defJobName + "]]></defJobName>" + Const.CR;
        retval += "		<recordList_number><![CDATA[" + this.saveRecordList_number() + "]]></recordList_number>" + Const.CR;
        retval += "		<recordList_string><![CDATA[" + this.saveRecordList_string() + "]]></recordList_string>" + Const.CR;
        if(getDs_num() != null){
        	for(String S : getDs_num())
        		retval += "		<output_number eclIsDef=\"true\" eclType=\"dataset\"><![CDATA[" + S + "]]></output_number>" + Const.CR;
        }
        if(getDs_string() != null){
        	for(String S : getDs_string())
        		retval += "		<output_string eclIsDef=\"true\" eclType=\"dataset\"><![CDATA[" + S + "]]></output_string>" + Const.CR;
        }
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	if(rep.getStepAttributeString(id_jobentry, "Name") != null)
                Name = rep.getStepAttributeString(id_jobentry, "Name"); //$NON-NLS-1$
        	
        	if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                DatasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$

        	if(rep.getStepAttributeString(id_jobentry, "flag") != null)
            	flag = rep.getStepAttributeString(id_jobentry, "flag"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "Sort") != null)
            	Sort = rep.getStepAttributeString(id_jobentry, "Sort"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "Histogram") != null)
            	histogram = rep.getStepAttributeString(id_jobentry, "Histogram"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "Cumulative") != null)
            	Cumulative = rep.getStepAttributeString(id_jobentry, "Cumulative"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "FilePath") != null)
            	filePath = rep.getStepAttributeString(id_jobentry, "FilePath"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "normList") != null)
                this.setnormList(rep.getStepAttributeString(id_jobentry, "normList")); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "people") != null)
                this.openPeople(rep.getStepAttributeString(id_jobentry, "people")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "data_type") != null)
            	data_type = (rep.getStepAttributeString(id_jobentry, "data_type")); //$NON-NLS-1$
            //if(rep.getStepAttributeString(id_jobentry, "outTables") != null)
                //this.openOutTables(rep.getStepAttributeString(id_jobentry, "outTables")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "outputName") != null)
            	outputName = rep.getStepAttributeString(id_jobentry, "outputName"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "label") != null)
            	label = rep.getStepAttributeString(id_jobentry, "label"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "persist_Output_Checked") != null)
            	persist = rep.getStepAttributeString(id_jobentry, "persist_Output_Checked"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "defJobName") != null)
            	defJobName = rep.getStepAttributeString(id_jobentry, "defJobName"); //$NON-NLS-1$

            if(rep.getStepAttributeString(id_jobentry, "recordList_number") != null)
                this.openRecordList_number(rep.getStepAttributeString(id_jobentry, "recordList_number")); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "recordList_string") != null)
                this.openRecordList_string(rep.getStepAttributeString(id_jobentry, "recordList_string")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "datasetName", DatasetName); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Sort", Sort); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Histogram", histogram); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Cumulative", Cumulative); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Name", Name); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "normList", this.getnormList()); //$NON-NLS-1$
        	
            rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "data_type", data_type); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "flag", flag); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
        	rep.saveStepAttribute(id_job, getObjectId(), "FilePath", filePath);
        	rep.saveStepAttribute(id_job, getObjectId(), "recordList_number", this.saveRecordList_number()); //$NON-NLS-1$
        	rep.saveStepAttribute(id_job, getObjectId(), "recordList_string", this.saveRecordList_string()); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    public boolean evaluates() {
        return true;
    }

    public boolean isUnconditional() {
        return true;
    }
    public void Change(String Chart, String path) throws Exception,FileNotFoundException, TransformerException {
        File xmlFile = new File(path+Chart.toLowerCase()+".xslt");//"D:\\Users\\Public\\Documents\\HPCC Systems\\ECL\\MY\\visualizations\\google_charts\\files\\"

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        NodeList nList = document.getElementsByTagName("xsl:template");
        Node n = nList.item(0);
        if (n.getNodeType() == Node.ELEMENT_NODE) {
        	Element eElement1 = (Element) n;
        	
    		System.out.println(eElement1.getElementsByTagName("div").item(0).getAttributes().getNamedItem("style").getNodeValue()); 
    		eElement1.getElementsByTagName("div").item(0).getAttributes().getNamedItem("style").setNodeValue("height:600px; width:1200px;");
        }
    	Node nNode = nList.item(1);         	
    	if (nNode.getNodeType() == Node.ELEMENT_NODE) {    			
    		Element eElement = (Element) nNode;
    		String S =");\n"+ 
    				"			var zoomed = false;\n			var MAX = 20;\n			var options = {\n"+
    				"						animation: {duration: 1000,easing: 'in'},\n"+
    				"						tooltip: { textStyle: { fontName: 'Arial', fontSize: 18, bold:false }},\n"+
    				"						hAxis: {viewWindow : {min : 0, max:4},title: 'Histogram', titleTextStyle:{color: 'Red', fontName:'Arial', fontSize:18, italic:0}},\n"+
    				"						colors: [\"red\",\"yellow\"]\n"+						
    				"			};\n"+
    				"			function draw";	
        	    eElement.getElementsByTagName("xsl:text").item(1).setTextContent(S);
        		
        		//System.out.println(eElement.getElementsByTagName("xsl:text").item(1).getTextContent());
    			
    	}

    	Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        

        
        StreamResult result = new StreamResult(new PrintWriter(new FileOutputStream(xmlFile, false)));
        DOMSource source = new DOMSource(document);
        transformer.transform(source, result);
    }

}
