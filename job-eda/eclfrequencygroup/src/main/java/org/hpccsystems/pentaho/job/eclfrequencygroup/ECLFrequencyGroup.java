/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclfrequencygroup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Table;
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
import org.w3c.dom.Node;
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author KeshavS
 */
public class ECLFrequencyGroup extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	private String Name = "";
	private String Sort = "";
	private String DatasetName = "";
	private String normList = "";
	private String data_type = "";
	private java.util.List people = new ArrayList();
    private String label ="";
	private String outputName ="";
	private String persist = "";
	private String defJobName = "";
	private ArrayList<String[]> GroupBy = new ArrayList<String[]>();
	
	private RecordList recordList_number = new RecordList();
	private RecordList recordList_string = new RecordList();
	private ArrayList<String> ds_num;
	private ArrayList<String> ds_string;
	

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
	
	public ArrayList<String[]> getGroupBy() {
		return GroupBy;
	}

	public void setGroupBy(ArrayList<String[]> groupBy) {
		GroupBy = groupBy;
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
    	
    	
        Result result = prevResult;
        if(result.isStopped()){
        	return result;
        }
        else{
        	//String sort = Sort;
	        String fieldStr = ""; String frequency = "";String[] norm = this.normList.split("-");String valueStr = "";String[] dataT = data_type.toLowerCase().split(",");
	        String fieldNum = "";String valueNum = "";int str = 0;int notstr = 0;String name = getName().replaceAll("\\s", "");
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
	        
	        String dataList = "";String col = "";String outstr = "";String outnum = "";String group = "";
	        for(String[] S : GroupBy){
	        	dataList += S[1] + " " +S[0]+";\n";
	        	col += "SELF."+S[0]+":=LEFT."+S[0]+",";
	        	outstr += "OutDSStr"+name+"."+S[0]+";\n";
	        	outnum += "OutDSNum"+name+"."+S[0]+";\n";
	        	group += S[0]+",";
	        }
	        
	        
	        
	        if(valueStr.length()>0){
	        	valueStr = valueStr.substring(0, valueStr.length()-1);
	        	fieldStr = fieldStr.substring(0, fieldStr.length()-1);
	        	frequency += "NumFieldStr"+name+":=RECORD\n"+dataList+"STRING field;\nSTRING value;\nEND;\n";
	        	frequency += "OutDSStr"+name+" := NORMALIZE("+this.getDatasetName()+","+str+", TRANSFORM(NumFieldStr"+name+","+col+"SELF.field:=CHOOSE(COUNTER,"+fieldStr+");SELF.value:=CHOOSE" +
	        			"(COUNTER,"+valueStr+")));\n";
	        	 frequency += "FreqRecStr"+name+":=RECORD\n"+outstr+"OutDSStr"+name+".field;\nOutDSStr"+name+".value;\nINTEGER frequency:=COUNT(GROUP);\n" +
	        		     "REAL8 Percent:=(COUNT(GROUP)/COUNT("+this.DatasetName+"))*100;\n" +
	        		     "END;\n";
	        	 
	        	 frequency += "Frequency1"+name+" := TABLE(OutDSStr"+name+",FreqRecStr"+name+","+group+"field,value,MERGE);\n";
	        }
	        if(valueNum.length()>0){
	        	valueNum = valueNum.substring(0, valueNum.length()-1);		        
		        fieldNum = fieldNum.substring(0, fieldNum.length()-1);
		        frequency += "NumField"+name+":=RECORD\n"+dataList+"STRING field;\nREAL value;\nEND;\n";
		        frequency += "OutDSNum"+name+" := NORMALIZE("+this.getDatasetName()+","+notstr+", TRANSFORM(NumField"+name+","+col+"SELF.field:=CHOOSE(COUNTER,"+fieldNum+");SELF.value:=CHOOSE" +
	        			"(COUNTER,"+valueNum+")));\n";
		        frequency += "FreqRecNum"+name+":=RECORD\n"+outnum+"OutDSNum"+name+".field;\nOutDSNum"+name+".value;\nINTEGER frequency:=COUNT(GROUP);\n" +
	       		     "REAL8 Percent:=(COUNT(GROUP)/COUNT("+this.DatasetName+"))*100;\n" +
	       		     "END;\n";
		        frequency += "Frequency2"+name+" := TABLE(OutDSNum"+name+",FreqRecNum"+name+","+group+"field,value,MERGE);\n";

	        }
	        
	        
	        for(int j = 0; j<norm.length; j++){
	        	String[] cols = norm[j].split(",");
	        	if(getSort().equals("NO") || getSort().equals("")){
	        		if(dataT[j].startsWith("string")) {
	        			if(cols.length == 5)
	        				frequency += cols[0]+"_FrequencyGroup := TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent});\n";
	        			else
	        				frequency += cols[0]+"_FrequencyGroup := TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent});\n";
	        			//frequency += "OUTPUT("+cols[0]+"_Frequency"+getNumber()+",THOR);\n";
	        			if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
	        	    		frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,NAMED('Frequency_"+cols[0]+"'));\n";
	        	    	}
	        			
	        		}
	        		else{
	        			if(cols.length == 5)
	        				frequency += cols[0]+"_FrequencyGroup :=TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent});\n";
	        			else
	        				frequency += cols[0]+"_FrequencyGroup :=TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent});\n";
	        			if(persist.equalsIgnoreCase("true")){
	        	    		if(outputName != null && !(outputName.trim().equals(""))){
	        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}else{
	        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
	        	    		}
	        	    	}
	        	    	else{
	        	    		frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,NAMED('Frequency_"+cols[0]+"'));\n";
	        	    	}
	        			
	        		}
	        	}
	        	else{
	        		if(cols[1].equals("ASC")){
	        			if(cols[2].equals("NAME")){
	        				if(cols[3].equals("YES")){
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),(REAL) value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),(REAL) value);\n";
	        					}else{
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),(REAL) value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),(REAL) value);\n";
	        					}
	        				}
	        				else{
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),value);\n";
	        					}else{
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),value);\n";
	        					}
	        				}
	        					
	        				//frequency += "OUTPUT("+cols[0]+"_Frequency"+getNumber()+",THOR);\n";
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,NAMED('Frequency_"+cols[0]+"'));\n";
		        	    	}
	        				
	        			}
	        			else{
	        				if(dataT[j].startsWith("string")){
	        					if(cols.length == 5)
	        						frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),frequency);\n";
	        					else
	        						frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),frequency);\n";
	        				}else{
	        					if(cols.length == 5)
	        						frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),frequency);\n";
	        					else
	        						frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),frequency);\n";
	        				}
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,NAMED('Frequency_"+cols[0]+"'));\n";
		        	    	}


	        			}
	        		}
	        		else if(cols[1].equals("DESC")){
	        			if(cols[2].equals("NAME")){
	        				if(cols[3].equals("YES")){
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),-(REAL) value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),-(REAL) value);\n";
	        					}else{
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),-(REAL) value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),-(REAL) value);\n";
	        					}
	        				}
	        				else{
	        					if(dataT[j].startsWith("string")){
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),-value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),-value);\n";
	        					}else{
	        						if(cols.length == 5)
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),-value);\n";
	        						else
	        							frequency += cols[0]+"_FrequencyGroup :=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),-value);\n";
	        					}
	        				}
	        				
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,NAMED('Frequency_"+cols[0]+"'));\n";
		        	    	}
	        				
	        			}
	        			else{
	        				if(dataT[j].startsWith("string")){
	        					if(cols.length == 5)
	        						frequency += cols[0]+"_FrequencyGroup:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),-frequency);\n";
	        					else
	        						frequency += cols[0]+"_FrequencyGroup:=SORT(TABLE(Frequency1"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),-frequency);\n";
	        				}else{
	        					if(cols.length == 5)
	        						frequency += cols[0]+"_FrequencyGroup:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\' AND "+cols[4].replace(cols[0], "value")+"),{"+group+""+"value;frequency;Percent}),-frequency);\n";
	        					else
	        						frequency += cols[0]+"_FrequencyGroup:=SORT(TABLE(Frequency2"+name+"(field = \'"+cols[0]+"\'),{"+group+""+"value;frequency;Percent}),-frequency);\n";
	        				}
	        				if(persist.equalsIgnoreCase("true")){
		        	    		if(outputName != null && !(outputName.trim().equals(""))){
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+outputName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}else{
		        	    			frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,,'~"+defJobName+"::"+cols[0]+"_Frequency', __compressed__, overwrite,NAMED('Frequency_"+cols[0]+"'))"+";\n";
		        	    		}
		        	    	}
		        	    	else{
		        	    		frequency += "OUTPUT("+cols[0]+"_FrequencyGroup,NAMED('Frequency_"+cols[0]+"'));\n";
		        	    	}
	        				
	        			}
	        		}
	        	}
	        	
	        }
	
	        logBasic("FrequencyGroup Job =" + frequency);//{Dataset Job} 
	        
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
        		if(S.length > 5)
        			P.setRule(S[5]);
        		else
        			P.setRule("");
        		people.add(P);
        	}
        }
    }

    public String saveGroupBy(){
    	String out = "";
    	
    	Iterator<String[]> it = GroupBy.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		String[] p = it.next();
    		out +=  p[0]+","+p[1];
            isFirst = false;
    	}
    	return out;
    }
    
    public void openGroupBy(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	GroupBy = new ArrayList<String[]>();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");         		
        		GroupBy.add(new String[]{S[0],S[1]});
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

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
                        
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "GroupBy")) != null)
                openGroupBy(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "GroupBy")));
                        
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));

            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Sort")) != null)
                setSort(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Sort")));
            
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
        retval += "		<normList><![CDATA[" + this.getnormList() + "]]></normList>" + Const.CR;
        retval += "		<dataset_name><![CDATA[" + DatasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<GroupBy eclIsGroup=\"true\"><![CDATA[" + this.saveGroupBy() + "]]></GroupBy>" + Const.CR;
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

        	if(rep.getStepAttributeString(id_jobentry, "GroupBy") != null)
            	this.openGroupBy(rep.getStepAttributeString(id_jobentry, "GroupBy")); //$NON-NLS-1$
        	
            if(rep.getStepAttributeString(id_jobentry, "Sort") != null)
            	Sort = rep.getStepAttributeString(id_jobentry, "Sort"); //$NON-NLS-1$
            
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
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Name", Name); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "normList", this.getnormList()); //$NON-NLS-1$
        	
            rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "data_type", data_type); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "GroupBy", this.saveGroupBy()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
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
    
}
