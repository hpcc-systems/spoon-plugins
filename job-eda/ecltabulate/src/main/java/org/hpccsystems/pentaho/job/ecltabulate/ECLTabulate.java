/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecltabulate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;

/**
 *
 * @author KeshavS
 */
public class ECLTabulate extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	


	private String DatasetName = "";
	private ArrayList<Player> people = new ArrayList<Player>();
	private ArrayList<Player> rows = new ArrayList<Player>();
	private ArrayList<Player> cols = new ArrayList<Player>();
	private ArrayList<Player> layers = new ArrayList<Player>();
	private ArrayList<String> settings = new ArrayList<String>();
	private String[] tables = null;
	private String number;
	
	private String label ="";
	private String outputName ="";
	private String persist = "";
	private String defJobName = "";
	public static int ct = 0;
	private ArrayList<RecordList> recordList;
	
	
	public ArrayList<RecordList> getRecordList() {
		return recordList;
	}

	public void setRecordList(ArrayList<RecordList> recordList) {
		this.recordList = recordList;
	}

	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

	public String[] getTables() {
		return tables;
	}

	public void setTables(String[] tables) {
		this.tables = tables;
	}

	public ArrayList<String> getSettings() {
		return settings;
	}

	public void setSettings(ArrayList<String> settings) {
		this.settings = settings;
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
	
	public void setPeople(ArrayList<Player> people){
		this.people = people;
	}
	
	public ArrayList<Player> getPeople(){
		return people;
	}
	
	public void setRows(ArrayList<Player> rows){
		this.rows = rows;
	}
	
	public ArrayList<Player> getRows(){
		return rows;
	}
	
	public void setCols(ArrayList<Player> cols){
		this.cols = cols;
	}
	
	public ArrayList<Player> getCols(){
		return cols;
	}
	
	public void setLayers(ArrayList<Player> layers){
		this.layers = layers;
	}
	
	public ArrayList<Player> getLayers(){
		return layers;
	}
	
	public String getDatasetName(){
		return DatasetName;
	}
    
	public void setDatasetName(String DatasetName){
		this.DatasetName = DatasetName;
	}
	
    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
    	
    	
        Result result = prevResult;
        if(result.isStopped()){
        	return result;
        }
        else{
        	String ecl = "";int cnt = 0;String layer = "";String group = "";String join =""; 
        	boolean parent = settings.contains("parent");
        	boolean total = settings.contains("total");
        	boolean zxc = true;
        	//String layerRule = "";
        	String MainRule="";
        	for(Iterator<Player> itL = layers.iterator(); itL.hasNext();){
        		Player sl = (Player) itL.next();
        		String Sl = sl.getFirstName();
    			//boolean sdsdfggg = Sl.equals("TestingNull");
    			String Sc = sl.getFirstName();
    			String ruleL = sl.getRule().replace(Sc, getDatasetName()+"."+Sc);
    			
    			if(!ruleL.equals("")) {
        			MainRule = MainRule + " AND "+ruleL;
        			if (zxc) {
    					zxc=false;
    					MainRule = ruleL;
    				}
    			}
    			else 
    				MainRule = MainRule+"No Rule";
        		layer += this.DatasetName+"."+Sl+";\n";
        		if(layers.indexOf(sl) == layers.size()-1 ){
        			join += "LEFT."+Sl+" = RIGHT."+Sl;
        			group += Sl;        		
        		}
        		else{
        			join += "LEFT."+Sl+" = RIGHT."+Sl+" AND ";
        			group += Sl+",";        			
        		}
        	}
        	
        	for(Iterator<Player> itR = rows.iterator(); itR.hasNext();){
        		String transform = "";
        		Player sr =  (Player) itR.next();
        		String Sr = sr.getFirstName();
    			String Scrow = sr.getFirstName();
    			String ruleRow = sr.getRule().replace(Scrow, getDatasetName()+"."+Scrow);
    			String MRule = MainRule;
    			if(!ruleRow.equals("")) {
    				MRule = MRule +" AND "+ruleRow;
            		if(!MRule.equals(""))
            			MRule = ruleRow;
            	}
        		int percent = 0;
        		if(layer!="")
        			ecl += "MyRec"+cnt+":=RECORD\nUNSIGNED id := 1;\n"+layer+this.getDatasetName()+"."+Sr+";\n";
        		else
        			ecl += "MyRec"+cnt+":=RECORD\nUNSIGNED id := 1;\n"+this.getDatasetName()+"."+Sr+";\n";
        		for(Iterator<Player> itC = cols.iterator(); itC.hasNext();){
        			Player sc = (Player) itC.next();
        			String Sc = sc.getFirstName();
        			String smallRule = sc.getRule().replace(Sc, getDatasetName()+"."+Sc);
        			String rule = MRule;
        			if(!smallRule.equals("")) {
        				
        				if(rule.equals("")) {
        					rule = smallRule;
        				}
        				else{
        					rule = rule + " AND " +smallRule;
        				}
        			}
        			if(!rule.equals("")) {
        				
        			}
        			logBasic("rule : "+rule);
        			int op = sc.getOP();
        			if(!settings.isEmpty())
        				percent = 1;
        			
        			switch(op){
        			case 0: 
        				if(!rule.equals(""))
        					ecl += "cnt_"+Sc+" := COUNT(GROUP,"+rule+");\n";
        				else
        					ecl += "cnt_"+Sc+" := COUNT(GROUP);\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			case 1:
        				if(!rule.equals(""))
        					ecl += "add_"+Sc+" := SUM(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
        				else
        					ecl += "add_"+Sc+" := SUM(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			case 2:
        				if(!rule.equals(""))
        					ecl += "Mean_"+Sc+" := AVE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
        				else
        					ecl += "Mean_"+Sc+" := AVE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			case 3:
        				if(!rule.equals(""))
        					ecl += "Var_"+Sc+" := VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
        				else
        					ecl += "Var_"+Sc+" := VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			case 4:
        				if(!rule.equals(""))
        					ecl += "Sd_"+Sc+" := SQRT(VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+"));\n";
        				else
        					ecl += "Sd_"+Sc+" := SQRT(VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+"));\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			case 5:
        				if(!rule.equals(""))
        					ecl += "maximum_"+Sc+":= MAX(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
        				else
        					ecl += "maximum_"+Sc+":= MAX(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			case 6:
        				if(!rule.equals(""))
        					ecl += "minimum_"+Sc+" := MIN(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
        				else
        					ecl += "minimum_"+Sc+" := MIN(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
        				if(parent)
        					ecl += "DECIMAL5_2 "+Sc+"_Parent_Percent := 0;\n";
        				if(total)
        					ecl += "DECIMAL5_2 "+Sc+"_Total_Percent := 0;\n";
        				break;
        			}        			
        		}
        		ecl += "END;\n";
    			if(group != "")
    				ecl += "MyTab"+cnt+":=TABLE("+this.getDatasetName()+",MyRec"+cnt+","+group+","+Sr+");\n";
    			else
    				ecl += "MyTab"+cnt+":=TABLE("+this.getDatasetName()+",MyRec"+cnt+","+Sr+");\n";
    			
    			
    			
        		if(settings.contains("parent") || settings.contains("total")){
        			String joinT = "";
        			
        			if(parent){
        				transform = "";
        				if(layer!="")
            				ecl += "MyNewRecParent"+cnt+" := RECORD\n"+layer;
            			else
            				ecl += "MyNewRecParent"+cnt+" := RECORD\n"+this.getDatasetName()+"."+Sr+";\n";
            			for(Iterator<Player> itC = cols.iterator(); itC.hasNext();){
                			Player sc = (Player) itC.next();
                			String Sc = sc.getFirstName();
                			String rule = sc.getRule().replace(Sc, getDatasetName()+"."+Sc);
                			int op = sc.getOP();
                			switch(op){
                			case 0: 
                				if(!rule.equals(""))
                					ecl += "cnt_layers_"+Sc+" := COUNT(GROUP,"+rule+");\n";
                				else
                					ecl += "cnt_layers_"+Sc+" := COUNT(GROUP);\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.cnt_"+Sc+"/L.cnt_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			case 1:
                				if(!rule.equals(""))
                					ecl += "add_layers_"+Sc+" := SUM(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "add_layers_"+Sc+" := SUM(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.add_"+Sc+"/L.add_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			case 2:
                				if(!rule.equals(""))
                					ecl += "Mean_layers_"+Sc+" := AVE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "Mean_layers_"+Sc+" := AVE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.Mean_"+Sc+"/L.Mean_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			case 3:
                				if(!rule.equals(""))
                					ecl += "Var_layers_"+Sc+" := VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "Var_layers_"+Sc+" := VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.Var_"+Sc+"/L.Var_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			case 4:
                				if(!rule.equals(""))
                					ecl += "Sd_layers_"+Sc+" := SQRT(VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+"));\n";
                				else
                					ecl += "Sd_layers_"+Sc+" := SQRT(VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+"));\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.Sd_"+Sc+"/L.Sd_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			case 5:
                				if(!rule.equals(""))
                					ecl += "maximum_layers_"+Sc+" := MAX(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "maximum_layers_"+Sc+" := MAX(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.maximum_"+Sc+"/L.maximum_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			case 6:
                				if(!rule.equals(""))
                					ecl += "minimum_layers_"+Sc+" := MIN(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "minimum_layers_"+Sc+" := MIN(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Parent_Percent := (100*(L.minimum_"+Sc+"/L.minimum_layers_"+Sc+"));\n";
                				joinT += "SELF."+Sc+"_Parent_Percent := R."+Sc+"_Parent_Percent;\n";
                				break;
                			}
            			}
            			ecl += "END;\n";
            			if(layer!=""){
            				ecl += "ParentTable"+cnt+" := TABLE("+getDatasetName()+",MyNewRecParent"+cnt+","+group+");\n";
            				ecl += "withtotParent"+cnt+" := JOIN(MyTab"+cnt+",ParentTable"+cnt+","+join+");\n";
            			}
            			else{
            				ecl += "ParentTable"+cnt+" := TABLE("+getDatasetName()+",MyNewRecParent"+cnt+","+getDatasetName()+"."+Sr+");\n";
            				ecl += "withtotParent"+cnt+" := JOIN(MyTab"+cnt+",ParentTable"+cnt+",LEFT."+Sr+" = RIGHT."+Sr+");\n";            				
            			}
            			ecl += "MyRec"+cnt+" trans1"+cnt+"(withtotParent"+cnt+" L,INTEGER R) := TRANSFORM\n"+transform+"SELF := L;\nEND;\n";
            			ecl += "ParentTab"+cnt+" := PROJECT(withtotParent"+cnt+",trans1"+cnt+"(LEFT,COUNTER));\n";            			
            			
        			}
        			if(total){
        				transform = "";
            			ecl += "MyNewRecTotal"+cnt+" := RECORD\nUNSIGNED id := 1;\n";
            			for(Iterator<Player> itC = cols.iterator(); itC.hasNext();){
                			Player sc = (Player) itC.next();
                			String Sc = sc.getFirstName();
                			String rule = sc.getRule().replace(Sc, getDatasetName()+"."+Sc);
                			int op = sc.getOP();
                			switch(op){
                			case 0: 
                				if(rule.equals(""))
                					ecl += "cnt_layers_"+Sc+" := COUNT(GROUP,"+rule+");\n";
                				else
                					ecl += "cnt_layers_"+Sc+" := COUNT(GROUP);\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.cnt_"+Sc+"/L.cnt_layers_"+Sc+"));\n";
                				break;
                			case 1:
                				if(!rule.equals(""))
                					ecl += "add_layers_"+Sc+" := SUM(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "add_layers_"+Sc+" := SUM(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.add_"+Sc+"/L.add_layers_"+Sc+"));\n";
                				break;
                			case 2:
                				if(!rule.equals(""))
                					ecl += "Mean_layers_"+Sc+" := AVE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "Mean_layers_"+Sc+" := AVE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.Mean_"+Sc+"/L.Mean_layers_"+Sc+"));\n";
                				break;
                			case 3:
                				if(!rule.equals(""))
                					ecl += "Var_layers_"+Sc+" := VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "Var_layers_"+Sc+" := VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.Var_"+Sc+"/L.Var_layers_"+Sc+"));\n";
                				break;
                			case 4:
                				if(!rule.equals(""))
                					ecl += "Sd_layers_"+Sc+" := SQRT(VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+"));\n";
                				else
                					ecl += "Sd_layers_"+Sc+" := SQRT(VARIANCE(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+"));\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.Sd_"+Sc+"/L.Sd_layers_"+Sc+"));\n";
                				break;
                			case 5:
                				if(!rule.equals(""))
                					ecl += "maximum_layers_"+Sc+" := MAX(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "maximum_layers_"+Sc+" := MAX(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.maximum_"+Sc+"/L.maximum_layers_"+Sc+"));\n";
                				break;
                			case 6:
                				if(!rule.equals(""))
                					ecl += "minimum_layers_"+Sc+" := MIN(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+","+rule+");\n";
                				else
                					ecl += "minimum_layers_"+Sc+" := MIN(GROUP,(REAL)"+this.getDatasetName()+"."+Sc+");\n";
                				transform += "SELF."+Sc+"_Total_Percent := (100*(L.minimum_"+Sc+"/L.minimum_layers_"+Sc+"));\n";         				
                				break;
                			}
            			}
            			ecl += "END;\n";
            			
            			ecl += "TotalTable"+cnt+" := TABLE("+getDatasetName()+",MyNewRecTotal"+cnt+");";
            			ecl += "withtotTotal"+cnt+" := JOIN(MyTab"+cnt+",TotalTable"+cnt+",LEFT.id = RIGHT.id);\n";
            			ecl += "MyRec"+cnt+" trans"+cnt+"(withtotTotal"+cnt+" L,INTEGER C) := TRANSFORM\n" +transform+        					
            					"	SELF := L;\n" +
            					"END;\n";
            			ecl += "TotalTab"+cnt+" := PROJECT(withtotTotal"+cnt+",trans"+cnt+"(LEFT,COUNTER));\n";
        			}
        			
        			if(parent && total){
        				ecl += "MyRec"+cnt+" Trans2"+cnt+"(TotalTab"+cnt+" L, ParentTab"+cnt+" R) := TRANSFORM\n" +joinT+
        						"	SELF := L;\n" +
        						"END;\n";
        				if(layer != ""){
        					ecl += "Tabulate_"+Sr+"_"+number+" := JOIN(TotalTab"+cnt+",ParentTab"+cnt+","+join+" AND LEFT."+Sr+" = RIGHT."+Sr+",Trans2"+cnt+"(LEFT,RIGHT));\n";
        				}
        				else{
        					ecl += "Tabulate_"+Sr+"_"+number+" := JOIN(TotalTab"+cnt+",ParentTab"+cnt+",LEFT."+Sr+" = RIGHT."+Sr+",Trans2"+cnt+"(LEFT,RIGHT));\n";
        				}
        					
        			}
        			if(parent && !total){
        				ecl += "Tabulate_"+Sr+"_"+number+" := ParentTab"+cnt+";\n";
        			}
        			if(total && !parent){
        				ecl += "Tabulate_"+Sr+"_"+number+" := TotalTab"+cnt+";\n";
        			}
        			if(persist.equalsIgnoreCase("true")){
    	        		if(outputName != null && !(outputName.trim().equals("")))    	        			
    	        			ecl += "OUTPUT(("+"Tabulate_"+Sr+"_"+number+")"+",,'~"+outputName+"::tabulate_"+Sr+"', __compressed__, overwrite,NAMED('"+Sr+"_crossTab'))"+";\n";
    	        		else    	        		
    	        			ecl += "OUTPUT(("+"Tabulate_"+Sr+"_"+number+")"+",,'~"+defJobName+"::tabulate_"+Sr+"', __compressed__, overwrite,NAMED('"+Sr+"_crossTab'))"+";\n";    	        		
    	        	}
    	        	else    	        		
    	        		ecl += "OUTPUT(("+"Tabulate_"+Sr+"_"+number+"),NAMED('"+Sr+"_crossTab'));\n";
    	        	
        		}
        		if(!settings.contains("parent") && !settings.contains("total")){
        			ct++;
        			//ecl += "OUTPUT(MyTab"+cnt+",NAMED('"+Sr+"_crossTab'));\n";
        			if(persist.equalsIgnoreCase("true")){
    	        		if(outputName != null && !(outputName.trim().equals(""))){
    	        			ecl += "Tabulate_"+Sr+"_"+number+" := MyTab"+cnt+";\n";
    	        			ecl += "OUTPUT(("+"Tabulate_"+Sr+"_"+number+")"+",,'~"+outputName+"::tabulate_"+Sr+"', __compressed__, overwrite,NAMED('"+Sr+"_crossTab'))"+";\n";
    	        		}else{
    	        			ecl += "Tabulate_"+Sr+"_"+number+" := MyTab"+cnt+";\n";
    	        			ecl += "OUTPUT(("+"Tabulate_"+Sr+"_"+number+")"+",,'~"+defJobName+"::tabulate_"+Sr+"', __compressed__, overwrite,NAMED('"+Sr+"_crossTab'))"+";\n";
    	        		}
    	        	}
    	        	else{
    	        		ecl += "Tabulate_"+Sr+"_"+number+" := MyTab"+cnt+";\n";
    	        		ecl += "OUTPUT(("+"Tabulate_"+Sr+"_"+number+"),NAMED('"+Sr+"_crossTab'));\n";
    	        	}
        		}
        		cnt = cnt + 1;
        		
        	}
        	result.setResult(true);
            
            RowMetaAndData data = new RowMetaAndData();
            data.addValue("ecl", Value.VALUE_TYPE_STRING, ecl);
            
            
            List list = result.getRows();
            list.add(data);
            String eclCode = parseEclFromRowData(list);
            result.setRows(list);
            result.setLogText("ECLTabulate executed, ECL code added");
        }
        
        return result;
    }
    
    public String saveSettings(){
    	String out = "";
    	
    	Iterator<String> it = settings.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		String p =  it.next();
    		out +=  p;
            isFirst = false;
    	}
    	return out;
    }
    public void openSettings(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	settings = new ArrayList<String>();
        	for(int i = 0; i<len; i++){
        		settings.add(strLine[i]);
        	}
        }
    }
    
    public String savePeople(){
    	String out = "";
    	
    	Iterator<Player> it = people.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getOP()+","+p.getType();
            isFirst = false;
    	}
    	return out;
    }

    public void openPeople(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	people = new ArrayList<Player>();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		if(S.length > 1)
        			P.setOP(Integer.parseInt(S[1]));
        		if(S.length > 2)
        			P.setType(S[2]);
        		people.add(P);
        	}
        }
    }
    public String saveRows(){
    	String out = "";
    	
    	Iterator<Player> it = rows.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getOP()+","+p.getType()+","+p.getRule();
            isFirst = false;
    	}
    	return out;
    }

    public void openRows(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	rows = new ArrayList<Player>();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		P.setOP(0); 
        		if(S.length > 2)
        			P.setType(S[2]); 
        		else
        			P.setType("");
        		if(S.length > 3)
        			P.setRule(S[3]);
        		else
        			P.setRule("");
        		rows.add(P);
        	}
        }
    }
    public String saveCols(){
    	String out = "";
    	
    	Iterator<Player> it = cols.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getOP()+","+p.getType()+","+p.getRule();
            isFirst = false;
    	}
    	return out;
    }

    public void openCols(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	cols = new ArrayList<Player>();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		P.setOP(Integer.parseInt(S[1]));
        		if(S.length > 2)
        			P.setType(S[2]); 
        		else
        			P.setType("");
        		if(S.length > 3)
        			P.setRule(S[3]); 
        		else
        			P.setRule("");
        		cols.add(P);
        	}
        }  
    }
    public String saveLayers(){
    	String out = "";
    	
    	Iterator<Player> it = layers.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getOP()+","+p.getType()+","+p.getRule();
            isFirst = false;
    	}
    	return out;
    }

    public void openLayers(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	layers = new ArrayList<Player>();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		P.setOP(0);
        		if(S.length > 2)
        			P.setType(S[2]); 
        		else
        			P.setType("");
        		if(S.length > 3)
        			P.setRule(S[3]);
        		else
        			P.setRule("");
        		layers.add(P);
        	}
        }
    }
    
    public String saveRecordList(int idx){
        String out = "";
        RecordList list = recordList.get(idx);
        ArrayList ar = list.getRecords();
        Iterator<RecordBO> itr = ar.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
    public void openRecordList(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            RecordList rec = new RecordList();
            //System.out.println("Open Record List");
            for(int i =0; i<len; i++){
                //System.out.println("++++++++++++" + strLine[i]);
                //this.recordDef.addRecord(new RecordBO(strLine[i]));
                RecordBO rb = new RecordBO(strLine[i]);
                //System.out.println(rb.getColumnName());
                rec.addRecordBO(rb);
            }
            recordList.add(rec);
        }        
    }
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "settings")) != null)
                openSettings(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "settings")));
            
            if(getTables() != null){
            	String[] tabs = new String[getTables().length];
	            for(int i = 0; i<getTables().length; i++){
	            	if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "table"+i)) != null)
	            		tabs[i] = XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "table"+i));
	            }
	            setTables(tabs);
            }
            if(getRecordList() != null){
            	for(int i = 0; i<getRecordList().size(); i++){
            		if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList_"+getRows().get(i).getFirstName())) != null)
            			openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList_"+getRows().get(i).getFirstName())));
            	}
            }
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")) != null)
                openPeople(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rows")) != null)
                openRows(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rows")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "cols")) != null)
                openCols(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "cols")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layers")) != null)
                openLayers(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layers")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")) != null)
                setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")) != null)
                setLabel(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")) != null)
                setPersistOutputChecked(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")) != null)
                setDefJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Number")) != null)
                setNumber(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Number")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "		<dataset_name ><![CDATA[" + DatasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<settings><![CDATA[" + this.saveSettings() + "]]></settings>" + Const.CR;
        if(getTables() != null){
	        for(int i = 0; i<getTables().length; i++){
	        	retval += "		<table"+i+" eclIsDef = \"true\" eclType = \"dataset\"><![CDATA[" + getTables()[i] + "]]></table"+i+">" + Const.CR;
	        }
        }
        if(getRecordList() != null){
        	for(int i = 0; i<getRecordList().size(); i++){
        		retval += "		<recordList_"+rows.get(i).getFirstName()+"><![CDATA[" + this.saveRecordList(i) + "]]></recordList_"+rows.get(i).getFirstName()+">" + Const.CR;
        	}
        }
        retval += "		<people><![CDATA[" + this.savePeople() + "]]></people>" + Const.CR;
        retval += "		<rows><![CDATA[" + this.saveRows() + "]]></rows>" + Const.CR;
        retval += "		<cols><![CDATA[" + this.saveCols() + "]]></cols>" + Const.CR;
        retval += "		<layers><![CDATA[" + this.saveLayers() + "]]></layers>" + Const.CR;
        retval += "		<label><![CDATA[" + label + "]]></label>" + Const.CR;
        retval += "		<output_name><![CDATA[" + outputName + "]]></output_name>" + Const.CR;
        retval += "		<persist_Output_Checked><![CDATA[" + persist + "]]></persist_Output_Checked>" + Const.CR;
        retval += "		<defJobName><![CDATA[" + defJobName + "]]></defJobName>" + Const.CR;
        retval += "		<Number><![CDATA[" + number + "]]></Number>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	
        	if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                DatasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "settings") != null)
                this.openSettings(rep.getStepAttributeString(id_jobentry, "settings")); //$NON-NLS-1$
        	if(getTables() != null){
        		String[] tables = new String[getTables().length];
        		for(int i = 0; i<getTables().length; i++){
        			if(rep.getStepAttributeString(id_jobentry, "table"+i) != null)
        				tables[i] = (rep.getStepAttributeString(id_jobentry, "table"+i)); //$NON-NLS-1$
        		}
        		setTables(tables);
        	}
        	
        	if(getRecordList() != null){
        		for(int i = 0; i<getRecordList().size(); i++){
        			if(rep.getStepAttributeString(id_jobentry, "recordList_"+getRows().get(i).getFirstName()) != null)
        				openRecordList(rep.getStepAttributeString(id_jobentry, "recordList_"+getRows().get(i).getFirstName()));
        		}
        	}
            
            if(rep.getStepAttributeString(id_jobentry, "people") != null)
                this.openPeople(rep.getStepAttributeString(id_jobentry, "people")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "rows") != null)
                this.openRows(rep.getStepAttributeString(id_jobentry, "rows")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "cols") != null)
                this.openCols(rep.getStepAttributeString(id_jobentry, "cols")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "layers") != null)
                this.openLayers(rep.getStepAttributeString(id_jobentry, "layers")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "outputName") != null)
            	outputName = rep.getStepAttributeString(id_jobentry, "outputName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "label") != null)
            	label = rep.getStepAttributeString(id_jobentry, "label"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "persist_Output_Checked") != null)
            	persist = rep.getStepAttributeString(id_jobentry, "persist_Output_Checked"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "defJobName") != null)
            	defJobName = rep.getStepAttributeString(id_jobentry, "defJobName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "Number") != null)
            	number = rep.getStepAttributeString(id_jobentry, "Number"); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "datasetName", DatasetName); //$NON-NLS-1$
        	rep.saveStepAttribute(id_job, getObjectId(), "settings", this.saveSettings()); //$NON-NLS-1$
        	
        	if(getTables() != null){
        		for(int i = 0; i<getTables().length; i++)
        			rep.saveStepAttribute(id_job, getObjectId(), "table"+i, getTables()[i]); //$NON-NLS-1$
        	}
        	
        	if(getRecordList() != null){
        		for(int i = 0; i<getRecordList().size(); i++){
        			rep.saveStepAttribute(id_job, getObjectId(), "recordList_"+getRows().get(i).getFirstName(), this.saveRecordList(i)); //$NON-NLS-1$
        		}
        	}
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "rows", this.saveRows()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "cols", this.saveCols()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "layers", this.saveLayers()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
        	rep.saveStepAttribute(id_job, getObjectId(), "Number", number);
            
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
