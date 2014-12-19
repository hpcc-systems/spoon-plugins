/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclchisquare;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author KeshavS
 */
public class Mean {
	
	
    private String datasetName = "";
    private ArrayList<String> people = new ArrayList<String>();
    private ArrayList<String> group = new ArrayList<String>();
    private String checkList = "";
    private String fieldList = "";
    private String single = "";
   	private String persist = "";
   	
   	Mean() {
   		Storage.noOfVariables = Storage.noOfVariables+1;
   	}
   	
   	public String getPersistOutputChecked() {
   		return persist;
   	}

   	public void setPersistOutputChecked(String persist) {
   		this.persist = persist;
   	}
    
	public void setPeople(ArrayList<String> people){
		this.people = people;
	}
	
	public ArrayList<String> getPeople(){
		return people;
	}

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getSingle() {
        return single;
    }

    public void setSingle(String single) {
        this.single = single;
    }

    public String getCheckList() {
        return checkList;
    }

    public void setCheckList(String checkList) {
        this.checkList = checkList;
    }
    
    public String getFieldList() {
        return fieldList;
    }

    public void setFieldList(String fieldList) {
        this.fieldList= fieldList;
    }
        
    public String ecl() {
    	
    	int noOfVar = Storage.noOfVariables;

        	String[] check = getCheckList().split(",");
        	String[] fieldNames = fieldList.split(",");
        	String normlist = "";int cnt = 0;
        	String List = "";
        	for(int i = 0; i<fieldNames.length; i++){
        		if(i!=fieldNames.length-1){
        			normlist += "LEFT."+fieldNames[i]+",";
        			List += "\'"+fieldNames[i]+"\',";
        		}
        		else{
        			normlist += "LEFT."+fieldNames[i];
        			List += "\'"+fieldNames[i]+"\'";
        		}
        	}
        	
        	String dataList = "";String col = "";String out = "";String grp = "";String forP = "";String join = "";String outfile = "";
        	for(Iterator<String> it = group.iterator(); it.hasNext();){
        		String P = (String)it.next();        		
        		dataList += "DECIMAL40_10" + " " +P+";\n";
        		col += "SELF."+P+":=LEFT."+P+",";
        		out += "OutDS"+noOfVar+"."+P+";\n";
        		grp += P+",";
        		forP += "P."+P+";\n";
        		outfile += "outfile."+P+";\n";
        		join += "LEFT."+P+"=RIGHT."+P+" AND ";
        	}
        	
        	
			String ecl = "URec"+noOfVar+" := RECORD\nUNSIGNED uid;\n"+this.datasetName+";\nEND;\n";
        	ecl += "URec"+noOfVar+" Trans"+noOfVar+"("+this.datasetName+" L, INTEGER C) := TRANSFORM\nSELF.uid := C;\nSELF := L;\nEND;\n"; 
        	ecl += "MyDS"+noOfVar+" := PROJECT("+datasetName+",Trans"+noOfVar+"(LEFT,COUNTER));\n";
        	
        	ecl += "NumField"+noOfVar+" := RECORD\nUNSIGNED id;\n"+dataList+"STRING field;\nREAL8 value;\nEND;\n";
        	ecl += "OutDS"+noOfVar+" := NORMALIZE(MyDS"+noOfVar+","+fieldNames.length+", TRANSFORM(NumField"+noOfVar+",SELF.id:=LEFT.uid,"+col+"SELF.field:=CHOOSE(COUNTER,"+List+");SELF.value:=CHOOSE" +
        			"(COUNTER,"+normlist+")));\n";
        	ecl += "SingleField"+noOfVar+" := RECORD\n"+out+"\nOutDS"+noOfVar+".field;\n";
        	if(check[0].equals("true"))
        		{ecl += "mean:=AVE(GROUP,OutDS"+noOfVar+".value);\n";cnt++;}
        	if(check[3].equals("true"))
        		{ecl += "Sd:=SQRT(VARIANCE(GROUP,OutDS"+noOfVar+".value));\n";cnt++;}
        	if(check[4].equals("true"))
        		{ecl += "Maxval:=MAX(GROUP,OutDS"+noOfVar+".value);\n";cnt++;}
        	if(check[5].equals("true"))
        		{ecl += "Minval:=MIN(GROUP,OutDS"+noOfVar+".value);\n";cnt++;}
        	ecl += "END;\n";
    		if(cnt>0)
        		ecl += getSingle()+" := TABLE(OutDS"+noOfVar+",SingleField"+noOfVar+","+grp+"field);\n";
        	
        	
        	if(check[1].equals("true") || check[2].equals("true")){
        	// this can be reused        		
	        	ecl += "RankableField"+noOfVar+" := RECORD\nOutDS"+noOfVar+";\nUNSIGNED pos:=0;\nEND;\n";
	        	ecl += "T"+noOfVar+":=TABLE(SORT(OutDS"+noOfVar+","+grp+"field,Value),RankableField"+noOfVar+");\n";
	        	ecl += "TYPEOF(T"+noOfVar+") add_rank(T"+noOfVar+" le, UNSIGNED c):=TRANSFORM\nSELF.pos:=c;\nSELF:=le;\nEND;\n";
	        	ecl += "P"+noOfVar+":=PROJECT(T"+noOfVar+",add_rank(LEFT,COUNTER));\n";
	        	ecl += "RS"+noOfVar+":=RECORD\nSeq:=MIN(GROUP,P"+noOfVar+".pos);\n"+forP+"P"+noOfVar+".field;\nEND;\n";
	        	ecl += "Splits"+noOfVar+" := TABLE(P"+noOfVar+",RS"+noOfVar+","+grp+"field,FEW);\n";
	        	ecl += "TYPEOF(T"+noOfVar+") to(P"+noOfVar+" le, Splits"+noOfVar+" ri):=TRANSFORM\nSELF.pos:=1+le.pos-ri.Seq;\nSELF:=le;\nEND;\n";
	        	ecl += "outfile"+noOfVar+" := JOIN(P"+noOfVar+",Splits"+noOfVar+","+join+"LEFT.field=RIGHT.field,to(LEFT,RIGHT),LOOKUP);\n";
	        	if(check[1].equals("true")){
	        		ecl += "cntRec"+noOfVar+":=RECORD\n"+outfile+"outfile"+noOfVar+".field;\n" +
	        				"cnt"+noOfVar+":=COUNT(GROUP);\nSET OF UNSIGNED poso := [];\nEND;\n";
	        		ecl += "cntgroups"+noOfVar+" := TABLE(outfile"+noOfVar+",cntRec"+noOfVar+","+grp+"field);\n";
	        		ecl += "cntRec"+noOfVar+" Transcnt"+noOfVar+"(cntgroups"+noOfVar+" L, INTEGER C):=" +
	        				"TRANSFORM\n" +
	        				"SELF.poso := IF(L.cnt"+noOfVar+"%2=0,[L.cnt"+noOfVar+"/2,L.cnt"+noOfVar+"/2 + 1],[(L.cnt"+noOfVar+"+1)/2]);\n" +
	        				"SELF:=L;\nEND;\n";
	        		ecl += "MyT"+noOfVar+" := PROJECT(cntgroups"+noOfVar+",Transcnt"+noOfVar+"(LEFT,COUNTER));\n";
	        		
	        		ecl += "MedianValues"+noOfVar+":=JOIN(outfile"+noOfVar+",MyT"+noOfVar+","+join+"LEFT.field=RIGHT.field AND LEFT.pos IN RIGHT.poso);\n";
	        		ecl += "MedianTable"+noOfVar+" := TABLE(MedianValues"+noOfVar+",{"+grp+"field;Median := AVE(GROUP, MedianValues"+noOfVar+".value);},"+grp+"field);\n";
	        		if(cnt == 0){
	        			ecl += getSingle()+" := MedianTable"+noOfVar+";\n";
	        		}
	        		else{
	        			ecl += getSingle()+" := JOIN(SingleUni"+noOfVar+",MedianTable"+noOfVar+","+join+"LEFT.field = RIGHT.field);\n";
	        		}
	        	}	        	
        	}
        	if(cnt>0 && check[1].equals("false")){
        		ecl += getSingle()+" := SingleUni"+noOfVar+";\n";
        	}
        	System.out.println(noOfVar);
        	System.out.println(Storage.noOfVariables);
        	return(ecl);
    }
}