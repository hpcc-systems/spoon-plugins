/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclpercentile;

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
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.ecljobentrybase.*;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;


/**
 *
 * @author KeshavS
 */
public class ECLPercentile extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	private String Name = "";
	private String DatasetName = "";
	private String normList = "";
	//private String outTables[] = null;
	//private ArrayList<String[]> percentileSettings = new ArrayList<String[]>();
	private java.util.List fields = new ArrayList();
	private String label ="";
	private String outputName ="";
	private String persist = "";
	private String defJobName = "";
	private String OutName = "";
	private RecordList recordList = new RecordList();
	
	public RecordList getRecordList() {
		return recordList;
	}

	public void setRecordList(RecordList recordList) {
		this.recordList = recordList;
	}
		
	public String getOutName() {
		return OutName;
	}

	public void setOutName(String outName) {
		OutName = outName;
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

	public void setFields(java.util.List fields){
		this.fields = fields;
	}
	
	public java.util.List getFields(){
		return fields;
	}
	
/*	public void setoutTables(String[] outTables){
		this.outTables = outTables;
	}
	
	public String[] getoutTables(){
		return outTables;
	}
*/	
	public String getName(){
		return Name;
	}
    
	public void setName(String Name){
		this.Name = Name;
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
	
/*	public ArrayList<String[]> getpercentileSettings(){
		return percentileSettings;
	}
	
	public void setpercentileSettings(ArrayList<String[]> percentileSettings){
		this.percentileSettings = percentileSettings;
	}
*/  final AutoPopulate ap = new AutoPopulate();
	
	
    @Override
    public Result execute(Result prevResult, int k) throws KettleException {

    	
    	
    	Result result = modifyResults(prevResult);
        if(result.isStopped()){
        return result;
       }

        else{
	        String percentile = "";String[] norm = this.normList.split("#");String value = "";String field = "";String normString = "SET OF INTEGER buckets:=[";
	        String percents = "";String normalize = "";int high = 0;String filter = "(";String name = getName().replaceAll("\\s","");
	        for(int i = 0; i<norm.length; i++){
	        	String[] S = norm[i].split("-");
	        	if(high < S[1].split(",").length)
	        		high = S[1].split(",").length;
	        	if(i != norm.length-1){ 
        			value += "LEFT."+S[0]+",";
        			field += "\'"+S[0]+"\',";
        			normalize += "IF(SELF.field='"+S[0]+"',"+S[0]+"P[COUNTER],";
        			if(S.length == 3)
        				filter += "(field = '"+S[0]+"' AND "+S[2].replace(S[0], "value")+") OR ";  
        		}
        		else{
        			value += "LEFT."+S[0];
        			field += "\'"+S[0]+"\'";        			
        			normalize += S[0]+"P[COUNTER]";
        			if(S.length == 3)
        				filter += "(field = '"+S[0]+"' AND "+S[2].replace(S[0], "value")+")";
        		}
	        	percents += "SET OF INTEGER "+S[0]+"P := [0,1,5,10,25,50,75,90,95,99,100";
    			if(!S[1].equals(" "))
    				percents += ","+S[1]+"];\n";
    			else
    				percents += "];\n";
	        }
	        for(int i = 0; i<norm.length-1; i++){
	        	normalize += ")";
	        }
	        filter += ")";
	        percentile += "NormRec"+name+":=RECORD\nINTEGER4 number;\nSTRING field;\nREAL value;\nEND;\n";
	        if(filter.length() == 2)
	        	percentile += "OutDS"+name+":=NORMALIZE("+this.getDatasetName()+","+norm.length+",TRANSFORM(NormRec"+name+",SELF.number:=COUNTER,SELF.field:=CHOOSE(COUNTER,"+field+")" +
	        			",SELF.value:=CHOOSE(COUNTER,"+value+")));\n";
	        else{
	        	percentile += "OutDS1"+name+":=NORMALIZE("+this.getDatasetName()+","+norm.length+",TRANSFORM(NormRec"+name+",SELF.number:=COUNTER,SELF.field:=CHOOSE(COUNTER,"+field+")" +
	        			",SELF.value:=CHOOSE(COUNTER,"+value+")));\n";
	        	percentile += "OutDS"+name+" := OutDS1"+name+"("+filter+");\n";
	        }
	        percentile += "RankableField"+name+" := RECORD\nOutDS"+name+";\nUNSIGNED pos:=0;\nEND;\n";
	        percentile += "T"+name+":=TABLE(SORT(OutDS"+name+",field,Value),RankableField"+name+");\n";
	        percentile += "TYPEOF(T"+name+") add_rank"+name+"(T"+name+" le, UNSIGNED c):=TRANSFORM\nSELF.pos:=c;\nSELF:=le;\nEND;\n";
	        percentile += "P"+name+":=PROJECT(T"+name+",add_rank"+name+"(LEFT,COUNTER));\n";
	        percentile += "RS"+name+":=RECORD\nSeq:=MIN(GROUP,P"+name+".pos);\nP"+name+".field;\nEND;\n";
	        percentile += "Splits"+name+" := TABLE(P"+name+",RS"+name+",field,FEW);\n";
	        percentile += "TYPEOF(T"+name+") to"+name+"(P"+name+" le, Splits"+name+" ri):=TRANSFORM\nSELF.pos:=1+le.pos-ri.Seq;\nSELF:=le;\nEND;\n";
	        percentile += "outfile"+name+" := JOIN(P"+name+",Splits"+name+",LEFT.field=RIGHT.field, to"+name+"(LEFT,RIGHT),LOOKUP);\n";
	        //percentile += "N"+name+":=COUNT("+this.getDatasetName()+");\n";
	        	       
/*	        for(Iterator<String[]> it = percentileSettings.iterator(); it.hasNext();)
	        {
	        	String[] S = it.next();
	        	normString += S[1].split("#").length+",";	        	
	        }
	        normString =  normString.substring(0, normString.length()-1);

	        percentile += normString+"];\nmaximum:=MAX(buckets);\n";
	        percentile += "T roll(T L, T R):=TRANSFORM\nSELF:=L;\nEND;\n";
	        percentile += "Split:=ROLLUP(T, LEFT.field = RIGHT.field, roll(LEFT,RIGHT));\n";*/
	        
	        percentile += percents;
	        percentile += "CntRec"+name+":=RECORD\nOutfile"+name+".field;\ncnt:=COUNT(GROUP);\nEND;\n";
	        percentile += "Cnt"+name+" := TABLE(Outfile"+name+",CntRec"+name+",field,FEW);\n";
	        		


	        percentile += "Rec1"+name+":=RECORD\nSTRING field;\nINTEGER4 percentiles;\nEND;\n";
	        
	        
	        percentile += "MyTab1"+name+":=NORMALIZE(Splits"+name+","+(11+high)+",TRANSFORM(Rec1"+name+",SELF.field:=LEFT.field,SELF.percentiles:="+normalize+"));\n";
	        percentile += "MyTab"+name+" := JOIN(MyTab1"+name+",Cnt"+name+",LEFT.field=RIGHT.field);\n";
	        percentile += "PerRec"+name+":=RECORD\nMyTab"+name+";\nREAL rank:=IF(mytab"+name+".percentiles = -1,0," +
																"IF(mytab"+name+".percentiles = 0,1,"+
																   "IF(ROUND(mytab"+name+".percentiles*(myTab"+name+".cnt+1)/100)>=myTab"+name+".cnt,myTab"+name+".cnt,"+
																	  "mytab"+name+".percentiles*(myTab"+name+".cnt+1)/100)));\nEND;\n";
	        percentile += "valuestab"+name+" := TABLE(mytab"+name+",perRec"+name+");\n";
	        percentile += "rankRec"+name+" := RECORD\nSTRING field := valuestab"+name+".field;\nREAL rank := valuestab"+name+".rank;\nINTEGER4 intranks;\n" +
	        				"REAL decranks;\nINTEGER4 plusOneranks;\n"+
								"valuestab"+name+".percentiles;\nEND;\n";
	        percentile += "rankRec"+name+" tr"+name+"(valuestab"+name+" L, INTEGER C) := TRANSFORM\n"+
					      "SELF.decranks := IF(L.rank - (ROUNDUP(L.rank) - 1) = 1,0,L.rank - (ROUNDUP(L.rank) - 1));\n"+						  
						  "SELF.intranks := IF(ROUNDUP(L.rank) = L.rank,L.rank,(ROUNDUP(L.rank) - 1));\n"+
						  "SELF.plusOneranks := SELF.intranks + 1;\n"+
						  "SELF := L;\n"+
						  "END;\n";
	        percentile += "ranksTab"+name+" := PROJECT(valuestab"+name+",tr"+name+"(LEFT,COUNTER));\n";
	        percentile += "ranksRec"+name+" := RECORD\nSTRING field;\nranksTab"+name+".decranks;\nranksTab"+name+".percentiles;\nINTEGER4 ranks;\nEND;\n";
	        percentile += "rankTab"+name+" := NORMALIZE(ranksTab"+name+",2,TRANSFORM(ranksRec"+name+",SELF.field := LEFT.field; " +
	        												"SELF.ranks := CHOOSE(COUNTER,LEFT.intranks,LEFT.plusOneranks),SELF := LEFT));\n";
	        percentile += "MTable"+name+":=SORT(JOIN(rankTab"+name+", outfile"+name+", LEFT.field = RIGHT.field AND LEFT.ranks = RIGHT.pos)" +
	        		",field,percentiles,ranks);\n";
	        percentile += "MyTable"+name+" := DEDUP(MTable"+name+", LEFT.percentiles = RIGHT.percentiles AND LEFT.ranks = RIGHT.ranks AND LEFT.field = RIGHT.field);\n";
	        percentile += "MyTable"+name+" RollThem"+name+"(MyTable"+name+" L, MyTable"+name+" R) := TRANSFORM\n"+
   						  "SELF.value := L.value + L.decranks*(R.value - L.value);\n"+
						  "SELF := L;\n"+
						  "END;\n";
	        percentile += "percentileTab"+name+" := ROLLUP(MyTable"+name+", LEFT.percentiles = RIGHT.percentiles AND LEFT.field=RIGHT.field, RollThem"+name+"(LEFT,RIGHT));\n";
	        percentile += getOutName()+":=TABLE(percentileTab"+name+",{field,percentiles,value});\n";
	        if(persist.equalsIgnoreCase("true")){
        		if(outputName != null && !(outputName.trim().equals(""))){
        			percentile += "OUTPUT("+getOutName()+",,'~"+outputName+"::"+OutName+"', __compressed__, overwrite,NAMED('Percentile'))"+";\n";
        		}else{
        			percentile += "OUTPUT("+getOutName()+",,'~"+defJobName+"::"+OutName+"', __compressed__, overwrite,NAMED('Percentile'))"+";\n";
        		}
        	}
        	else{
        		percentile += "OUTPUT("+getOutName()+",NAMED('Percentile'));\n";
        	}
	        /*for(int i = 0, j=norm.length; i<norm.length; i++,j++){
	        	String[] S = norm[i].split("-");
	        	percentile += S[0]+"_"+name+":=TABLE(percentileTab(field='"+S[0]+"'),{field,percentiles,value});\n";	        	
	        	if(persist.equalsIgnoreCase("true")){
	        		if(outputName != null && !(outputName.trim().equals(""))){
	        			percentile += "OUTPUT("+S[0]+"_"+name+",,'~eda::"+outputName+S[0]+"::percentile', __compressed__, overwrite,NAMED('Percentile_"+S[0]+"'))"+";\n";
	        		}else{
	        			percentile += "OUTPUT("+S[0]+"_"+name+",,'~eda::"+defJobName+S[0]+"::percentile', __compressed__, overwrite,NAMED('Percentile_"+S[0]+"'))"+";\n";
	        		}
	        	}
	        	else{
	        		percentile += "OUTPUT("+S[0]+"_"+name+",NAMED('Percentile_"+S[0]+"'));\n";
	        	}
	        	//percentile += "OUTPUT("+S[0]+"_"+name+",THOR);\n";
	        }*/
        	
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, percentile);
	        
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	        result.setLogText("ECLPercentile executed, ECL code added");
        }
        return result;
  }
    
    public String saveFields(){
    	String out = "";
    	
    	Iterator it = fields.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Cols p = (Cols) it.next();
    		out +=  p.getFirstName()+"-"+p.getNumber()+"-"+p.getRule();
            isFirst = false;
    	}
    	return out;
    }

    public void openFields(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	fields = new ArrayList();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split("-");
        		Cols P = new Cols();
        		P.setFirstName(S[0]);
        		if(S.length > 1)
        			P.setNumber(S[1]);        		
        		if(S.length > 2)
        			P.setRule(S[2]); 
        		
        		fields.add(P);
        	}
        }
    }

    public String saveRecordList(){
        String out = "";
        ArrayList list = recordList.getRecords();
        Iterator<RecordBO> itr = list.iterator();
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
            recordList = new RecordList();
            //System.out.println("Open Record List");
            for(int i =0; i<len; i++){
                //System.out.println("++++++++++++" + strLine[i]);
                //this.recordDef.addRecord(new RecordBO(strLine[i]));
                RecordBO rb = new RecordBO(strLine[i]);
                //System.out.println(rb.getColumnName());
                recordList.addRecordBO(rb);
            }
        }
    }
/*    public String saveOutTables(){
    	String out = "";int i = 0;    	
    	boolean isFirst = true;
    	if(outTables!=null){
	    	while(i < outTables.length){
	    		if(!isFirst){out+="|";}
	    		
	    		out +=  outTables[i];
	    		i++;
	            isFirst = false;
	    	}
    	}
    	return out;
    }
*/
/*    public void openOutTables(String in){
    	String[] strLine = in.split("[|]");
    	int len = strLine.length;
    	if(len>0){
    		outTables = new String[len];
    		for(int i = 0; i<len; i++){
    			outTables[i] = strLine[i];
    		}
    	}
    }
 */
    
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")) != null)
                setName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
                        
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "normList")) != null)
                setnormList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "normList")));
            
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "percentileSettings")) != null)
              //  openPercentile(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "percentileSettings")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fields")) != null)
                openFields(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fields")));
            
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outTables")) != null)
            	//openOutTables(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outTables")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")) != null)
                setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")) != null)
                setLabel(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")) != null)
                setPersistOutputChecked(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")) != null)
                setDefJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "OutName")) != null)
                setOutName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "OutName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
                openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));

/*            String[] S = normList.split("#");
        	this.outTables = new String[S.length];        	
        	for(int i = 0; i<S.length; i++){
        		String[] s = S[i].split("-");
        		this.outTables[i] = s[0]+"_"+getName();
        	}
*/            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "		<name><![CDATA[" + Name + "]]></name>" + Const.CR;
        retval += "		<fields><![CDATA[" + this.saveFields() + "]]></fields>" + Const.CR;
        retval += "		<normList><![CDATA[" + this.getnormList() + "]]></normList>" + Const.CR;
        retval += "		<dataset_name><![CDATA[" + DatasetName + "]]></dataset_name>" + Const.CR;
        //retval += "		<percentileSettings><![CDATA[" + this.savePercentile() + "]]></percentileSettings>" + Const.CR;
/*        for(int i = 0; i<saveOutTables().split("[|]").length; i++){
        	retval += "		<outTables eclIsGraphable=\"true\"><![CDATA[" + saveOutTables().split("[|]")[i] + "]]></outTables>" + Const.CR;
        }
*/        retval += "		<label><![CDATA[" + label + "]]></label>" + Const.CR;
        retval += "		<output_name><![CDATA[" + outputName + "]]></output_name>" + Const.CR;
        retval += "		<persist_Output_Checked><![CDATA[" + persist + "]]></persist_Output_Checked>" + Const.CR;
        retval += "		<defJobName><![CDATA[" + defJobName + "]]></defJobName>" + Const.CR;
        retval += "		<OutName eclIsDef=\"true\" eclType=\"dataset\"><![CDATA[" + OutName + "]]></OutName>" + Const.CR;
        retval += "		<recordList><![CDATA[" + this.saveRecordList() + "]]></recordList>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	if(rep.getStepAttributeString(id_jobentry, "Name") != null)
                Name = rep.getStepAttributeString(id_jobentry, "Name"); //$NON-NLS-1$
        	
        	if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                DatasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "normList") != null)
                this.setnormList(rep.getStepAttributeString(id_jobentry, "normList")); //$NON-NLS-1$
            
          //  if(rep.getStepAttributeString(id_jobentry, "percentileSettings") != null)
            //    this.openPercentile(rep.getStepAttributeString(id_jobentry, "percentileSettings")); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "fields") != null)
                this.openFields(rep.getStepAttributeString(id_jobentry, "fields")); //$NON-NLS-1$
            
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
            
            if(rep.getStepAttributeString(id_jobentry, "OutName") != null)
            	OutName = rep.getStepAttributeString(id_jobentry, "OutName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
                this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$

        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "datasetName", DatasetName); //$NON-NLS-1$        	
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Name", Name); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "normList", this.getnormList()); //$NON-NLS-1$
        	
        	//rep.saveStepAttribute(id_job, getObjectId(), "percentileSettings", this.savePercentile()); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "fields", this.saveFields()); //$NON-NLS-1$
/*        	for(int i = 0; i<saveOutTables().split("[|]").length; i++)
        		rep.saveStepAttribute(id_job, getObjectId(), "outTables", this.saveOutTables().split("[|]")[i]); //$NON-NLS-1$
*/        	rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
        	rep.saveStepAttribute(id_job, getObjectId(), "OutName", OutName);
        	rep.saveStepAttribute(id_job, getObjectId(), "recordList", this.saveRecordList()); //$NON-NLS-1$
        	
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
