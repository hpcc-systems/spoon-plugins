/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclpercentilebuckets;

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
public class ECLPercentileBuckets extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	
	private String Name = "";
	private String Ties = "";
	private String DatasetName = "";
	private String normList = "";
	private java.util.List people = new ArrayList();
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
	
	public String getTies(){
		return Ties;
	}
    
	public void setTies(String Ties){
		this.Ties = Ties;
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
        	String ecl = "";String field = "";String value = "";String[] norm = normList.split("-");String buck = "";String Uname = getName().replaceAll("\\s", "");
        	String new_record = "";String rule = "";
        	String denorm = "";String myrec = "";String mytrans = "";
        	for(int i = 0; i<norm.length; i++){
        		String[] S = norm[i].split(",");
        		if(i!=norm.length-1){        			
        			field += "\'"+S[0]+"\',";
        			value += "LEFT."+S[0]+",";
        			buck += S[1]+",";        		
        			if(S.length == 3)
        				rule += S[2]+" OR ";
        		}
        		else{
        			field += "\'"+S[0]+"\'";
        			value += "LEFT."+S[0];
        			buck += S[1];        	
        			if(S.length == 3)
        				rule += S[2];
        		}
        		denorm += "REAL bucket;\n"; // needs to be changed when two or more variables are chosen for bucketing
        		myrec += "SELF.bucket:=0;\n";// needs to be changed when two or more variables are chosen for bucketing
        		mytrans += "SELF."+S[0]+":=IF(C="+(i+1)+",R.value,L."+S[0]+");\nSELF.bucket:=IF(C="+(i+1)+",R.bucket,L.bucket);\n";
        	}
        	ecl += Uname+"RejectDS := "+this.getDatasetName()+"(~("+rule+"));\n";;
        	ecl += Uname+"OutlierDS := "+this.getDatasetName()+"("+rule+");\n";
        	
        	ecl += Uname+"URec := RECORD\nUNSIGNED uid;\n"+Uname+"OutlierDS;\nEND;\n";
        	ecl += Uname+"URec "+Uname+"Trans("+Uname+"OutlierDS L, INTEGER C) := TRANSFORM\nSELF.uid := C;\nSELF := L;\nEND;\n"; 
        	ecl += ""+Uname+"MyDS := PROJECT("+Uname+"OutlierDS,"+Uname+"Trans(LEFT,COUNTER));\n";
        	
        	ecl += ""+Uname+"NormRec:=RECORD\nINTEGER4 id;\nINTEGER4 number;\nSTRING field;\nREAL value;\nEND;\n";
        	ecl += ""+Uname+"OutDS:=NORMALIZE("+Uname+"MyDS,"+norm.length+",TRANSFORM("+Uname+"NormRec,SELF.id:=LEFT.uid;\nSELF.number:=COUNTER;SELF.field:=CHOOSE(COUNTER,"+field+");" +
        			"SELF.value:=CHOOSE(COUNTER,"+value+")));\n";
        	
        	ecl += ""+Uname+"RankableField := RECORD\n"+Uname+"OutDS;\nUNSIGNED pos:=0;\nEND;\n";
        	ecl += ""+Uname+"T:=TABLE(SORT("+Uname+"OutDS,number,field,value),"+Uname+"RankableField);\n";
        	ecl += "TYPEOF("+Uname+"T) add_rank("+Uname+"T le, UNSIGNED c):=TRANSFORM\nSELF.pos:=c;\nSELF:=le;\nEND;\n";
        	ecl += ""+Uname+"P:=PROJECT("+Uname+"T,add_rank(LEFT,COUNTER));\n";
        	ecl += ""+Uname+"RS:=RECORD\nSeq:=MIN(GROUP,"+Uname+"P.pos);\n"+Uname+"P.number;\nEND;\n";
        	ecl += ""+Uname+"Splits:= TABLE("+Uname+"P,"+Uname+"RS,number,FEW);\n";
        	ecl += "TYPEOF("+Uname+"T) "+Uname+"to("+Uname+"P le, "+Uname+"Splits ri):=TRANSFORM\nSELF.pos:=1+le.pos-ri.Seq;\nSELF:=le;\nEND;\n";
        	ecl += ""+Uname+"outfile:= JOIN("+Uname+"P,"+Uname+"Splits,LEFT.number=RIGHT.number,"+Uname+"to(LEFT,RIGHT),LOOKUP);\n";
        	
        	ecl += "N:=COUNT("+Uname+"MyDS);\n";
        	ecl += "SET OF INTEGER buckets := ["+buck+"];\n";
 
        	if(getTies().equals("HI")){
        		ecl += "TYPEOF("+Uname+"outfile) "+Uname+"tra("+Uname+"Outfile L, "+Uname+"Outfile R) := TRANSFORM\nSELF.pos := R.pos;\nSELF := L;\nEND;\n";
        		ecl += ""+Uname+"newTab := ROLLUP("+Uname+"Outfile,LEFT.field = RIGHT.field AND LEFT.value = RIGHT.value,"+Uname+"tra(LEFT,RIGHT));\n";
        	}
        	
        	else{
        		ecl += ""+Uname+"NewTab:=DEDUP("+Uname+"outfile, LEFT.field=RIGHT.field AND LEFT.value=RIGHT.value);\n";
        	}
        	
        	ecl += ""+Uname+"NewRec:=RECORD\n"+Uname+"NewTab;\nREAL bucket;\nEND;\n";
        	ecl += ""+Uname+"NewRec "+Uname+"Tr("+Uname+"NewTab L, INTEGER C) := TRANSFORM\nSELF.bucket := ROUNDUP(L.pos*buckets[L.number]/(N+1))-1;\nSELF := L;\nEND;\n";
        	ecl += ""+Uname+"Tab := PROJECT("+Uname+"NewTab,"+Uname+"Tr(LEFT,COUNTER));\n";
        	ecl += ""+Uname+"Rec1 := RECORD\n"+Uname+"Tab.value;\n"+Uname+"Tab.bucket;\n"+Uname+"Tab.field;\n"+Uname+"Tab.number;\nEND;\n";
        	ecl += ""+Uname+"Tab1 := TABLE("+Uname+"Tab,"+Uname+"Rec1);\n";
        	ecl += ""+Uname+"Tab2 := DEDUP(JOIN("+Uname+"Outfile,"+Uname+"Tab1,LEFT.field = RIGHT.field AND LEFT.value = RIGHT.value),LEFT.field = RIGHT.field AND LEFT.pos = RIGHT.pos);\n";
        	ecl += ""+Uname+"Tab3 := SORT("+Uname+"Tab2,id,number);\n";
        	ecl += ""+Uname+"new_record:=RECORD\n"+Uname+"MyDS;\n"+denorm+"END;\n";
        	ecl += ""+Uname+"new_record "+Uname+"transfo("+Uname+"MyDS L, INTEGER C) := TRANSFORM\n" + myrec +
        			"SELF:=L;\nEND;\n";
        	
        	ecl += ""+Uname+"Orig := PROJECT("+Uname+"MyDS,"+Uname+"transfo(LEFT,COUNTER));\n";
        	ecl += ""+Uname+"new_record "+Uname+"denorm("+Uname+"Orig L, "+Uname+"Tab3 R, INTEGER C):=TRANSFORM\n"+ mytrans;
        	ecl += "SELF := L;\nEND;\n";
        	
            ecl += ""+Uname+"Denormed:=DENORMALIZE("+Uname+"Orig,"+Uname+"Tab3,LEFT.uid=RIGHT.id,"+Uname+"denorm(LEFT,RIGHT,COUNTER));\n";
            
            ArrayList<RecordBO> ar = getRecordList().getRecords();
            
            for(int i = 0; i<ar.size();i++){
            	RecordBO ob = ar.get(i);
            	new_record += ob.getColumnName()+",";
            }
            
        	ecl += ""+Uname+"Reco := RECORD\n"+Uname+"Tab2.bucket;\nminval := MIN(GROUP,"+Uname+"Tab3.value);\nmaxval := MAX(GROUP,"+Uname+"Tab3.value);\nEND;\n";
        	ecl += ""+Uname+"TabRec := TABLE("+Uname+"Tab2,"+Uname+"Reco,bucket);\n";
        	ecl += Uname+"Intermediate := TABLE(JOIN("+Uname+"Denormed, "+Uname+"TabRec, LEFT.bucket = RIGHT.bucket),{"+new_record.substring(0, new_record.length()-1)+"});\n";
        	ecl += "TYPEOF("+Uname+"Intermediate) "+Uname+"RejectTrans("+Uname+"RejectDS L) := TRANSFORM\n" +
        			"	SELF.bucket := -1;\n" +
        			"	SELF.minval := -1;\n" +
        			"	SELF.maxval := -1;\n" +
        			"	SELF := L;\n" +
        			"END;\n";
        	ecl += Uname+"Reject := PROJECT("+Uname+"RejectDS,"+Uname+"RejectTrans(LEFT));\n";
        	ecl += OutName+" := "+Uname+"Intermediate&"+Uname+"Reject;\n";
        	if(persist.equalsIgnoreCase("true")){
        		if(outputName != null && !(outputName.trim().equals(""))){
        			ecl += "OUTPUT("+OutName+",,'~"+outputName+"::"+OutName+"', __compressed__, overwrite,NAMED('BucketDataset'));\n";
        		}
        		else{
        			ecl += "OUTPUT("+OutName+",,'~"+defJobName+"::"+OutName+"', __compressed__, overwrite,NAMED('BucketDataset'));\n";
        		}
            }
            else{
            	ecl += "OUTPUT("+OutName+",THOR);\n";
            }
        	
        	
        	
	        logBasic("PercentileBuckets Job =" + ecl); 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, ecl);
	        
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	        result.setLogText("ECLPercentileBuckets executed, ECL code added");
	        return result;
        }
        
    }
    
    public String savePeople(){
    	String out = "";
    	
    	Iterator it = people.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+"-"+p.getBuckets()+"-"+p.getRule();
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
        		String[] S = strLine[i].split("-");
        		Player P = new Player();
        		if(S.length == 1){
        			P.setFirstName(S[0]);
        			P.setBuckets("");
        			P.setRule("");
        		}
        		else if(S.length ==2){
        			P.setFirstName(S[0]);
        			P.setBuckets(S[1]);
        			P.setRule("");
        		}
        		else if(S.length == 3){
        			P.setFirstName(S[0]);
        			P.setBuckets(S[1]);
        			P.setRule(S[2]);
        		}
        		/*P.setFirstName(S[0]);
        		P.setBuckets(S[1]);*/
        		people.add(P);
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
    
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));

            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Ties")) != null)
                setTies(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Ties")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "normList")) != null)
                setnormList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "normList")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")) != null)
                openPeople(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")));
            
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
    
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "		<name><![CDATA[" + Name + "]]></name>" + Const.CR;
        retval += "		<people><![CDATA[" + this.savePeople() + "]]></people>" + Const.CR;
        retval += "		<Ties><![CDATA[" + Ties + "]]></Ties>" + Const.CR;
        retval += "		<normList><![CDATA[" + this.getnormList() + "]]></normList>" + Const.CR;
        retval += "		<dataset_name><![CDATA[" + DatasetName + "]]></dataset_name>" + Const.CR;        
        retval += "		<label><![CDATA[" + label + "]]></label>" + Const.CR;
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

            if(rep.getStepAttributeString(id_jobentry, "Ties") != null)
            	Ties = rep.getStepAttributeString(id_jobentry, "Ties"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "normList") != null)
                this.setnormList(rep.getStepAttributeString(id_jobentry, "normList")); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "people") != null)
                this.openPeople(rep.getStepAttributeString(id_jobentry, "people")); //$NON-NLS-1$
            
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
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Ties", Ties); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "Name", Name); //$NON-NLS-1$
        	
        	rep.saveStepAttribute(id_job, getObjectId(), "normList", this.getnormList()); //$NON-NLS-1$
        	
            rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
        	rep.saveStepAttribute(id_job, getObjectId(), "OutName", OutName); //$NON-NLS-1$
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
