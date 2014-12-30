/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecltabulatebuckets;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hpccsystems.javaecl.Rollup;
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
public class ECLTabulateBuckets extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
	


	private String DatasetName = "";
	private java.util.List people = new ArrayList();
	private java.util.List rows = new ArrayList();
	private java.util.List layers = new ArrayList();
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
	
	public void setRows(java.util.List rows){
		this.rows = rows;
	}
	
	public java.util.List getRows(){
		return rows;
	}
	
	public void setLayers(java.util.List layers){
		this.layers = layers;
	}
	
	public java.util.List getLayers(){
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
    	
    	
    	Result result = modifyResults(prevResult);
        if(result.isStopped()){
        return result;
       }
        else{
        	String ecl = "";int cnt = 0;String layer = "    ";String smoking = " ";String group = "";String layerRec = "";
        	String field = "";String value = "";String name = getName().replaceAll("\\s", "");
        	String buck = "";int i = 0;String filter = "(";
        	
        	for(Iterator it = rows.iterator(); it.hasNext();){
        		Player S = (Player) it.next();
        		if(i!=rows.size()-1){        			
        			field += "\'"+S.getFirstName()+"\',";
        			value += "LEFT."+S.getFirstName()+",";
        			if(S.getBuckets().equals("0"))
        				buck += "1,";
        			else
        				buck += S.getBuckets()+",";
        			if(S.getRule().length()>0)
        				filter += "(field = '"+S.getFirstName()+"' AND "+S.getRule().replace(S.getFirstName(), "value")+") OR ";
        		}
        		else{
        			field += "\'"+S.getFirstName()+"\'";
        			value += "LEFT."+S.getFirstName();
        			if(S.getBuckets().equals("0"))
        				buck += "1";
        			else
        				buck += S.getBuckets();
        			if(S.getRule().length()>0)
        				filter += "(field = '"+S.getFirstName()+"' AND "+S.getRule().replace(S.getFirstName(), "value")+")";
        		}
        		i++;
        	}
        	
        	filter += ")";
        	
        	for(Iterator it = layers.iterator(); it.hasNext();){
        		Player S = (Player) it.next();
        		if(i < layers.size()-1){
        			layer += "SELF.layer_"+S.getFirstName()+":=CHOOSE(COUNTER,LEFT."+S.getFirstName()+");";
        			group += "layer_"+S.getFirstName()+",";
        		}
        		else{
        			layer += "SELF.layer_"+S.getFirstName()+":=CHOOSE(COUNTER,LEFT."+S.getFirstName()+")";
        			group += "layer_"+S.getFirstName();
        		}
        		smoking += "Smokin"+name+".layer_"+S.getFirstName()+";\n";
        		layerRec += S.getType()+" layer_"+S.getFirstName()+";\n";
        		i++;
        	}
        	
        	ecl += "URec"+name+" := RECORD\nUNSIGNED uid;\n"+this.getDatasetName()+";\nEND;\n";
        	ecl += "URec"+name+" Trans"+name+"("+this.getDatasetName()+" L, INTEGER C) := TRANSFORM\nSELF.uid := C;\nSELF := L;\nEND;\n"; 
        	ecl += "MyDS"+name+" := PROJECT("+getDatasetName()+",Trans"+name+"(LEFT,COUNTER));\n";
        	ecl += "NormRec"+name+":=RECORD\nINTEGER4 id;\nINTEGER4 number;\nSTRING field;\nREAL value;\nEND;\n";
        	if(filter.length() == 2)
        		ecl += "OutDS"+name+":=NORMALIZE(MyDS"+name+","+rows.size()+",TRANSFORM(NormRec"+name+"," +
        				"SELF.id:=LEFT.uid;SELF.number:=COUNTER;SELF.field:=CHOOSE(COUNTER,"+field+");" +
        				"SELF.value:=CHOOSE(COUNTER,"+value+")));\n";
        	else{
        		ecl += "OutDS1"+name+":=NORMALIZE(MyDS"+name+","+rows.size()+",TRANSFORM(NormRec"+name+"," +
        				"SELF.id:=LEFT.uid;SELF.number:=COUNTER;SELF.field:=CHOOSE(COUNTER,"+field+");" +
        				"SELF.value:=CHOOSE(COUNTER,"+value+")));\n";
        		ecl += "OutDS"+name+":=OutDS1"+name+"("+filter+");\n";
        	}
        		
        	
        	if(layers.size()>0){
        		ecl += "LayerRec"+name+" := RECORD\nINTEGER4 id;\n"+layerRec+"END;\n";
        		ecl += "LayerDS"+name+" := NORMALIZE(MyDS"+name+",1,TRANSFORM(LayerRec"+name+",SELF.id:=LEFT.uid;"+layer+"));\n";
        	}
        	
        	ecl += "RankableField"+name+" := RECORD\nOutDS"+name+";\nUNSIGNED pos:=0;\nEND;\n";
        	ecl += "T"+name+":=TABLE(SORT(OutDS"+name+",number,field,value),RankableField"+name+");\n";
        	ecl += "TYPEOF(T"+name+") add_rank"+name+"(T"+name+" le, UNSIGNED c):=TRANSFORM\nSELF.pos:=c;\nSELF:=le;\nEND;\n";
        	ecl += "P"+name+":=PROJECT(T"+name+",add_rank"+name+"(LEFT,COUNTER));\n";
        	ecl += "RS"+name+":=RECORD\nSeq:=MIN(GROUP,P"+name+".pos);\nP"+name+".number;\nEND;\n";
        	ecl += "Splits"+name+":= TABLE(P"+name+",RS"+name+",number,FEW);\n";
        	ecl += "TYPEOF(T"+name+") to"+name+"(P"+name+" le, Splits"+name+" ri):=TRANSFORM\nSELF.pos:=1+le.pos-ri.Seq;\nSELF:=le;\nEND;\n";
        	ecl += "outfile"+name+":= JOIN(P"+name+",Splits"+name+",LEFT.number=RIGHT.number,to"+name+"(LEFT,RIGHT),LOOKUP);\n";
        	
        	ecl += "N"+name+":=COUNT("+getDatasetName()+");\n";
        	ecl += "SET OF INTEGER buckets"+name+" := ["+buck+"];\n";
 
        	ecl += "NewTab"+name+":=DEDUP(outfile"+name+", LEFT.field=RIGHT.field AND LEFT.value=RIGHT.value);\n";
        	
        	
        	ecl += "NewRec"+name+":=RECORD\nNewTab"+name+";\nREAL bucket;\nEND;\n";
        	ecl += "NewRec"+name+" Tr"+name+"(NewTab"+name+" L, INTEGER C) := TRANSFORM\nSELF.bucket := ROUNDUP(L.pos*buckets"+name+"[L.number]/(N"+name+"+1))-1;\nSELF := L;\nEND;\n";
        	ecl += "Tab"+name+" := PROJECT(NewTab"+name+",Tr"+name+"(LEFT,COUNTER));\n";
        	ecl += "Rec1"+name+" := RECORD\nTab"+name+".value;\nTab"+name+".bucket;\nTab"+name+".field;\nTab"+name+".number;\nEND;\n";
        	ecl += "Tab1"+name+" := TABLE(Tab"+name+",Rec1"+name+");\n";
        	ecl += "Tab2"+name+" := DEDUP(JOIN(Outfile"+name+",Tab1"+name+",LEFT.field = RIGHT.field AND LEFT.value = RIGHT.value),LEFT.field = RIGHT.field AND LEFT.pos = RIGHT.pos);\n";
        	ecl += "TabRec"+name+" := RECORD\nINTEGER id;\nREAL left_bucket;\nREAL right_bucket;\nSTRING left_field;\nREAL left_value;\nSTRING right_field;\nREAL right_value;\nEND;\n";
        	ecl += "TabRec"+name+" rt"+name+"(Tab2"+name+" L, Tab2"+name+" R) := TRANSFORM\nSELF.id := L.id;\nSELF.left_bucket := L.bucket;\nSELF.right_bucket := R.bucket;\n" +
        		"SELF.left_field := L.field;SELF.left_value := L.value;\nSELF.right_field := R.field;\nSELF.right_value := R.value;\nEND;\n";
        	ecl += "Jhakaas"+name+" := JOIN(Tab2"+name+",Tab2"+name+",LEFT.id = RIGHT.id AND LEFT.field < RIGHT.field ,rt"+name+"(LEFT,RIGHT));\n";
        	
        	if(layers.size()>0){
        		ecl += "Smokin"+name+" := JOIN(Jhakaas"+name+",LayerDS"+name+",LEFT.id = RIGHT.id, FULL OUTER);\n";
        		ecl += "MyRec"+name+" := RECORD\nSmokin"+name+".left_bucket;\n" +
        				"Smokin"+name+".right_bucket;\nSmokin"+name+".left_field;\nSmokin"+name+".right_field;\n" +
        				smoking + "INTEGER cnt := COUNT(GROUP);\nEND;\n";
        		ecl += getOutName()+" := TABLE(Smokin"+name+",MyRec"+name+",left_bucket,right_bucket,left_field,right_field,"+group+");\n";
        		if(persist.equalsIgnoreCase("true")){
            		if(outputName != null && !(outputName.trim().equals(""))){
            			ecl += "OUTPUT("+getOutName()+",,'~"+outputName+"::"+getOutName()+"', __compressed__, overwrite,NAMED('TabulateBuckets'))"+";\n";
            		}else{
            			ecl += "OUTPUT("+getOutName()+",,'~"+defJobName+"::"+getOutName()+"', __compressed__, overwrite,NAMED('TabulateBuckets'))"+";\n";
            		}
            	}
            	else{
            		ecl += "OUTPUT("+getOutName()+",NAMED('TabulateBuckets'));\n";
            	}
        		//ecl += "OUTPUT(MyTable,THOR);\n";
        	}
        	else{
        	      	
	        	ecl += "MyRec"+name+" := RECORD\nJhakaas"+name+".left_bucket;\nJhakaas"+name+".right_bucket;\nJhakaas"+name+".left_field;\nJhakaas"+name+".right_field;\nINTEGER cnt := COUNT(GROUP);\nEND;\n";
	        	ecl += getOutName()+" := SORT(TABLE(Jhakaas"+name+",MyRec"+name+",left_bucket,right_bucket,left_field,right_field),left_field,right_field);\n";
	        	
	        	if(persist.equalsIgnoreCase("true")){
            		if(outputName != null && !(outputName.trim().equals(""))){
            			ecl += "OUTPUT("+getOutName()+",,'~"+outputName+"::TabulateBuckets_"+getOutName()+"', __compressed__, overwrite,NAMED('TabulateBuckets'))"+";\n";
            		}else{
            			ecl += "OUTPUT("+getOutName()+",,'~"+defJobName+"::TabulateBuckets_"+getOutName()+"', __compressed__, overwrite,NAMED('TabulateBuckets'))"+";\n";
            		}
            	}
            	else{
            		ecl += "OUTPUT("+getOutName()+",NAMED('TabulateBuckets'));\n";
            	}        	
        	}
        	result.setResult(true);
            
            RowMetaAndData data = new RowMetaAndData();
            data.addValue("ecl", Value.VALUE_TYPE_STRING, ecl);
            
            
            List list = result.getRows();
            list.add(data);
            String eclCode = parseEclFromRowData(list);
            result.setRows(list);
            result.setLogText("ECLTabulate executed, ECL code added");
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
    		out +=  p.getFirstName()+","+p.getType();
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
        		if(S.length == 2)
        			P.setType(S[1]);
        		else
        			P.setType("");
        		people.add(P);
        	}
        }
    }
    public String saveRows(){
    	String out = "";
    	
    	Iterator it = rows.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getBuckets()+","+p.getType()+","+p.getRule();
            isFirst = false;
    	}
    	return out;
    }

    public void openRows(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	rows = new ArrayList();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		if(S.length > 1)
        			P.setBuckets(S[1]);
        		else
        			P.setBuckets(" ");
        		if(S.length > 2)
        			P.setType(S[2]);
        		else
        			P.setType("");
        		if(S.length == 4)
        			P.setRule(S[3]);
        		else
        			P.setRule("");
        		rows.add(P);
        	}
        }
    }
    public String saveLayers(){
    	String out = "";
    	
    	Iterator it = layers.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Player p = (Player) it.next();
    		out +=  p.getFirstName()+","+p.getType();
            isFirst = false;
    	}
    	return out;
    }

    public void openLayers(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	layers = new ArrayList();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Player P = new Player();
        		P.setFirstName(S[0]);
        		if(S.length == 2)
        			P.setType(S[1]);
        		else
        			P.setType("");
        		layers.add(P);
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
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "out_name")) != null)
                setOutName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "out_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")) != null)
                openPeople(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rows")) != null)
                openRows(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rows")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layers")) != null)
                openLayers(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layers")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")) != null)
                setLabel(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outputName")) != null)
                setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outputName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")) != null)
                setPersistOutputChecked(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")) != null)
                setDefJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
                openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "		<dataset_name ><![CDATA[" + DatasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<out_name eclIsDef=\"true\" eclType = \"dataset\"><![CDATA[" + OutName + "]]></out_name>" + Const.CR;
        retval += "		<people><![CDATA[" + this.savePeople() + "]]></people>" + Const.CR;
        retval += "		<rows><![CDATA[" + this.saveRows() + "]]></rows>" + Const.CR;
        retval += "		<layers><![CDATA[" + this.saveLayers() + "]]></layers>" + Const.CR;
        retval += "		<label><![CDATA[" + label + "]]></label>" + Const.CR;
        retval += "		<outputName><![CDATA[" + outputName + "]]></outputName>" + Const.CR;
        retval += "		<persist_Output_Checked><![CDATA[" + persist + "]]></persist_Output_Checked>" + Const.CR;
        retval += "		<defJobName><![CDATA[" + defJobName + "]]></defJobName>" + Const.CR;
        retval += "		<recordList><![CDATA[" + this.saveRecordList() + "]]></recordList>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	
        	if(rep.getStepAttributeString(id_jobentry, "dataset_name") != null)
                DatasetName = rep.getStepAttributeString(id_jobentry, "dataset_name"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "out_name") != null)
                OutName = rep.getStepAttributeString(id_jobentry, "out_name"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobentry, "people") != null)
                this.openPeople(rep.getStepAttributeString(id_jobentry, "people")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "rows") != null)
                this.openRows(rep.getStepAttributeString(id_jobentry, "rows")); //$NON-NLS-1$
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
            if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
            	this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	rep.saveStepAttribute(id_job, getObjectId(), "dataset_name", DatasetName); //$NON-NLS-1$
        	rep.saveStepAttribute(id_job, getObjectId(), "out_name", OutName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "rows", this.saveRows()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "layers", this.saveLayers()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
        	rep.saveStepAttribute(id_job, getObjectId(), "recordList", saveRecordList());
            
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
