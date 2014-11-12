/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclaggregate;

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
public class ECLAggregate extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String datasetName = "";
    private java.util.List people = new ArrayList();
    private java.util.List group = new ArrayList();
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
	
	public void setGroup(java.util.List group){
		this.group = group;
	}
	
	public java.util.List getGroup(){
		return group;
	}

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
    	Result result = prevResult;
        if(result.isStopped()){
        	return result;
        }
        else{
        	String name = getName().replaceAll("\\s", "");  
        	String ecl = name+"AggRec := RECORD\n";String grprec = "";boolean flag = false;
        	if(group.size() > 0){
        		flag = true;        	
	        	for(Iterator<Cols> it = group.iterator();it.hasNext();){
	        		Cols C = (Cols) it.next();
	        		ecl += getDatasetName()+"."+C.getFirstName()+";\n";
	        		grprec += C.getFirstName()+",";
	        	}
        	}
        	
        	for(Iterator<Cols> it = people.iterator();it.hasNext();){
        		Cols C = (Cols) it.next();
        		String sb = null;
        		if(C.getRule() != null){
        			sb  = C.getRule().replaceFirst("\\(", "("+getDatasetName()+".");
        		}
        		switch(C.getOperator()){
        		case 0:
        			if(sb.length()>0)
        				ecl += C.getFirstName()+"_sum := SUM(GROUP,"+getDatasetName()+"."+C.getFirstName()+","+sb+");\n";
        			else
        				ecl += C.getFirstName()+"_sum := SUM(GROUP,"+getDatasetName()+"."+C.getFirstName()+");\n";
        			break;
        		
	        	case 1:
	    			if(sb.length()>0)
	    				ecl += C.getFirstName()+"_ave := AVE(GROUP,"+getDatasetName()+"."+C.getFirstName()+","+sb+");\n";
	    			else
	    				ecl += C.getFirstName()+"_ave := AVE(GROUP,"+getDatasetName()+"."+C.getFirstName()+");\n";
	    			break;
	        	case 2:
        			if(sb.length()>0)
        				ecl += C.getFirstName()+"_count := COUNT(GROUP,"+sb+");\n";
        			else
        				ecl += C.getFirstName()+"_count := COUNT(GROUP);\n";
        			break;
        			
        		}
        	}
        	ecl += "END;\n";
        	if(flag)
        		ecl += OutName+" := TABLE("+getDatasetName()+","+name+"AggRec,"+grprec.substring(0, grprec.length()-1)+");\n";
        	else
        		ecl += OutName+" := TABLE("+getDatasetName()+","+name+"AggRec);\n";
        	if(persist.equalsIgnoreCase("true")){
        		if(outputName != null && !(outputName.trim().equals(""))){
        			ecl += "OUTPUT("+OutName+",,'~"+outputName+"::aggregate', __compressed__, overwrite,NAMED('Aggregate'));\n";
        		}
        		else{
        			ecl += "OUTPUT("+OutName+",,'~"+defJobName+"::aggregate', __compressed__, overwrite,NAMED('Aggregate'));\n";
        		}
            }
            else{
            	ecl += "OUTPUT("+OutName+",THOR);\n";
            }	
        	
        	result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, ecl);
	        
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	        result.setLogText("ECLUnivariate executed, ECL code added");
        	return result;

        }
    }

    public String savePeople(){
    	String out = "";
    	
    	Iterator it = people.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Cols p = (Cols) it.next();
    		out +=  p.getFirstName()+","+p.getType()+","+p.getOperator()+","+p.getRule();
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
        		Cols P = new Cols();
        		P.setFirstName(S[0]);
        		P.setType(S[1]);
        		P.setOperator(Integer.parseInt(S[2]));
        		if(S.length > 3)
        			P.setRule(S[3]);
        		else
        			P.setRule("");
        		people.add(P);
        	}
        }
    }

    public String saveGroup(){
    	String out = "";
    	
    	Iterator it = group.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Cols p = (Cols) it.next();
    		out +=  p.getFirstName()+","+p.getType();
            isFirst = false;
    	}
    	return out;
    }

    public void openGroup(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	group = new ArrayList();
        	for(int i = 0; i<len; i++){
        		String[] S = strLine[i].split(",");
        		Cols P = new Cols();
        		P.setFirstName(S[0]);
        		P.setType(S[1]);
        		group.add(P);
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
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")) != null)
                openPeople(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "group")) != null)
                openGroup(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "group")));
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
        retval += "		<people><![CDATA[" + this.savePeople() + "]]></people>" + Const.CR;
        retval += "     <group eclIsGroup=\"true\"><![CDATA[" + this.saveGroup() + "]]></group>" + Const.CR;
        retval += "		<dataset_name><![CDATA[" + datasetName + "]]></dataset_name>" + Const.CR;		
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
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "people") != null)
                this.openPeople(rep.getStepAttributeString(id_jobentry, "people")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "group") != null)
                this.openGroup(rep.getStepAttributeString(id_jobentry, "group")); //$NON-NLS-1$
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
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "group", this.saveGroup()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
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
