/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclnewreportbuilder;

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
public class ECLNewReportBuilder extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String datasetName = "";
    private ArrayList<String> items = new ArrayList<String>();
    private ArrayList<String> types = new ArrayList<String>();
    private java.util.List people = new ArrayList();
    private ArrayList<String> vars = new ArrayList<String>();
    private String label ="";
	private String outputName ="";
	private String persist = "";	
	private String defJobName = "";
    private String reportname = "";
	private String rule = "";	
	private RecordList recordList = new RecordList();

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

	public RecordList getRecordList() {
		return recordList;
	}

	public void setRecordList(RecordList recordList) {
		this.recordList = recordList;
	}

	public String getRule() {
		return rule;
	}

	public void setRule(String rule) {
		this.rule = rule;
	}
    
	public String getReportname() {
		return reportname;
	}

	public void setReportname(String reportname) {
		this.reportname = reportname;
	}

	public ArrayList<String> getVars() {
		return vars;
	}

	public void setVars(ArrayList<String> vars) {
		this.vars = vars;
	}

	public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public ArrayList<String> getItems() {
        return items;
    }

	public void setPeople(java.util.List people){
		this.people = people;
	}
	
	public java.util.List getPeople(){
		return people;
	}

    public void setItems(ArrayList<String> items) {
        this.items = items;
    }

    public ArrayList<String> getTypes() {
        return types;
    }

    public void setTypes(ArrayList<String> types) {
        this.types = types;
    }


    @Override
    public Result execute(Result prevResult, int k) throws KettleException {    	  
    	Result result = prevResult;
        if(result.isStopped()){
        	return result;
        }
        else{
        	String report = "RepDS := "+getDatasetName()+"("+getRule()+");\n";
        	report += getName().replaceAll("\\s", "")+"Rec := RECORD\nRepDS;\n";boolean flag = false;
        	for(Iterator it = people.iterator(); it.hasNext();){
        		Person P = (Person) it.next();
        		String varName = P.getVariableName().trim();
        		vars.add(varName.toLowerCase());
        		report += "	REAL8 "+varName+";\n";        		
        	}
        	report += "END;\n";
        	report += getName().replaceAll("\\s", "")+"Rec "+getName().replaceAll("\\s", "")+"Trans(RepDS L, INTEGER C) := TRANSFORM\n";

        	for(Iterator it = people.iterator(); it.hasNext();){
        		String S1 = "";String[] S = new String[]{};
        		Person P = (Person) it.next();
        		String fields = P.getFields().trim();        		
        		int operator = P.getOP();
        		String varName = P.getVariableName().trim();
        		
        		
        		
        		switch(operator){
        		case 1:
        			S1 = "SELF."+varName+" := ";
        			S = fields.split(",");
        			for(String s : S){
        				if(!vars.contains(s.toLowerCase()))
        				{
        					if(s.contains("(") && s.contains(")")){
        						if(!s.toLowerCase().startsWith("count"))
        							S1 += new StringBuilder(s).insert(s.indexOf("(")+1, "RepDS,RepDS.").toString()+"/";
        						else{
        							S1 += "COUNT(RepDS)+"; 
        						}
        							
            				}
        					else
        						S1 += "L."+s+"+";
        				}
        				else
        					S1 += "SELF."+s+"+";
        			}
        			S1 = S1.substring(0,S1.length()-1);
        			report += S1+";\n";
        			break;
        		case 2:
        			S1 = "SELF."+varName+" := ";
        			S = fields.split(",");
        			for(String s : S){
        				if(!vars.contains(s.toLowerCase()))
        				{
        					if(s.contains("(") && s.contains(")")){
        						if(!s.toLowerCase().startsWith("count"))
        							S1 += new StringBuilder(s).insert(s.indexOf("(")+1, "RepDS,RepDS.").toString()+"/";
        						else{
        							S1 += "COUNT(RepDS)-"; 
        						}
        							
            				}
        					else
        						S1 += "L."+s+"-";
        				}
        				else
        					S1 += "SELF."+s+"-";
        			}
        			S1 = S1.substring(0,S1.length()-1);
        			report += S1+";\n";
        			break;
        		case 3:
        			S1 = "SELF."+varName+" := ";
        			S = fields.split(",");
        			for(String s : S){
        				if(!vars.contains(s.toLowerCase())){
        					if(s.contains("(") && s.contains(")")){
        						if(!s.toLowerCase().startsWith("count"))
        							S1 += new StringBuilder(s).insert(s.indexOf("(")+1, "RepDs,RepDS.").toString()+"/";
        						else{
        							S1 += "COUNT(RepDS)*"; 
        						}
        							
            				}
        					else
        						S1 += "L."+s+"*";
        				}
        					
        				else
        					S1 += "SELF."+s+"*";
        			}
        			S1 = S1.substring(0,S1.length()-1);
        			report += S1+";\n";
        			break;
        		case 4:
        			S1 = "SELF."+varName+" := ";
        			S = fields.split(",");
        			for(String s : S){
        				if(!vars.contains(s.toLowerCase())){
        					if(s.contains("(") && s.contains(")")){
        						if(!s.toLowerCase().startsWith("count"))
        							S1 += new StringBuilder(s).insert(s.indexOf("(")+1, "RepDS,RepDS.").toString()+"/";
        						else{
        							S1 += "COUNT(RepDS)/"; 
        						}
        							
            				}
        					else
        						S1 += "L."+s+"/";
        				}
        					
        				else
        					S1 += "SELF."+s+"/";
        			}
        			S1 = S1.substring(0,S1.length()-1);
        			report += S1+";\n";
        			break;
        		case 5:
        			S1 = "SELF."+varName+" := ";
        			S = fields.split(",");
        			for(String s : S){
        				if(!vars.contains(s.toLowerCase())){
        					if(s.contains("(") && s.contains(")")){
        						if(!s.toLowerCase().startsWith("count"))
        							S1 += new StringBuilder(s).insert(s.indexOf("(")+1, "RepDS,RepDS.").toString()+"/";
        						else{
        							S1 += "COUNT(RepDS)%"; 
        						}
        							
            				}
        					else
        						S1 += "L."+s+"%";
        				}        					
        				else
        					S1 += "SELF."+s+"%";
        			}
        			S1 = S1.substring(0,S1.length()-1);
        			report += S1+";\n";
        			break;
        		case 6:
        			S1 = "SELF."+varName+" := ";
        			S = fields.split(",");
        			for(String s : S){        				
        				if(!vars.contains(s.toLowerCase())){
        					if(s.contains("(") && s.contains(")")){
        						if(!s.toLowerCase().startsWith("count"))
        							S1 += new StringBuilder(s).insert(s.indexOf("(")+1, "RepDS,RepDS.").toString()+"/";
        						else{
        							S1 += "COUNT(RepDS)/"; 
        						}
        							
            				}
        					else
        						S1 += "L."+s+"/";
        				}
        				else{
        					S1 += "SELF."+s+"/";
        				}
        			}
        			S1 = S1.substring(0,S1.length()-1)+"*100";
        			report += S1+";\n";
        			break;
        		
        		default:
        			break;
        		}
        	}
        	
        	report += "SELF := L;\nEND;\n";
        	report += reportname+" := PROJECT(RepDS,"+getName().replaceAll("\\s", "")+"Trans(LEFT,COUNTER));\n";
        	report += "OUTPUT("+reportname+",NAMED('Report_"+reportname+"'));\n";
        	//report += "OUTPUT(My"+getName()+"DS,NAMED('Report2'));\n";
        	
        	logBasic("ReportBuilder =" + report);//{Dataset Job} 
	        
	        result.setResult(true);
	        
	        RowMetaAndData data = new RowMetaAndData();
	        data.addValue("ecl", Value.VALUE_TYPE_STRING, report);
	        
	        
	        List list = result.getRows();
	        list.add(data);
	        String eclCode = parseEclFromRowData(list);
	        result.setRows(list);
	        result.setLogText("ECLReportBuilder executed, ECL code added");
        	return result;
        }
          
          
    }
        
    public String saveItems(){
    	String out = "";
    	
    	Iterator<String> it = items.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		String p = it.next();
    		out +=  p;
            isFirst = false;
    	}
    	return out;
    }
    
    public void openItems(String in){
    	String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	items = new ArrayList<String>();
        	for(int i = 0; i<len; i++){        		
        		items.add(strLine[i]);
        	}
        }
    }

    public String saveTypes(){
    	String out = "";
    	
    	Iterator<String> it = types.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		String p = it.next();
    		out +=  p;
            isFirst = false;
    	}
    	return out;
    }
    
    public void openTypes(String in){
    	String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
        	types = new ArrayList<String>();
        	for(int i = 0; i<len; i++){        		
        		types.add(strLine[i]);
        	}
        }
    }
    
    public String savePeople(){
    	String out = "";
    	
    	Iterator it = people.iterator();
    	boolean isFirst = true;
    	while(it.hasNext()){
    		if(!isFirst){out+="|";}
    		Person p = (Person) it.next();
    		out +=  p.getVariableName()+"-"+p.getOP().toString()+"-"+p.getFields();
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
        		Person P = new Person();
        		P.setVariableName(S[0]);
        		P.setOP(Integer.parseInt(S[1]));
        		if(S.length>2)
        			P.setFields(S[2]);
        		else
        			P.setFields("");
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
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "report_name")) != null)
                setReportname(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "report_name")));
           	if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "items")) != null)
           		this.openItems(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "items")));//
           	if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "types")) != null)
           		openTypes(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "types")));
           	if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")) != null)
                openPeople(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "people")));
           	if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rule")) != null)
                setRule(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "rule")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
                openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")) != null)
                setOutputName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "output_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")) != null)
                setLabel(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "label")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")) != null)
                setPersistOutputChecked(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "persist_Output_Checked")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")) != null)
                setDefJobName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "defJobName")));
           
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<dataset_name><![CDATA[" + datasetName + "]]></dataset_name>" + Const.CR;
        retval += "		<report_name eclIsDef=\"true\" eclType=\"dataset\"><![CDATA[" + reportname + "]]></report_name>" + Const.CR;
        retval += "		<people><![CDATA[" + this.savePeople() + "]]></people>" + Const.CR;
        retval += "		<items><![CDATA[" + this.saveItems() + "]]></items>" + Const.CR;
        retval += "		<types><![CDATA[" + this.saveTypes() + "]]></types>" + Const.CR;
        retval += "		<label><![CDATA[" + label + "]]></label>" + Const.CR;
        retval += "		<output_name><![CDATA[" + outputName + "]]></output_name>" + Const.CR;
        retval += "		<persist_Output_Checked><![CDATA[" + persist + "]]></persist_Output_Checked>" + Const.CR;
        retval += "		<defJobName><![CDATA[" + defJobName + "]]></defJobName>" + Const.CR;
        retval += "		<rule><![CDATA[" + rule + "]]></rule>" + Const.CR;
        retval += "		<recordList><![CDATA[" + this.saveRecordList() + "]]></recordList>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "reportname") != null)
            	reportname = rep.getStepAttributeString(id_jobentry, "reportname"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "items") != null)
                this.openItems(rep.getStepAttributeString(id_jobentry, "items")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "types") != null)
                this.openTypes(rep.getStepAttributeString(id_jobentry, "types")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "outputName") != null)
            	outputName = rep.getStepAttributeString(id_jobentry, "outputName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "label") != null)
            	label = rep.getStepAttributeString(id_jobentry, "label"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "persist_Output_Checked") != null)
            	persist = rep.getStepAttributeString(id_jobentry, "persist_Output_Checked"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "defJobName") != null)
            	defJobName = rep.getStepAttributeString(id_jobentry, "defJobName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "people") != null)
                this.openPeople(rep.getStepAttributeString(id_jobentry, "people")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "rule") != null)
                rule = rep.getStepAttributeString(id_jobentry, "rule"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
                this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "reportname", reportname); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "items", this.saveItems()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "types", this.saveTypes()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "people", this.savePeople()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "rule", rule); //$NON-NLS-1$
        	rep.saveStepAttribute(id_job, getObjectId(), "recordList", this.saveRecordList()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outputName", outputName);
        	rep.saveStepAttribute(id_job, getObjectId(), "label", label);
        	rep.saveStepAttribute(id_job, getObjectId(), "persist_Output_Checked", persist);
        	rep.saveStepAttribute(id_job, getObjectId(), "defJobName", defJobName);
            
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
