/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltconcept;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.saltui.concept.*;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.ecljobentrybase.*;
import org.hpccsystems.javaecl.SaltHygieneReport;
/**
 *
 * @author ChambersJ
 */
public class SALTConcept extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
	
    private String datasetName;
    private String layout;
    private String cleanData;
	//private String rules;
	private ConceptEntryList entryList = new ConceptEntryList();
	
	public String getDatasetName() {
		return datasetName;
	}
	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}
	public String getLayout() {
		return layout;
	}
	public void setLayout(String layout) {
		this.layout = layout;
	}
    
	public ConceptEntryList getEntryList() {
		return entryList;
	}
	public void setEntryList(ConceptEntryList entryList) {
		this.entryList = entryList;
	}
	
	
	
	public String getCleanData() {
		return cleanData;
	}
	public void setCleanData(String cleanData) {
		this.cleanData = cleanData;
	}
	public String saveEntryList(){
        String out = "";
        ArrayList<ConceptEntryBO> list = entryList.getEntries();
        Iterator<ConceptEntryBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        
        //fieldTypes
        return out;
    }
    
    public void openEntryList(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        if(len>0){
            entryList = new ConceptEntryList();
            for(int i =0; i<len; i++){
                ConceptEntryBO eb = new ConceptEntryBO(strLine[i]);
                entryList.addEntryBO(eb);
            }
        }
        
        //fieldTypes
    }
    
    public String saveFieldTypeList(){
        String out = "";
        //ArrayList<ConceptEntryBO> list = entryList.getFields();
       // Iterator<ConceptEntryBO> itr = list.iterator();
        boolean isFirst = true;
       /* while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }*/
        
        //fieldTypes
        return out;
    }
    
    public void openFieldTypeList(String in){
        String[] strLine = in.split("[|]");
        int len = strLine.length;
        
        if(len>0){
           // fieldTypeList = new ConceptRuleList();
            for(int i =0; i<len; i++){
               // ConceptRuleBO ft = new ConceptRuleBO(strLine[i]);
                //ft.fromCSV(strLine[i]);
                //fieldTypeList.add(ft);
            }
        }
        
        //fieldTypes
    }
    
    
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
		Result result = modifyResults(prevResult);
		if(result.isStopped()){
        	return result;
 		}
        return result;
    }
 
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "datasetName")) != null)
            	setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "datasetName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layout")) != null)
            	setLayout(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "layout")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "entryList")) != null)
                openEntryList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "entryList")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fieldTypeList")) != null)
                openFieldTypeList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fieldTypeList")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "cleanedData")) != null)
                setCleanData(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "cleanedData")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "		<datasetName><![CDATA[" + datasetName + "]]></datasetName>" + Const.CR;
        retval += "		<layout><![CDATA[" + layout + "]]></layout>" + Const.CR;
        retval += "		<entryList><![CDATA[" + this.saveEntryList() + "]]></entryList>" + Const.CR;
        retval += "		<fieldTypeList><![CDATA[" + this.saveFieldTypeList() + "]]></fieldTypeList>" + Const.CR;
        retval += "		<cleanedData><![CDATA[" + this.cleanData + "]]></cleanedData>" + Const.CR;
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "layout") != null)
                layout = rep.getStepAttributeString(id_jobentry, "layout"); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "entryList") != null)
                this.openEntryList(rep.getStepAttributeString(id_jobentry, "entryList")); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "fieldTypeList") != null)
                this.openFieldTypeList(rep.getStepAttributeString(id_jobentry, "fieldTypeList")); //$NON-NLS-1$
            
            if(rep.getStepAttributeString(id_jobentry, "cleanedData") != null)
                this.setCleanData(rep.getStepAttributeString(id_jobentry, "cleanedData")); //$NON-NLS-1$
        
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "layout", layout); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "entryList", this.saveEntryList()); //$NON-NLS-1$.
            rep.saveStepAttribute(id_job, getObjectId(), "fieldTypeList", this.saveFieldTypeList()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "cleanedData", this.getCleanData()); //$NON-NLS-1$
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }


}
