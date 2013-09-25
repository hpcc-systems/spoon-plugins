/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclsort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Sort;
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
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author ChambersJ
 */
public class ECLSort extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String logicalFileName;
    private String datasetName = "";
    //private String fields = "";//Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    private String recordsetName = "";
    private RecordList recordList = new RecordList();
    
    

    public RecordList getRecordList() {
		return recordList;
	}

	public void setRecordList(RecordList recordList) {
		this.recordList = recordList;
	}

	public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }
/*
    public String getLogicalFileName() {
        return logicalFileName;
    }

    public void setLogicalFileName(String fileName) {
        this.logicalFileName = fileName;
    }
  */  
    //public String getFields() {
    //    return fields;
    //}

    //public void setFields(String fields) {
    //    this.fields = fields;
    //}
    
    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }

    
    
    public String recordListToSortECL(){
        String out = "";
        
        if(recordList != null){
            if(recordList.getRecords() != null && recordList.getRecords().size() > 0) {
                   
                    for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO record = (RecordBO) iterator.next();
                        	System.out.println("record.getSortOrder()----------------------" + record.getSortOrder());
                            if(!out.equals("")){
                            	out += ",";
                            }
                            if(record.getSortOrder().equalsIgnoreCase("DESENDING")){
                            	out += "-";
                            }
                            out += record.getColumnName();
                            
                            
                    }
            }
        }
        
        return out;
    }
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
    	Result result = modifyResults(prevResult);
    	if(result.isStopped()){
        	return result;
 		}
        Sort sort = new Sort();
        //sort.setFields(getFields());
        sort.setDatasetName(getDatasetName());
        sort.setName(getRecordsetName());
        System.out.println("------------------- GET RECORDS ------------------");
        sort.setFields(recordListToSortECL());
        logBasic("{Sort Job} Execute = " + sort.ecl());
        
        logBasic("{Sort Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, sort.ecl());
        
        
        List list = result.getRows();
        list.add(data);
        String eclCode = parseEclFromRowData(list);
        /*
        String eclCode = "";
        if (list == null) {
            list = new ArrayList();
        } else {
            
            for (int i = 0; i < list.size(); i++) {
                RowMetaAndData rowData = (RowMetaAndData) list.get(i);
                String code = rowData.getString("ecl", null);
                if (code != null) {
                    eclCode += code;
                }
            }
            logBasic("{Sort Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fields")) != null)
                //setFields(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "fields")));
            //setRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "record_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")) != null)
                setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")) != null)
                setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
                openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));
           
        } catch (Exception e) {
            throw new KettleXMLException("ECL Sort Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        
        //retval += "		<record_name>" + recordName + "</record_name>" + Const.CR;
        
        retval += "		<dataset_name ><![CDATA[" + datasetName + "]]></dataset_name>" + Const.CR;
        //retval += "		<fields><![CDATA[" + fields + "]]></fields>" + Const.CR;
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<recordList><![CDATA[" + this.saveRecordList() + "]]></recordList>" + Const.CR;

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //if(rep.getStepAttributeString(id_jobentry, "fields") != null)
            //    fields = rep.getStepAttributeString(id_jobentry, "fields"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "datasetName") != null)
                datasetName = rep.getStepAttributeString(id_jobentry, "datasetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "recordset_name") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordset_name"); //$NON-NLS-1$
            //recordName = rep.getStepAttributeString(id_jobentry, "recordName"); //$NON-NLS-1$
            //recordDef = rep.getStepAttributeString(id_jobentry, "recordDef"); //$NON-NLS-1$
        
            if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
                this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "fields", fields); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "datasetName", datasetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordset_name", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordList", this.saveRecordList()); //$NON-NLS-1$
           // rep.saveStepAttribute(id_job, getObjectId(), "recordName", recordName); //$NON-NLS-1$
           // rep.saveStepAttribute(id_job, getObjectId(), "recordDef", recordDef); //$NON-NLS-1$
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
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
    /*
    public RecordList ArrayListToRecordList(ArrayList<String[]> in){
    	
    	RecordList recordList = null;
  
        
        if(in.size()>0){
            recordList = new RecordList();
            for(int i =0; i<in.size(); i++){
                RecordBO rb = new RecordBO();
                rb.setColumnName(in.get(i)[0]);
                //rb.setColumnType(in.get(i)[1].replaceAll("\\d+",""));//replaces digit with "" so we get STRING/INTEGER etc
                //System.out.println("Letters: " + x.replaceAll("\\d+[_]*",""));
                rb.setColumnType(in.get(i)[1].replaceAll("\\d+[_]*",""));//replaces digit with "" so we get STRING/INTEGER etc
                
                //rb.setColumnWidth(in.get(i)[1].replaceAll("\\D+",""));//replace non digit with "" so we get just number 
                //System.out.println("Numbers: " + x.replaceAll("[^0-9_]+",""));
                rb.setColumnWidth(in.get(i)[1].replaceAll("[^0-9_]+",""));//replace non digit with "" so we get just number 
                
                rb.setDefaultValue(in.get(i)[2]);
                rb.setSortOrder(in.get(i)[4]);
                recordList.addRecordBO(rb);
            }
        }
        return recordList;
    }
*/
   
}
