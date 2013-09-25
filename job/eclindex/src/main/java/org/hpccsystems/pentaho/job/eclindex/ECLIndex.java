package org.hpccsystems.pentaho.job.eclindex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Index;
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
public class ECLIndex extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private RecordList keys = new RecordList();
    private RecordList payload = new RecordList();
    
    private String baserecset;
    private String indexfile;
    private String sorted;
    private String preload;
    private String opt;
    private String compressed;
    private String distributed;
    private String index;
    private String newindexfile;
    private String isDuplicate;
    private String overwrite;

    public String getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(String overwrite) {
        this.overwrite = overwrite;
    }

    public String getIsDuplicate() {
        return isDuplicate;
    }

    public void setIsDuplicate(String isDuplicate) {
        this.isDuplicate = isDuplicate;
    }
    
    public RecordList getKeys() {
        return keys;
    }

    public void setKeys(RecordList keys) {
        this.keys = keys;
    }

    public RecordList getPayload() {
        return payload;
    }

    public void setPayload(RecordList payload) {
        this.payload = payload;
    }
    
    public String getBaserecset() {
        return baserecset;
    }

    public void setBaserecset(String baserecset) {
        this.baserecset = baserecset;
    }

    public String getCompressed() {
        return compressed;
    }

    public void setCompressed(String compressed) {
        this.compressed = compressed;
    }

    public String getDistributed() {
        return distributed;
    }

    public void setDistributed(String distributed) {
        this.distributed = distributed;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getIndexfile() {
        return indexfile;
    }

    public void setIndexfile(String indexfile) {
        this.indexfile = indexfile;
    }

    public String getNewindexfile() {
        return newindexfile;
    }

    public void setNewindexfile(String newindexfile) {
        this.newindexfile = newindexfile;
    }

    public String getOpt() {
        return opt;
    }

    public void setOpt(String opt) {
        this.opt = opt;
    }


    public String getPreload() {
        return preload;
    }

    public void setPreload(String preload) {
        this.preload = preload;
    }

    public String getSorted() {
        return sorted;
    }

    public void setSorted(String sorted) {
        this.sorted = sorted;
    }

    public String resultListToString(RecordList recordList){
        String out = "";
        
        if(recordList != null){
            if(recordList.getRecords() != null && recordList.getRecords().size() > 0) {
                    for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO record = iterator.next();
                            if(!out.equals("")){
                                out += ",";
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
            
        }else{
        
            Index index = new Index();
            index.setName(this.getName());
            index.setOverwrite(this.getOverwrite());
            if(isDuplicate.equals("Yes")){
                index.setIndex(this.index);
                index.setNewindexfile(newindexfile);
            }else{
                index.setBaserecset(baserecset);
                index.setKeys(resultListToString(keys));
                index.setPayload(resultListToString(payload));
                index.setIndexfile(indexfile);
                index.setSorted(sorted);
                index.setPreload(preload);
                index.setOpt(opt);
                index.setCompressed(compressed);
                index.setDistributed(distributed);
            }
            
            result.setResult(true);

            RowMetaAndData data = new RowMetaAndData();
            data.addValue("ecl", Value.VALUE_TYPE_STRING, index.ecl());
           

            List<RowMetaAndData> list = result.getRows();
            list.add(data);
            String eclCode = "";
            try{
                for (int i = 0; i < list.size(); i++) {
                    RowMetaAndData rowData = list.get(i);
                    
                    String code = rowData.getString("ecl", null);
                    if (code != null) {
                        eclCode += code;
                    }
                    
                 }
            }catch (Exception e){

            }
            logBasic("{Index Job} ECL Code (below):\r\n" + eclCode);

            result.setRows(list);
        }
        
        return result;
    }
    
    
    public String saveKeys(){
        String out = "";
        List<RecordBO> list = keys.getRecords();
        Iterator<RecordBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
    public void openKeys(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            keys = new RecordList();
            for(int i =0; i<len; i++){
                RecordBO rb = new RecordBO(strLine[i]);
                keys.addRecordBO(rb);
            }
        }
    }
    
    public String savePayload(){
        String out = "";
        ArrayList<RecordBO> list = payload.getRecords();
        Iterator<RecordBO> itr = list.iterator();
        boolean isFirst = true;
        while(itr.hasNext()){
            if(!isFirst){out+="|";}
            
            out += itr.next().toCSV();
            isFirst = false;
        }
        return out;
    }
    
    public void openPayload(String in){
        String[] strLine = in.split("[|]");
        
        int len = strLine.length;
        if(len>0){
            payload = new RecordList();
            for(int i =0; i<len; i++){
                RecordBO rb = new RecordBO(strLine[i]);
                payload.addRecordBO(rb);
            }
        }
    }
    
    
    public String loadXMLElement(Node node,String key){
        String rStr = "";
        if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, key)) != null){
                rStr = XMLHandler.getNodeValue(XMLHandler.getSubNode(node, key));
        }
        return rStr;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            
            this.setBaserecset(loadXMLElement(node,"baserecset"));
           
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "keys")) != null)
                openKeys(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "keys")));
             if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "payload")) != null)
                openPayload(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "payload")));
            
            this.setIndexfile(loadXMLElement(node,"indexfile"));
            this.setSorted(loadXMLElement(node,"sorted"));
            this.setPreload(loadXMLElement(node,"preload"));
            this.setOpt(loadXMLElement(node,"opt"));
            this.setCompressed(loadXMLElement(node,"compressed"));
            this.setDistributed(loadXMLElement(node,"distributed"));
            this.setIndex(loadXMLElement(node,"index"));
            this.setNewindexfile(loadXMLElement(node,"newindexfile"));
            this.setIsDuplicate(loadXMLElement(node,"isDuplicate"));
            this.setOverwrite(loadXMLElement(node,"overwrite"));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Join Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXMLElement(String key, String value){
    	
        return "        <" + key + "><![CDATA["+value+"]]></"+key+">"+ Const.CR;
    }
    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        

         retval += getXMLElement("baserecset", this.getBaserecset());
         retval += "		<keys eclIsDef=\"true\" eclType=\"keys\"><![CDATA[" + this.saveKeys() + "]]></keys>" + Const.CR;
         retval += "		<payload eclIsDef=\"true\" eclType=\"payload\"><![CDATA[" + this.savePayload() + "]]></payload>" + Const.CR;
         retval += getXMLElement("indexfile", this.getIndexfile());
         retval += getXMLElement("sorted", this.getSorted());
         retval += getXMLElement("preload", this.getPreload());
         retval += getXMLElement("opt", this.getOpt());
         retval += getXMLElement("compressed", this.getCompressed());
         retval += getXMLElement("distributed", this.getDistributed());
         retval += getXMLElement("index", this.getIndex());
         retval += getXMLElement("newindexfile", this.getNewindexfile());
         retval += getXMLElement("isDuplicate", this.getIsDuplicate());
         retval += getXMLElement("overwrite", this.getOverwrite());
         
        return retval;

    }
    public String getRepElement(Repository rep, ObjectId id_jobentry, String key) throws KettleException{
        String rStr = "";
        try{
        if(rep.getStepAttributeString(id_jobentry, key) != null)
                rStr = rep.getStepAttributeString(id_jobentry, key); //$NON-NLS-1$
        

        }catch (Exception e){
             e.printStackTrace();
            throw new KettleException("Unexpected Exception", e);
        }
        
        
        return rStr;
    }
    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            this.setBaserecset(getRepElement(rep, id_jobentry, "baserecset"));
             if(rep.getStepAttributeString(id_jobentry, "keys") != null)
                this.openKeys(rep.getStepAttributeString(id_jobentry, "keys")); //$NON-NLS-1$
             if(rep.getStepAttributeString(id_jobentry, "payload") != null)
                this.openPayload(rep.getStepAttributeString(id_jobentry, "payload")); //$NON-NLS-1$
            this.setIndexfile(getRepElement(rep, id_jobentry, "indexfile"));
            this.setSorted(getRepElement(rep, id_jobentry, "sorted"));
            this.setPreload(getRepElement(rep, id_jobentry, "preload"));
            this.setOpt(getRepElement(rep, id_jobentry, "opt"));
            this.setCompressed(getRepElement(rep, id_jobentry, "compressed"));
            this.setDistributed(getRepElement(rep, id_jobentry, "distributed"));
            this.setIndex(getRepElement(rep, id_jobentry, "index"));
            this.setNewindexfile(getRepElement(rep, id_jobentry, "newindexfile"));
            this.setIsDuplicate(getRepElement(rep, id_jobentry, "isDuplicate"));
            this.setOverwrite(getRepElement(rep, id_jobentry, "overwrite"));
                
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRepElement(Repository rep, ObjectId id_job,String key,String value) throws KettleException{
        try{
            rep.saveStepAttribute(id_job, getObjectId(), key, value); //$NON-NLS-1$
        }catch (Exception e){
             e.printStackTrace();
             throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }
    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
           saveRepElement(rep,id_job,"baserecset",this.getBaserecset());
           rep.saveStepAttribute(id_job, getObjectId(), "keys", this.saveKeys()); //$NON-NLS-1$
           rep.saveStepAttribute(id_job, getObjectId(), "payload", this.savePayload()); //$NON-NLS-1$
           saveRepElement(rep,id_job,"indexfile",this.getIndexfile());
           saveRepElement(rep,id_job,"sorted",this.getSorted());
           saveRepElement(rep,id_job,"preload",this.getPreload());
           saveRepElement(rep,id_job,"opt",this.getOpt());
           saveRepElement(rep,id_job,"compressed",this.getCompressed());
           saveRepElement(rep,id_job,"distributed",this.getDistributed());
           saveRepElement(rep,id_job,"index",this.getIndex());
           saveRepElement(rep,id_job,"newindexfile",this.getNewindexfile());
           saveRepElement(rep,id_job,"isDuplicate",this.getIsDuplicate());
           saveRepElement(rep,id_job,"overwrite",this.getOverwrite());
           
           
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    
}
