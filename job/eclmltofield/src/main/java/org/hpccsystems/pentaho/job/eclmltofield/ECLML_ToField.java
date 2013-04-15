/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclmltofield;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.ToField;
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
public class ECLML_ToField extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    private String inDS = "";
    private String outDS = "";
    private String idColumn = "";
    private String parseToNumeric = "";

    public String getInDS() {
        return inDS;
    }

    public void setInDS(String inDS) {
        this.inDS = inDS;
    }

    public String getOutDS() {
        return outDS;
    }

    public void setOutDS(String outDS) {
        this.outDS = outDS;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }

    public String getParseToNumeric() {
        return parseToNumeric;
    }

    public void setParseToNumeric(String parseToNumeric) {
        this.parseToNumeric = parseToNumeric;
    }
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        
        
    
    
        ToField tf = new ToField();
        tf.setInDS(inDS);
        tf.setName(name);
        tf.setOutDS(outDS);
        tf.setIdColumn(idColumn);
        tf.setParseToNumeric(parseToNumeric);
        


        logBasic("{Iterate Job} Execute = " + tf.ecl());
        
        logBasic("{Iterate Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, tf.ecl());
        
        
        List list = result.getRows();
        list.add(data);
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
            logBasic("{Iterate Job} ECL Code =" + eclCode);
        }
        
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
        
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"inDS")) != null)
            this.setInDS(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"inDS")));

          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"outDS")) != null)
            this.setOutDS(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"outDS")));
         
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"idColumn")) != null)
            this.setIdColumn(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"idColumn")));
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"parseToNumeric")) != null)
            this.setParseToNumeric(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"parseToNumeric")));
         
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      
        retval += "             <inDS><![CDATA["+this.inDS+"]]></inDS>"+Const.CR;
        retval += "             <outDS eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.outDS+"]]></outDS>"+Const.CR;  
        retval += "             <idColumn><![CDATA["+this.idColumn+"]]></idColumn>"+Const.CR;  
        retval += "             <parseToNumeric><![CDATA["+this.parseToNumeric+"]]></parseToNumeric>"+Const.CR;  
        

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {

            if(rep.getStepAttributeString(id_jobentry, "inDS") != null)
                inDS = rep.getStepAttributeString(id_jobentry, "inDS");

            if(rep.getStepAttributeString(id_jobentry, "outDS") != null)
                outDS = rep.getStepAttributeString(id_jobentry, "outDS");
            
            if(rep.getStepAttributeString(id_jobentry, "idColumn") != null)
                idColumn = rep.getStepAttributeString(id_jobentry, "idColumn");
            if(rep.getStepAttributeString(id_jobentry, "parseToNumeric") != null)
                parseToNumeric = rep.getStepAttributeString(id_jobentry, "parseToNumeric");
            
           
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

            rep.saveStepAttribute(id_job, getObjectId(), "inDS", inDS);

            rep.saveStepAttribute(id_job, getObjectId(), "outDS", outDS);
            
             rep.saveStepAttribute(id_job, getObjectId(), "idColumn", idColumn);
              rep.saveStepAttribute(id_job, getObjectId(), "parseToNumeric", parseToNumeric);

           
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
