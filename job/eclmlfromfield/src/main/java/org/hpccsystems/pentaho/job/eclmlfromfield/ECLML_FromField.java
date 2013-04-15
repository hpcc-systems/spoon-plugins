/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclmlfromfield;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.FromField;
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
public class ECLML_FromField extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;
    private String name = "";
    private String inDS = "";
    private String outDS = "";
    private String fromType = "";
    private String map = "";

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    public String getFromType() {
        return fromType;
    }

    public void setFromType(String fromType) {
        this.fromType = fromType;
    }
    
    

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
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        
        
        System.out.println("Start From Field Execute");
    
        FromField ff = new FromField();
        ff.setInDS(inDS);
        ff.setName(name);
        ff.setOutDS(outDS);
        ff.setFromType(fromType);
        ff.setMap(map);
        
        


        logBasic("{Iterate Job} Execute = " + ff.ecl());
        
        logBasic("{Iterate Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, ff.ecl());
        
        
        List list = result.getRows();
        list.add(data);
        String eclCode = "";
        
          
        if (list == null) {
            list = new ArrayList();
        } else {
            
            for (int i = 0; i < list.size(); i++) {
                System.out.println(i);
                RowMetaAndData rowData = (RowMetaAndData) list.get(i);
                String code = rowData.getString("ecl", null);
                if (code != null) {
                    eclCode += code;
                }
            }
            logBasic("{Iterate Job} ECL Code =" + eclCode);
        }
        
        result.setRows(list);
        System.out.println("End From Field Execute");
        
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
          
          
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fromType")) != null)
            this.setFromType(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"fromType")));
          
          if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"map")) != null)
            this.setMap(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"map")));
         
        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        
        retval += "             <inDS><![CDATA["+this.inDS+"]]></inDS>"+Const.CR;
        retval += "             <outDS eclIsDef=\"true\" eclType=\"dataset\"><![CDATA["+this.outDS+"]]></outDS>"+Const.CR;  
        retval += "             <fromType><![CDATA["+this.fromType+"]]></fromType>"+Const.CR;  
        retval += "             <map><![CDATA["+this.map+"]]></map>"+Const.CR;

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {

            if(rep.getStepAttributeString(id_jobentry, "inDS") != null)
                inDS = rep.getStepAttributeString(id_jobentry, "inDS");

            if(rep.getStepAttributeString(id_jobentry, "outDS") != null)
                outDS = rep.getStepAttributeString(id_jobentry, "outDS");
            
            if(rep.getStepAttributeString(id_jobentry, "fromType") != null)
                fromType = rep.getStepAttributeString(id_jobentry, "fromType");
            
            if(rep.getStepAttributeString(id_jobentry, "map") != null)
                map = rep.getStepAttributeString(id_jobentry, "map");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {

            rep.saveStepAttribute(id_job, getObjectId(), "inDS", inDS);

            rep.saveStepAttribute(id_job, getObjectId(), "outDS", outDS);
            rep.saveStepAttribute(id_job, getObjectId(), "fromType", fromType);
            rep.saveStepAttribute(id_job, getObjectId(), "map", map);
           
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
