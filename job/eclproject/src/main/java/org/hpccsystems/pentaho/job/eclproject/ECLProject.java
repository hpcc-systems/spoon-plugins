/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclproject;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.ecldirect.Project;
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

/**
 *
 * @author ChalaAX
 */
public class ECLProject extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    //private String jobName;

    private String recordsetName = "";
    private boolean declareCounter = false;
    private String inRecordName = "";
    private String outRecordName = "";
    private String outRecordFormat = "";
    private String transformName = "";
    private String transformFormat = "";
    private String parameterName = "";

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }
    
   // public String getJobName(){
   //     return jobName;
   // }
   // public void setJobName(String jobName){
   //     this.jobName = jobName;
   // }
    

    
    public boolean getDeclareCounter() {
        return declareCounter;
    }

    public void setDeclareCounter(boolean declareCounter) {
        this.declareCounter = declareCounter;
    }
    
    
    public String getDeclareCounterString() {
        //declareCounter;
       // logBasic("{DeclareCounter as string");
        //System.out.println("getDeclareCounter as string");

            if(declareCounter){
               // System.out.println("yes");
                return "yes";
            }else{
              //  System.out.println("No");
                return "no";
            }

    }

    public void setDeclareCounterString(String declareCounter) {
        //this.declareCounter = declareCounter;
        //System.out.println("{setDeclareCounter" + declareCounter);
        if(declareCounter.compareToIgnoreCase("yes") == 0){
            this.declareCounter = true;
        }else{
            this.declareCounter = false;
        }
    }
    
    public String getInRecordName() {
        return inRecordName;
    }

    public void setInRecordName(String inRecordName) {
        this.inRecordName = inRecordName;
    }
    
    public String getOutRecordName(){
        return outRecordName;
    }
    public void setOutRecordName(String outRecordName){
        this.outRecordName = outRecordName;
    }
    
    public String getOutRecordFormat(){
        return outRecordFormat;
    }
    public void setOutRecordFormat(String outRecordFormat){
        this.outRecordFormat=outRecordFormat;
    }
    
    public String getTransformName(){
        return this.transformName;
    }
    
    public void setTransformName(String transformName){
        this.transformName = transformName;
    }
    
    public String getTransformFormat(){
        return this.transformFormat;
    }
    public void setTransformFormat(String transformFormat){
        this.transformFormat = transformFormat;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
    }

   
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
      /*  private boolean declareCounter;
    private String inRecordName;
    private String outRecordName;
    private String outRecordFormat;
    private String transformName;
    private String transformFormat;
    */
    
        Project project = new Project();
        project.setName(this.getRecordsetName());
        project.setDeclareCounter(this.getDeclareCounter());
        project.setInRecordName(this.getInRecordName());
        project.setOutRecordName(this.getOutRecordName());
        project.setOutRecordFormat(this.getOutRecordFormat());
        project.setTransformName(this.getTransformName());
        project.setTransformFormat(this.getTransformFormat());
        project.setParameterName(this.getParameterName());
        
        
        

        logBasic("{Project Job} Execute = " + project.ecl());
        
        logBasic("{Project Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, project.ecl());
        
        
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
            logBasic("{Project Job} ECL Code =" + eclCode);
        }
        
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);

            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")) != null)
            //    setName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "declareCounter")) != null)
                setDeclareCounterString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "declareCounter")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")) != null)
                setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordset_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "inRecordName")) != null)
                setInRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "inRecordName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outRecordName")) != null)
                setOutRecordName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outRecordName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outRecordFormat")) != null)
                setOutRecordFormat(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "outRecordFormat")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "transformName")) != null)
                setTransformName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "transformName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "transformFormat")) != null)
                setTransformFormat(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "transformFormat")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"parameterName")) != null)
                setParameterName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"parameterName")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Project Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";

        retval += super.getXML();
        //System.out.println(" ------------ getXML ------------- ");
        
        //retval += "		<name>" + name + "</name>" + Const.CR;
        
       
        retval += "		<declareCounter><![CDATA[" + this.getDeclareCounterString() + "]]></declareCounter>" + Const.CR;
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<inRecordName><![CDATA[" + inRecordName + "]]></inRecordName>" + Const.CR;
        retval += "		<outRecordName eclIsDef=\"true\" eclType=\"record\"><![CDATA[" + outRecordName + "]]></outRecordName>" + Const.CR;
        retval += "		<outRecordFormat><![CDATA[" + outRecordFormat + "]]></outRecordFormat>" + Const.CR;
        retval += "		<transformName eclIsDef=\"true\" eclType=\"recordset\"><![CDATA[" + transformName + "]]></transformName>" + Const.CR;
        retval += "		<transformFormat><![CDATA[" + transformFormat + "]]></transformFormat>" + Const.CR;
        retval += "		<parameterName><![CDATA[" + parameterName + "]]></parameterName>" + Const.CR;
        
       
       //System.out.println(" end getXML ");
       
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

           // if(rep.getStepAttributeString(id_jobentry, "name") != null)
           //     name = rep.getStepAttributeString(id_jobentry, "name"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "declareCounter") != null)
                setDeclareCounterString(rep.getStepAttributeString(id_jobentry, "declareCounter")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "recordsetName") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordsetName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "inRecordName") != null)
                inRecordName = rep.getStepAttributeString(id_jobentry, "inRecordName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "outRecordName") != null)
                outRecordName = rep.getStepAttributeString(id_jobentry, "outRecordName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "outRecordFormat") != null)
                outRecordFormat = rep.getStepAttributeString(id_jobentry, "outRecordFormat"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "transformName") != null)
                transformName = rep.getStepAttributeString(id_jobentry, "transformName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "transformFormat") != null)
                transformFormat = rep.getStepAttributeString(id_jobentry, "transformFormat"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "parameterName") != null)
                parameterName = rep.getStepAttributeString(id_jobentry, "parameterName"); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$

            
           // rep.saveStepAttribute(id_job, getObjectId(), "name", name); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "declareCounter", this.getDeclareCounterString());
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetName", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "inRecordName", inRecordName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outRecordName", outRecordName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outRecordFormat", outRecordFormat); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "transformName", transformName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "transformFormat", transformFormat); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "parameterName", parameterName); //$NON-NLS-1$
            
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
