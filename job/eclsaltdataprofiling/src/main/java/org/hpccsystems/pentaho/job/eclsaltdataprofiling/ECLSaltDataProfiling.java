/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclsaltdataprofiling;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.widgets.Combo;
import org.hpccsystems.ecljobentrybase.ECLJobEntry;
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
public class ECLSaltDataProfiling extends ECLJobEntry {
    
    
    private String fileName = "";
    
    private String fileType;

    public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        


        
        RowMetaAndData data = new RowMetaAndData();
        
        
        List list = result.getRows();
        list.add(data);
        
        //String eclCode = "";
        //indata:= dataset('~in::testing',R,CSV(SEPARATOR('')));
        
        String ft = "";
        if(this.fileType.equalsIgnoreCase("csv")){
        	ft = "CSV(SEPARATOR(''))";
        }else{
        	ft = this.fileType;
        }
        String ecl = "R:= record " + "\r\n" +
        		"String Txt;" + "\r\n" +
        		"end; " + "\r\n\r\n" +
        		"indata:= dataset('" + this.fileName + "',R," + ft + "); " + "\r\n" +
        		"indata_filtered := PROJECT(indata, TRANSFORM(R, SELF.TXT := STD.Str.FilterOut(LEFT.txt, '\"')));" + "\r\n" +
        		"T := indata_filtered[1..1];" + "\r\n" +
        		"D := indata_filtered[2..];" + "\r\n" +
        		"SALT23.MAC_Character_Counts.Data_Layout Take(R le,UNSIGNED c) := TRANSFORM" + "\r\n" +
        		"SELF.Fld := ut.Word(le.Txt,c,',');" + "\r\n" +
        		"SELF.FldNo := c;" + "\r\n" +
        		"END;" + "\r\n" +
        		"Flds := NORMALIZE(D,ut.NoWords(LEFT.Txt,',')+1,Take(LEFT,COUNTER));" + "\r\n" +
        		"SALT23.MAC_Character_Counts.Field_Identification TakeName(R le,UNSIGNED c) := TRANSFORM" + "\r\n" +
        		"SELF.FieldName := ut.Word(le.Txt,c,',');" + "\r\n" +
        		"SELF.FldNo := c;" + "\r\n" +
        		"END;" + "\r\n" +
        		"Titles := NORMALIZE(T,ut.NoWords(LEFT.Txt,',')+1,TakeName(LEFT,COUNTER));" + "\r\n" +
        		"p := SALT23.MAC_Character_Counts.FN_Profile(Flds,Titles);" + "\r\n" +
        		"output(SALT23.MAC_Character_Counts.EclRecord(p),NAMED('dataProfilingStructure'));" + "\r\n" +
        		"output(p,NAMED('dataProfilingResults'),all);" + "\r\n" +
        		"\r\n\r\n";
        
        logBasic("{Dataset Job} Execute = " + ecl);
        
        logBasic("{Dataset Job} Previous =" + ecl);
        
        result.setResult(true);

        data.addValue("ecl", Value.VALUE_TYPE_STRING, ecl);
        String eclCode = parseEclFromRowData(list);
       /*   
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
        */
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            //this.setName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "name")));
          
           
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"file_name")) != null)
                this.setFileName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"file_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"file_type")) != null)
                this.setFileType(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"file_type")));

        
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        retval += super.getXML();
        retval += "             <file_name><![CDATA["+this.fileName+"]]></file_name>"+Const.CR;
        retval += "             <file_type><![CDATA["+this.fileType+"]]></file_type>"+Const.CR;
        return retval;
    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {    
            
            if(rep.getStepAttributeString(id_jobentry, "file_name") != null)
                fileName = rep.getStepAttributeString(id_jobentry, "file_name");
            
            if(rep.getStepAttributeString(id_jobentry, "file_type") != null)
                fileType = rep.getStepAttributeString(id_jobentry, "file_type");
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$
            
            rep.saveStepAttribute(id_job, getObjectId(), "file_name", fileName);
            rep.saveStepAttribute(id_job, getObjectId(), "file_type", fileType);
           
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
