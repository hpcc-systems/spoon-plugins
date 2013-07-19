/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclproject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Project;
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
import org.hpccsystems.javaecl.Dataset;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.mapper.*;
import org.hpccsystems.ecljobentrybase.*;


/**
 *
 * @author ShetyeD
 */
public class ECLProject extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
    private String recordsetName = "";
    private boolean declareCounter = false;
    private String inRecordName = "";
    private String outRecordName = "";
    private String outRecordFormat = "";
    private String transformName = "";
    private String transformFormat = "";
    private String parameterName = "input";
    private RecordList recordList = new RecordList();
    private MapperRecordList mapperRecList = new MapperRecordList();

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }

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

    //public void setParameterName(String parameterName) {
    //    this.parameterName = parameterName;
    //}

    public RecordList getRecordList() {
        return recordList;
    }

    public void setRecordList(RecordList recordList) {
        this.recordList = recordList;
    }
    
    public MapperRecordList getMapperRecList() {
		return mapperRecList;
	}

	public void setMapperRecList(MapperRecordList mapperRecList) {
		this.mapperRecList = mapperRecList;
	}
    
	/*
	 //moved to base class
	
	public String resultListToString() {
		String out = "";
		if (recordList != null) {
			if (recordList.getRecords() != null && recordList.getRecords().size() > 0) {
				//System.out.println("Size: " + recordList.getRecords().size());
				for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
					RecordBO record = (RecordBO) iterator.next();
					String rLen = record.getColumnWidth();
					if (rLen != null && rLen.trim().length() >0) {
						if (record.getColumnName() != null && !record.getColumnName().equals("")) {
							out += record.getColumnType() + rLen + " " + record.getColumnName();
							if (record.getDefaultValue() != "") {
								out += " := " + record.getDefaultValue();
							}
							out += ";\r\n";
						}
					} else {
						if (record.getColumnName() != null && !record.getColumnName().equals("")) {
							out += record.getColumnType() + " " + record.getColumnName();
							if (record.getDefaultValue() != "") {
								out += " := " + record.getDefaultValue();
							}
							out += ";\r\n";
						}
					}

				}
			}
		}
		
		//System.out.println("RESULT of resultListToString() ........... "+out);
		
		return out;
	}
	 */
	
	public String saveRecordList() {
		String out = "";
		ArrayList list = recordList.getRecords();
		Iterator<RecordBO> itr = list.iterator();
		boolean isFirst = true;
		while (itr.hasNext()) {
			if (!isFirst) {
				out += "|";
			}

			out += itr.next().toCSV();
			isFirst = false;
		}
		
		//System.out.println("RESULT of saveRecordList() ........... "+out);
		
		return out;
	}

	public void openRecordList(String in) {
		//System.out.println("Inside Method openRecordList .........."+in);
		String[] strLine = in.split("[|]");

		int len = strLine.length;
		if (len > 0) {
			recordList = new RecordList();
			//System.out.println("Open Record List");
			for (int i = 0; i < len; i++) {
				//System.out.println("++++++++++++" + strLine[i]);
				// this.recordDef.addRecord(new RecordBO(strLine[i]));
				RecordBO rb = new RecordBO(strLine[i]);
				//System.out.println(rb.getColumnName());
				recordList.addRecordBO(rb);
			}
		}
	}
	
	/**
	 * This method is used to generate ECl for the Mapper Grid values
	 * @param arlRecords
	 * @return String containing ECL Code 
	 */
	public String generateEclForMapperGrid() {
		String out = "";
		if (mapperRecList != null) {
			if (mapperRecList.getRecords() != null && mapperRecList.getRecords().size() > 0) {
				//System.out.println("Size: " + mapperRecList.getRecords().size());
				for (Iterator<MapperBO> iterator = mapperRecList.getRecords().iterator(); iterator.hasNext();) {
					MapperBO record = (MapperBO) iterator.next();
					out += record.getOpVariable() + " := " + record.getExpression();
					out += ";\r\n";
				}
			}
		}
		//System.out.println("RESULT of generateEclForMapperGrid() ........... "+out);
		
		return out;
	}
	
    
	public String saveRecordListForMapper() {
		String out = "";
		ArrayList list = mapperRecList.getRecords();
		Iterator<MapperBO> itr = list.iterator();
		boolean isFirst = true;
		while (itr.hasNext()) {
			//System.out.println("loop");
			if (!isFirst) {
				out += "|";
			}

			out += itr.next().toCSV();
			isFirst = false;
		}
		
		//System.out.println("RESULT of saveRecordListForMapper() ........... "+out);
		
		return out;
	}

	public void openRecordListForMapper(String in) {
		//System.out.println("Inside Method openRecordListForMapper .........."+in);
		String[] strLine = in.split("[|]");

		int len = strLine.length;
		if (len > 0) {
			mapperRecList = new MapperRecordList();
			//System.out.println("Open Record List");
			for (int i = 0; i < len; i++) {
				//System.out.println("++++++++++++" + strLine[i]);
				// this.recordDef.addRecord(new RecordBO(strLine[i]));
				MapperBO rb = new MapperBO(strLine[i]);
				mapperRecList.addRecord(rb);
			}
		}
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
        project.setOutRecordFormat(resultListToString(recordList));
        project.setTransformName(this.getTransformName());
        //project.setTransformFormat(this.getTransformFormat());
        project.setTransformFormat(generateEclForMapperGrid());
        project.setParameterName(this.getParameterName());
        //project.setRecordFormatString(resultListToString(recordList));
        
        

        logBasic("{Project Job} Execute = " + project.ecl());
        //System.out.println("PROJECT JOB ---->>>> : "+project.ecl());
        
        logBasic("{Project Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, project.ecl());
        
        
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
            logBasic("{Project Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        
        
        return result;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);

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
            //if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"parameterName")) != null)
             //   setParameterName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"parameterName")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")) != null)
                openRecordList(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "recordList")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "mapperRecList")) != null)
            	openRecordListForMapper(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "mapperRecList")));
            
        } catch (Exception e) {
            throw new KettleXMLException("ECL Project Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";

        retval += super.getXML();

        retval += "		<declareCounter><![CDATA[" + this.getDeclareCounterString() + "]]></declareCounter>" + Const.CR;
        retval += "		<recordset_name eclIsDef=\"true\" eclType=\"dataset\"><![CDATA[" + recordsetName + "]]></recordset_name>" + Const.CR;
        retval += "		<inRecordName><![CDATA[" + inRecordName + "]]></inRecordName>" + Const.CR;
        retval += "		<outRecordName clIsDef=\"true\" eclType=\"record\"><![CDATA[" + outRecordName + "]]></outRecordName>" + Const.CR;
        retval += "		<outRecordFormat><![CDATA[" + outRecordFormat + "]]></outRecordFormat>" + Const.CR;
        retval += "		<transformName><![CDATA[" + transformName + "]]></transformName>" + Const.CR;
        retval += "		<transformFormat><![CDATA[" + transformFormat + "]]></transformFormat>" + Const.CR;
        //retval += "		<parameterName><![CDATA[" + parameterName + "]]></parameterName>" + Const.CR;
        retval += "		<recordList><![CDATA[" + this.saveRecordList() + "]]></recordList>" + Const.CR;
        retval += "		<mapperRecList><![CDATA[" + this.saveRecordListForMapper() + "]]></mapperRecList>" + Const.CR;
       //add cdata above see below for example
       // retval += "		<declareCounter><![CDATA[" + this.getDeclareCounterString() + "]]></declareCounter>" + Const.CR;
       


        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            //jobName = rep.getStepAttributeString(id_jobentry, "jobName"); //$NON-NLS-1$

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
            //if(rep.getStepAttributeString(id_jobentry, "parameterName") != null)
               // parameterName = rep.getStepAttributeString(id_jobentry, "parameterName"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "recordList") != null)
                this.openRecordList(rep.getStepAttributeString(id_jobentry, "recordList")); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "mapperRecList") != null)
                this.openRecordListForMapper(rep.getStepAttributeString(id_jobentry, "mapperRecList")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            //rep.saveStepAttribute(id_job, getObjectId(), "jobName", jobName); //$NON-NLS-1$

            rep.saveStepAttribute(id_job, getObjectId(), "declareCounter", this.getDeclareCounterString());
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetName", recordsetName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "inRecordName", inRecordName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outRecordName", outRecordName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "outRecordFormat", outRecordFormat); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "transformName", transformName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "transformFormat", transformFormat); //$NON-NLS-1$
            //rep.saveStepAttribute(id_job, getObjectId(), "parameterName", parameterName); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "recordList", this.saveRecordList()); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "mapperRecList", this.saveRecordListForMapper()); //$NON-NLS-1$
            
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
