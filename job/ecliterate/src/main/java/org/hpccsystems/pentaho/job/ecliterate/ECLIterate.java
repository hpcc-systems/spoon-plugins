/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecliterate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hpccsystems.javaecl.Iterate;
import org.hpccsystems.mapper.MapperBO;
import org.hpccsystems.mapper.MapperRecordList;
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
 * @author ChalaAX
 */
public class ECLIterate extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    

    private String transformName = "";
    private String dataset = "";//Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    private Boolean runLocal = false;

    private String recordsetName = "";
    

    private MapperRecordList mapperRecList = new MapperRecordList();

   

    public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	public MapperRecordList getMapperRecList() {
		return mapperRecList;
	}

	public void setMapperRecList(MapperRecordList mapperRecList) {
		this.mapperRecList = mapperRecList;
	}

	public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }


    public String getTransformName() {
        return transformName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
    }


    public Boolean getRunLocal() {
        return runLocal;
    }

    public void setRunLocal(Boolean runLocal) {
        this.runLocal = runLocal;
    }
    
    public void setRunLocalString(String runLocal) {
        
        if(runLocal.equals("true")){
            this.runLocal = true;
        }else{
            this.runLocal = false;
        }
        
    }
    
    public String getRunLocalString() {
        if(runLocal){
            return "true";
        }else{
            return "false";
        }
    }

    
   
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
    	Result result = modifyResults(prevResult);
    	if(result.isStopped()){
        	return result;
 		}
        Iterate iterate = new Iterate();
        iterate.setRunLocal(this.getRunLocal());
        iterate.setTransformName(this.getTransformName());
        iterate.setRecordsetName(this.getRecordsetName());
        iterate.setDataset(this.getDataset());
        iterate.setTransform(generateEclForMapperGrid());
        
        logBasic("{Iterate Job} Execute = " + iterate.ecl());
        
        logBasic("{Iterate Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, iterate.ecl());
        
        
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
            logBasic("{Iterate Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        
        
        return result;
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
		String[] strLine = in.split("[|]");
		int len = strLine.length;
		if (len > 0) {
			mapperRecList = new MapperRecordList();
			for (int i = 0; i < len; i++) {
				MapperBO rb = new MapperBO(strLine[i]);
				mapperRecList.addRecord(rb);
			}
		}
	}

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform_name")) != null)
                this.setTransformName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"transform_name")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")) != null)
                this.setRecordsetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"recordset_name")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"dataset")) != null)
                this.setDataset(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"dataset")));
            
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "mapperRecList")) != null)
            	openRecordListForMapper(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "mapperRecList")));
            
            this.setRunLocalString(XMLHandler.getNodeValue(XMLHandler.getSubNode(node,"runLocal")));
        } catch (Exception e) {
            throw new KettleXMLException("ECL Distribute Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        retval += "             <recordset_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.recordsetName+"]]></recordset_name>"+Const.CR;
        retval += "             <transform_name eclIsDef=\"true\" eclType=\"recordset\"><![CDATA["+this.transformName+"]]></transform_name>"+Const.CR;
        retval += "             <runLocal><![CDATA["+this.getRunLocalString()+"]]></runLocal>"+Const.CR;
        retval += "             <dataset><![CDATA["+this.getDataset()+"]]></dataset>"+Const.CR;
        retval += "		<mapperRecList><![CDATA[" + this.saveRecordListForMapper() + "]]></mapperRecList>" + Const.CR;
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
            if(rep.getStepAttributeString(id_jobentry, "recordsetName") != null)
                recordsetName = rep.getStepAttributeString(id_jobentry, "recordsetName");
            if(rep.getStepAttributeString(id_jobentry, "transformName") != null)
                transformName = rep.getStepAttributeString(id_jobentry, "transformName");
            if(rep.getStepAttributeString(id_jobentry, "runLocal") != null)
                this.setRunLocalString(rep.getStepAttributeString(id_jobentry, "runLocal"));
            if(rep.getStepAttributeString(id_jobentry, "dataset") != null)
                this.setDataset(rep.getStepAttributeString(id_jobentry, "dataset"));
            
            if(rep.getStepAttributeString(id_jobentry, "mapperRecList") != null)
                this.openRecordListForMapper(rep.getStepAttributeString(id_jobentry, "mapperRecList")); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
            rep.saveStepAttribute(id_job, getObjectId(), "recordsetName", recordsetName);
            rep.saveStepAttribute(id_job, getObjectId(), "transformName", transformName);
            rep.saveStepAttribute(id_job, getObjectId(), "runLocal", this.getRunLocalString());
            rep.saveStepAttribute(id_job, getObjectId(), "dataset", dataset);
            rep.saveStepAttribute(id_job, getObjectId(), "mapperRecList", this.saveRecordListForMapper()); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    
}
