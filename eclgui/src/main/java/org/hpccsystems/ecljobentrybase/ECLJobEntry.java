/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.ecljobentrybase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
//import org.hpccsystems.javaecl.Output;
import org.hpccsystems.javaecl.EclDirect;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.javaecl.Column;
import java.io.*;
import org.hpccsystems.javaecl.ECLSoap;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.core.*;
import org.pentaho.di.core.gui.SpoonFactory;


import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.javaecl.Column;
import org.hpccsystems.javaecl.ECLSoap;
import org.hpccsystems.javaecl.EclDirect;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.pentaho.di.job.JobMeta;

/**
 *
 * @author Chambers,Joseph
 */
public class ECLJobEntry extends JobEntryBase implements Cloneable, JobEntryInterface  {
    
	
	
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
return null;
}



@Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
            super.loadXML(node, list, list1);
    }


    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {

    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {

    }
    public String resultListToString(RecordList recordList){
    	return ECLJobEntry.resultListToStringStatic(recordList);
    }
    public static String resultListToStringStatic(RecordList recordList){
        String out = "";
        
        if(recordList != null){
            if(recordList.getRecords() != null && recordList.getRecords().size() > 0) {
                    System.out.println("Size: "+recordList.getRecords().size());
                    for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO record = (RecordBO) iterator.next();
                        	String rLen = record.getColumnWidth();
        					if (rLen != null && rLen.trim().length() >0) {
                                if(record.getColumnName() != null && !record.getColumnName().equals("")){
                                    out += record.getColumnType()+rLen + " " + record.getColumnName();
                                    if(record.getDefaultValue().equalsIgnoreCase("null")){
                                    	//added so we can set null values
                                        out += ":= ''";
                                    }else if(record.getDefaultValue() != ""){
                                    	//added check to make non numeric be quoted ''
                                    	String regex = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
                                    	if(record.getDefaultValue().matches(regex)){
                                    		out += ":= " + record.getDefaultValue();
                                    	}else{
                                    		out += ":= '" + record.getDefaultValue() + "'";
                                        }
                                    }
                                    out += ";\r\n";
                                 }
                            }else{
                                if(record.getColumnName() != null && !record.getColumnName().equals("")){
                                    out += record.getColumnType() + " " + record.getColumnName();
                                    //if(record.getDefaultValue() != ""){
                                    //    out += ":= " + record.getDefaultValue();
                                    //}
                                    
                                    if(record.getDefaultValue().equalsIgnoreCase("null")){
                                    	//added so we can set null values
                                        out += ":= ''";
                                    }else if(record.getDefaultValue() != ""){
                                    	//added check to make non numeric be quoted ''
                                    	String regex = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
                                    	if(record.getDefaultValue().matches(regex)){
                                    		out += ":= " + record.getDefaultValue();
                                    	}else{
                                    		out += ":= '" + record.getDefaultValue() + "'";
                                        }
                                    }
                                    
                                    out += ";\r\n";
                                }
                            }
                            
                            
                    }
            }
        }
        
        return out;
    }
    
    
    public String parseEclFromRowData(List<RowMetaAndData> list){
    	 String eclCode = "";
         if (list == null) {
             list = new ArrayList<RowMetaAndData>();
         } else {

         	for (int i = 0; i < list.size(); i++) {
             	try{
             		boolean hasECL = false;
 	                RowMetaAndData rowData = (RowMetaAndData) list.get(i);
 	                RowMeta rowMeta = (RowMeta) rowData.getRowMeta();
 	                String[] fields = rowMeta.getFieldNames();
 	                for(int cnt = 0; cnt<fields.length; cnt++){
 	                	if(fields[cnt].equals("ecl")){
 	                		hasECL = true;
 	                	}
 	                }
 	                if(hasECL){
 		                String code = rowData.getString("ecl", null);
 		                if (code != null) {
 		                    eclCode += code;
 		                }
 	                }
             	}catch (Exception e){
             		//ecl doesn't exist skip it
             		//I can't find a way to check rowData if it exists
             		e.printStackTrace();
             	}
             }
             logBasic("{Execute Job} Execute Code =" + eclCode);
         }
         
         return eclCode;
    }
    
   
}
