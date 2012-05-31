/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.eclguifeatures;

import java.util.ArrayList;
import java.util.List;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.util.regex.*;

/**
 *
 * @author ChambeJX
 */
public class AutoPopulate {
    
    private String[] dataSets;
    private String[] recordSets;
    
    
    
    
    
    public String[] parseDefinitions(List<JobEntryCopy> jobs, String attributeName, String attributeValue) throws Exception{
        //System.out.println(" ------------ parseDataSet ------------- ");
        String datasets[] = null;
        ArrayList<String> adDS = new ArrayList<String>();
      
        
        Object[] jec = jobs.toArray();

        int k = 0;

        for(int j = 0; j<jec.length; j++){
            //System.out.println("Node(i): " + j + " | " +((JobEntryCopy)jec[j]).getName());

            if(!((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("START") && !((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("OUTPUT") && !((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("SUCCESS")){
                //System.out.println("Node(k): " + k);
                
                //adDS.add((String)((JobEntryCopy)jec[j]).getName());
                String xml = ((JobEntryCopy)jec[j]).getXML();
                //System.out.println(xml);
                
               NodeList nl = (XMLHandler.loadXMLString(xml)).getChildNodes(); 
               for (int temp = 0; temp < nl.getLength(); temp++){
                   Node nNode = nl.item(temp);
                   
                   
                   NodeList children;
                   Node childnode;
                   String defValue = null;
                   String type = null;
                  
                   children=nNode.getChildNodes();
                   
                   for (int i=0;i<children.getLength();i++)
                   {
                	   try{
	                	  
	                	   childnode=children.item(i);
	                	  // System.out.println("NODE_NAME: " + childnode.getNodeName());
	                	   //if (childnode.getNodeName().equalsIgnoreCase("type"))  // <hop>
	                       //{
	                		   
	                		   //type = XMLHandler.getNodeValue(childnode);
	                		 //  System.out.println("TYPE: " + type);
	                       //}
	                	   if(childnode != null){
	                		   if(childnode.getAttributes() != null){
				                   Node attribute = childnode.getAttributes().getNamedItem(attributeName);
				                   if (attribute!=null && attributeValue.equals(attribute.getTextContent())){
				                	   
				                	   defValue = XMLHandler.getNodeValue(childnode);
				                	  
				                	   if(defValue != null){
				                		   //System.out.println("NODE_VALUE: " + defValue);
				                		   adDS.add((String)defValue);
				                		   k++;
				                		   
				                	   }else{
				                		   //System.out.println("NODE_VALUE: IS NULL");
				                	   }
				                   }
	                		   }
	                	   }
                	   }catch (Exception exc){
                		   System.out.println("Failed to Read XML");
                		   //System.out.println(exc);
                		   //exc.printStackTrace();
                	   }

                   }
               }
            }
        }
        //saving the loop code using arraylists
        datasets = adDS.toArray(new String[k]);
        return datasets;
    }
    
    public String[] parseAllDefinitions(List<JobEntryCopy> jobs) throws Exception{
        return parseDefinitions(jobs,"eclIsDef","true");
    }
    
    public String[] parseDatasetsRecordsets(List<JobEntryCopy> jobs) throws Exception{
    	String datasets1[] = parseDefinitions(jobs, "eclType", "dataset");
    	String datasets2[] = parseDefinitions(jobs, "eclType", "recordset");
    	
    	return merge(datasets1,datasets2);
    	
    	
    }
    public String[] parseDatasets(List<JobEntryCopy> jobs) throws Exception{
    	return parseDefinitions(jobs, "eclType", "dataset");
    }
    
    public String[] parseRecordsets(List<JobEntryCopy> jobs) throws Exception{
    	return parseDefinitions(jobs, "eclType", "recordset");
    	
    }
    public String[] parseRecords(List<JobEntryCopy> jobs) throws Exception{
    	return parseDefinitions(jobs, "eclType", "record");
    	
    }
    
    
    public String[] merge(String[] x, String[] y) {

        String[] merged = new String[x.length + y.length];
        int m = 0;
        for(int i = 0; i<x.length;i++) {
            merged[m] = (String)x[i];
            m++;
        }
        for(int i = 0; i<y.length;i++) {
            merged[m] = (String)y[i];
            m++;
        }
        return merged;
    }
        		
        		
   
    
    /*
     * Gets the Dataset fields from all existing nodes
     * this function uses fieldsByDatasetList to build a List<String>
     * then it converts that to a String[] for use in the dialog classes
     * 
     */
    public String[] fieldsByDataset(String datasetName,List<JobEntryCopy> jobs)throws Exception{
        System.out.println("***fieldsByDataset***");
        String datasets[] = new String[1];
        ArrayList<String> adDS = new ArrayList<String>();
        this.fieldsByDatasetList(adDS, datasetName,jobs);
        datasets = adDS.toArray(new String[adDS.size()]);
        return datasets;
    }
    
     /*
     * Gets the Dataset fields from all existing nodes
     * This uses recursion to travel up from joins etc to the 
     * def of the datasets
     */
    public void fieldsByDatasetList(ArrayList<String> adDS, String datasetName,List<JobEntryCopy> jobs)throws Exception{
        System.out.println(" ------------ fieldsByDatasetList ------------ ");
        Object[] jec = jobs.toArray();

        int k = 0;

        if(jec != null){


            for(int j = 0; j<jec.length; j++){
                //System.out.println("Node(i): " + j + " | " +((JobEntryCopy)jec[j]).getName());

                if(!((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("START") && !((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("OUTPUT") && !((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("SUCCESS")){
                    //System.out.println("Node(k): " + k);

                    //adDS.add((String)((JobEntryCopy)jec[j]).getName());
                    String xml = ((JobEntryCopy)jec[j]).getXML();
                    //System.out.println(xml);

                   NodeList nl = (XMLHandler.loadXMLString(xml)).getChildNodes(); 
                   for (int temp = 0; temp < nl.getLength(); temp++){
                       Node nNode = nl.item(temp);
                       String name = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "name")
                           );

                       String dataset = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "dataset_name")
                           );

                       String type = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "type")
                           );

                       String record = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "record_name")
                           );
                       String recordset = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "recordset_name")
                           );
                      /* System.out.println("XMLParse Compare Val: "+ datasetName + "~~~~~~~~~");
                       System.out.println("XMLParse Type: " + type);
                       System.out.println("XMLParse Recordset: " + recordset);
                       System.out.println("XMLParse record: " + record);
                       System.out.println("XMLParse Dataset: " + dataset);
                       System.out.println("XMLParse name: " + name);*/
                       if(type.equals("ECLDataset") && 
                               ((dataset!=null && dataset.equals(datasetName)) || 
                               (recordset != null && recordset.equals(datasetName)) || (record != null && record.equals(datasetName)))){
                            //System.out.println("---------------------ECLDataset----");
                            String def = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "record_def")
                           );
                            if(def != null && !def.equals("")){
                                this.chunkFormat(def,adDS);

                            }



                       }else if(type.equals("ECLJoin") && 
                               (
                                (dataset != null && dataset.equals(datasetName)) || 
                                (recordset != null && recordset.equals(datasetName)) || 
                                (record != null && record.equals(datasetName))
                               )
                               ){
                           //if it is a sort
                           //well we need to look at the parents so we call this funtion recursivelly
                           //leftRecordSet
                            //System.out.println("------------------------ECLJoin----");

                           String leftRS = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "left_recordset") );
                           String rightRS = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "right_recordset") );


                           this.fieldsByDatasetList(adDS, leftRS, jobs);
                           this.fieldsByDatasetList(adDS, rightRS, jobs);



                       }else if(type.equals("ECLProject") && 
                               ((dataset!=null && dataset.equals(datasetName)) || 
                               (recordset != null && recordset.equals(datasetName)) || (record != null && record.equals(datasetName)))){
                           // System.out.println("---------------------ECLProject----");

                           String def = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "outRecordFormat")
                           );
                            if(def != null && !def.equals("")){
                                this.chunkFormat(def,adDS);

                            }
                           //outRecordFormat
                       }
                       //will need to add logic for each type




                       //dataset_name
                       //name
                       //type
                       //record_name
                   }

                }

                //System.out.println(((JobEntryCopy)jec[j]).getXML());

            }
        }
        System.out.println("finished parsing");
    }
    
    private void chunkFormat(String def, List<String> adDS){
         if(def != null && !def.equals("")){
                           // System.out.println("has def");
                            String[] pieces = def.split("\n");
                            
                            for (int p = pieces.length - 1; p >= 0; p--) {
                             //  System.out.println("" + (String)pieces[p].trim());
                               
                               Pattern pattern = Pattern.compile("[ ;]+",Pattern.CASE_INSENSITIVE);
                               
                               String peice = (String)pieces[p].trim();
                               
                               String[] chunks = pattern.split(peice);
                              // System.out.println("Chunks lenght " + chunks.length);
                               if(chunks.length >= 2 && chunks[1]!=null){
                                    adDS.add(chunks[1]);
                               }
                            }

                        }
    }
    
    private String[] mergeArray(String[] a1, String[] a2){
        System.out.println(" ------------ mergeArray ------------- ");
        
        String[] out = new String[a1.length+a2.length];
      
        System.arraycopy(a1, 0, out, 0, a1.length);
        System.arraycopy(a1, 0, out, a1.length, a2.length);
        
        return out;
    }

    
    public String getGlobalVariable(List<JobEntryCopy> jobs, String ofType) throws Exception{
        System.out.println(" ------------ getGlobalVariable ------------- ");
        String datasets[] = null;
        ArrayList<String> adDS = new ArrayList<String>();
        String out = "";
        
        Object[] jec = jobs.toArray();

        int k = 0;
        
        if(jec != null){
            

            for(int j = 0; j<jec.length; j++){
                //System.out.println("Node(i): " + j + " | " +((JobEntryCopy)jec[j]).getName());

                //if( ((JobEntryCopy)jec[j]).getName().equalsIgnoreCase("ECLGlobalVariables") ){
                    //System.out.println("Node(k): " + k);

                    //adDS.add((String)((JobEntryCopy)jec[j]).getName());
                    String xml = ((JobEntryCopy)jec[j]).getXML();
                    //System.out.println(xml);

                   NodeList nl = (XMLHandler.loadXMLString(xml)).getChildNodes(); 
                   for (int temp = 0; temp < nl.getLength(); temp++){
                       Node nNode = nl.item(temp);
                       
                       String type = XMLHandler.getNodeValue(
                           XMLHandler.getSubNode(nNode, "type")
                           );
                       //System.out.println("Type: " + type);
                       if(type.equalsIgnoreCase("ECLGlobalVariables")){
                           if(ofType.equalsIgnoreCase("server_ip")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "server_ip")
                                );
                           }
                           if(ofType.equalsIgnoreCase("server_port")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "server_port")
                                );
                           }
                          if(ofType.equalsIgnoreCase("landing_zone")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "landing_zone")
                                );
                          }
                          
                          if(ofType.equalsIgnoreCase("cluster")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "cluster")
                                );
                          }
                          
                          if(ofType.equalsIgnoreCase("jobName")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "jobName")
                                );
                          }
                          
                          if(ofType.equalsIgnoreCase("eclccInstallDir")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "eclccInstallDir")
                                );
                          }
                          
                          if(ofType.equalsIgnoreCase("mlPath")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "mlPath")
                                );
                          }
                          
                          if(ofType.equalsIgnoreCase("includeML")){
                                out = XMLHandler.getNodeValue(
                                    XMLHandler.getSubNode(nNode, "includeML")
                                );
                          }
                          
                          
                                  

                       }
                      
                       
                   }

               // }


            }
            //saving the loop code using arraylists
            datasets = adDS.toArray(new String[k]);

        }
        return out;

    }
}
