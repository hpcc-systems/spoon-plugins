package com.hpccsystems.salt.helpers;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

import org.hpccsystems.salt.hygiene.Generate;
import org.hpccsystems.salt.hygiene.bean.FieldHygieneRule;
import org.hpccsystems.salt.hygiene.bean.HygieneSpecDocument;

import au.com.bytecode.opencsv.CSVReader;



public class LoadSpecificties {
	private String moduleName = "";
	private String fileName = "";
	private String eclDatasetCode = "";
	
	private String rsDef = "";
	private String dsDef = "";
	private String recordName = "";
	private String logicalFile = "";
	private String fileType = "";
	
	private String dsName = "";
	
	private String clusterID = "";
	private String recordID = "";
	private boolean idExists = false;
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String datasetName) {
		this.fileName = datasetName;
	}
	
	

	public String getEclDatasetCode() {
		return eclDatasetCode;
	}

	public void setEclDatasetCode(String eclDatasetCode) {
		this.eclDatasetCode = eclDatasetCode;
	}

	
	public String getRsDef() {
		return rsDef;
	}

	public void setRsDef(String rsDef) {
		this.rsDef = rsDef;
	}

	public String getDsDef() {
		return dsDef;
	}

	public void setDsDef(String dsDef) {
		this.dsDef = dsDef;
	}

	public String getRecordName() {
		return recordName;
	}

	public void setRecordName(String recordName) {
		this.recordName = recordName;
	}

	public String getLogicalFile() {
		return logicalFile;
	}

	public void setLogicalFile(String logicalFile) {
		this.logicalFile = logicalFile;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
	
	

	public String getDsName() {
		return dsName;
	}

	public void setDsName(String dsName) {
		this.dsName = dsName;
	}

	
	public String getModuleName() {
		return moduleName;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	
	public boolean isIdExists() {
		return idExists;
	}

	public void setIdExists(boolean idExists) {
		this.idExists = idExists;
	}

	//loads the dataset xml and combines the specificities passed in
	public ArrayList loadDataset(String fileName, HashMap specificities, String outFile){
		ArrayList<DatasetNode> dataSet = new ArrayList<DatasetNode>();
		

		//System.out.println(fileName);
		try{
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = (Document) dBuilder.parse(fileName);
			doc.getDocumentElement().normalize();
			
			//System.out.println("Root element : " + doc.getDocumentElement().getNodeName());
			
			this.fileName = doc.getElementsByTagName("hyg:file-name").item(0).getTextContent();
			
			doc.getElementsByTagName("hyg:module-name").item(0).setTextContent(this.moduleName);
			//doc.getElementsByTagName("hyg:file-name").item(0).setTextContent(this.fileName);
			this.rsDef = doc.getElementsByTagName("hyg:dataset-rsdef").item(0).getTextContent();
			this.dsDef = doc.getElementsByTagName("hyg:dataset-dsdef").item(0).getTextContent();
			this.recordName = doc.getElementsByTagName("hyg:dataset-record-name").item(0).getTextContent();
			this.logicalFile = doc.getElementsByTagName("hyg:dataset-logical-file").item(0).getTextContent();
			this.fileType = doc.getElementsByTagName("hyg:dataset-file-type").item(0).getTextContent();
			this.dsName = doc.getElementsByTagName("hyg:dataset-name").item(0).getTextContent();
			
			this.eclDatasetCode = rsDef + dsDef;
			
			
			
			
			NodeList nList = doc.getElementsByTagName("hyg:field-rule");
			//System.out.println("LEN: " + nList.getLength());
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				 
				//System.out.println("\nCurrent Element :" + nNode.getNodeName());
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					String fieldName = eElement.getElementsByTagName("hyg:field-name").item(0).getTextContent();
					String specificity = (String)specificities.get(fieldName.toLowerCase() + "_specificity");
					if(specificity != null){
						Element newSpecificity = doc.createElement("hyg:field-specificity");
						newSpecificity.appendChild(doc.createTextNode(specificity));
						eElement.appendChild(newSpecificity);
						
					}
					DatasetNode dn = new DatasetNode(fieldName,eElement.getElementsByTagName("hyg:field-type").item(0).getTextContent(),specificity);
					if(eElement.getElementsByTagName("hyg:caps").item(0) != null)
						dn.setCaps(eElement.getElementsByTagName("hyg:caps").item(0).getTextContent());
					if(eElement.getElementsByTagName("hyg:left-trim").item(0) != null)
						dn.setLeftTrim(eElement.getElementsByTagName("hyg:left-trim").item(0).getTextContent());
					if(eElement.getElementsByTagName("hyg:right-trim").item(0) != null)
						dn.setRightTrim(eElement.getElementsByTagName("hyg:right-trim").item(0).getTextContent());
					if(eElement.getElementsByTagName("hyg:allow").item(0) != null)
						dn.setAllow(eElement.getElementsByTagName("hyg:allow").item(0).getTextContent());
					if(eElement.getElementsByTagName("hyg:on-fail").item(0) != null)
						dn.setOnFail(eElement.getElementsByTagName("hyg:on-fail").item(0).getTextContent());
					
					dataSet.add(dn);
					
					
				}
			}
			
			NodeList conceptList = doc.getElementsByTagName("hyg:concept-def");
			for (int temp = 0; temp < conceptList.getLength(); temp++) {
				
				Node nNode = conceptList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element cElement = (Element) nNode;
					String conceptName = cElement.getElementsByTagName("hyg:concept-name").item(0).getTextContent();
					//System.out.println(conceptName);
					String specificity = (String)specificities.get(conceptName.toLowerCase() + "_specificity");
					if(specificity != null){
						if(cElement.getElementsByTagName("hyg:specificity").item(0) != null){
							cElement.getElementsByTagName("hyg:specificity").item(0).setTextContent(specificity);
						}else{
							Element newSpecificity = doc.createElement("hyg:specificity");
							newSpecificity.appendChild(doc.createTextNode(specificity));
							cElement.appendChild(newSpecificity);
						}
					}
				}
			}
			NodeList spec = doc.getElementsByTagName("hyg:hygiene-spec");
			Element newrid = doc.createElement("hyg:ridfield");
			newrid.appendChild(doc.createTextNode("spoonRecordID"));
			spec.item(0).appendChild(newrid);
			//doc.appendChild(newrid);
			//add exist code idExists
			if(idExists){
				Element newidExists = doc.createElement("hyg:idfieldExists");
				newidExists.appendChild(doc.createTextNode("true"));
				spec.item(0).appendChild(newidExists);
			}
			//write it out to a new file
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
		    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
		    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		    //transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(outFile));
			transformer.transform(source, result);
		}catch (Exception e){
			System.out.println("Failed to parse dataset" + e.toString());
		}finally{

			
		}
		return dataSet;
	}
	
	//loads the specificities from the csv file created during the specificities run
	public HashMap loadSpecificitiesData(String fileName){
		HashMap spec = new HashMap();
		
		FileInputStream fstream = null;
		DataInputStream in = null;
		BufferedReader br = null;
		
		try{
			fstream = new FileInputStream(fileName);
			in = new DataInputStream(fstream);
	        br = new BufferedReader(new InputStreamReader(in));
	    	CSVReader reader = new CSVReader(br,',','"','\\');
	    	String [] nextLine = null;
	    	String [] keys = null;
	    	String [] vals = null;
	    	int cnt = 0; 
	    	
	    	while ((nextLine = reader.readNext()) != null && cnt < 2) {
	    		if(cnt == 0){
	    			//build hash map keys
	    			keys = nextLine;
	    		}
	    		if(cnt == 1){
	    			//add values to hash map
	    			vals = nextLine;
	    		}
	    		cnt++;
	    	}
	    	if(keys.length == vals.length){
		    	for(int i = 0; i< keys.length; i++){
		    		
		    		if(vals[i].replaceAll("[.0-9]+","").length() == 0){
		    			//System.out.println("xx: |" + vals[i].replaceAll("[.0-9]+","") + "|");
		    			//System.out.println(keys[i] + " - " + vals[i]);
		    			Integer val = (int)Math.ceil(Float.parseFloat(vals[i]));
		    			spec.put(keys[i].toLowerCase(), val.toString());
		    		}
		    		//System.out.println("KEY: " + keys[i] + " Vals: " + Math.ceil(Float.parseFloat(vals[i])));
		    	}
	    	}
	    	
	    	fstream.close();
	    	in.close();
	    	br.close();
			
		}catch (Exception e){
			System.out.println("Error");
			e.printStackTrace();
		}finally{
			try{
				fstream.close();
			}catch (Exception e){}
			
			try{
				in.close();
			}catch (Exception e){}
			
			try{
				br.close();
			}catch (Exception e){}
			
		}
		return spec;
	}
	
	/*
	 * USE:
	 * point to the specificities output folder pick up salt.xml and Specificities.csv
	 * if user selects to include hygiene rules then use salt.xml from the hygiene folder
	 * this will build a unified salt.xml with all rules an specificities 
	 * 
	 * DestinationFolder and SpecificitiesFolder should always be populated, if not including hygiene rules pass empty string
	 */
	
	public ArrayList<DatasetNode> buildLinkingXML(String destinationFolder, String specificitiesFolder, String hygineFolder){
		String specificitiesFile = specificitiesFolder + "Specificities.csv";
		String hygineXML = "";
		if(hygineFolder != null && !hygineFolder.equals("")){
			hygineXML = hygineFolder + "salt.xml";
		}
		String specificitiesXML = specificitiesFolder + "salt.xml";
		String sourceXML = "";
		if(!hygineXML.equals("")){
			sourceXML = hygineXML;
		}else{
			sourceXML = specificitiesXML;
		}
		HashMap specs =  this.loadSpecificitiesData(specificitiesFile);
		ArrayList<DatasetNode> ds = this.loadDataset(sourceXML, specs, destinationFolder + "salt.xml");
		return ds;
	}
	/*
	 * Testing function
	 */
	public static void main(String[] args) throws Exception {
		LoadSpecificties ls = new LoadSpecificties();
		/*ArrayList<DatasetNode> ds = ls.buildLinkingXML("C:/Spoon Demos/new/salt/out_internal_clustering/",
													"C:/Spoon Demos/new/salt/out_specificities/",
													"C:/Spoon Demos/new/salt/out_hygine/");
													*/
		ArrayList<DatasetNode> ds = ls.buildLinkingXML("C:\\Spoon Demos\\new\\salt\\out_internal_clustering\\", "C:\\Spoon Demos\\new\\saltdemos\\specificity_out\\", "C:\\Spoon Demos\\new\\saltdemos\\datahygiene_out\\");
		//System.out.println(ls.getEclDatasetCode());
		/*for(int i = 0; i< ds.size(); i++){
        	System.out.println("Field Name : " + ds.get(i).getFieldName());
        	System.out.println("Field Type : " + ds.get(i).getFieldType());
        	System.out.println("Field Specificity : " + ds.get(i).getSpecificity());
        	System.out.println("CAPS : " + ds.get(i).getCaps());
        	System.out.println("Left Trim : " + ds.get(i).getLeftTrim());
        	System.out.println("On Fail : " + ds.get(i).getOnFail());
        }*/

    }

}

