package org.hpccsystems.ecldirect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class FileInfoSoap {
	ECLSoap soap;
		
	private String serverHost;
	private int serverPort;
	
	public FileInfoSoap(String serverHost,int serverPort){
		this.serverHost = serverHost;
		this.serverPort = serverPort;
	}

	public String buildSoapEnv (){
		//http://192.168.80.132:8010/WsDfu/DFUQuery?ver_=1.19&wsdl
				String soap = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
						"<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">" +
						"<soap:Body><DFUQuery xmlns=\"urn:hpccsystems:ws:wsdfu\">" +
						"<Prefix></Prefix>" +
						"<ClusterName></ClusterName>" +
						"<LogicalName></LogicalName>" +
						"<Description></Description>" +
						"<Owner></Owner>" +
						"<StartDate></StartDate>" +
						"<EndDate></EndDate>" +
						"<FileType></FileType>" +
						"<FileSizeFrom></FileSizeFrom>" +
						"<FileSizeTo></FileSizeTo>" +
						"<FirstN></FirstN>" +
						"<FirstNType></FirstNType>" +
						"<PageSize></PageSize>" +
						"<PageStartFrom></PageStartFrom>" +
						"<Sortby></Sortby>" +
						"<Descending></Descending>" +
						"<OneLevelDirFileReturn></OneLevelDirFileReturn>" +
						"</DFUQuery>" +
						"</soap:Body>" +
						"</soap:Envelope>";
		return soap;
	}
	
	public ArrayList<String> processReturn(InputStream is) throws Exception{
		ArrayList results = new ArrayList();

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        
        Document dom = db.parse(is);

        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("DFULogicalFiles");
        if (dsList != null && dsList.getLength() > 0) {
        	
           //ArrayList dsArray = new ArrayList();

           //results = dsArray;

            for (int i = 0; i < dsList.getLength(); i++) {
            	System.out.println("Node");
                Element ds = (Element) dsList.item(i);
                NodeList rowList = ds.getElementsByTagName("DFULogicalFile");
                if (rowList != null && rowList.getLength() > 0) {

                    ArrayList rowArray = new ArrayList();

                    for (int j = 0; j < rowList.getLength(); j++) {
                        Element row = (Element) rowList.item(j);
                        
                        NodeList columnList = row.getChildNodes();
                        for (int k = 0; k < columnList.getLength(); k++) {
                            
                            
                            if(columnList.item(k).getNodeName().equals("Name")){
                                //System.out.println("Name: " + columnList.item(k).getNodeName());
                               // System.out.println("Value: " + columnList.item(k).getTextContent());
                                results.add("~"+columnList.item(k).getTextContent());
                            }  
                        }  
                    }
                }

            }
        }
        Iterator iterator = results.iterator();
        return results;
	}
	
	/*
	 * fetchClusters
	 * 
	 * @accepts String ("ThorCluster", "RoxieCluster", "")
	 * @returns String[]
	 */
	public String[] fetchFiles(){
		
		soap = new ECLSoap();
		
		soap.setHostname(serverHost);
		soap.setPort(this.serverPort);
		
		String path = "/WsDfu/DFUQuery";
        InputStream is = soap.doSoap(buildSoapEnv(), path);
        
		try{
			ArrayList<String> c = processReturn(is);
			String[] clusters = new String[c.size()];
		    return c.toArray(clusters);
		}catch (Exception e){
			System.out.println("error");
			System.out.println(e);
		}
		return null;
	}
	
	public void fetchFileMeta(String fileName){
	 	String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
	 			"<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">" +
	 			"<soap:Body>" +
	 			"<DFUBrowseData xmlns=\"urn:hpccsystems:ws:wsdfu\">" +
	 			"<LogicalName>" + fileName + "</LogicalName>" +
	 			"<FilterBy></FilterBy>" +
	 			"<ShowColumns></ShowColumns>" +
	 			"<SchemaOnly>true</SchemaOnly>" +
	 			"<StartForGoback></StartForGoback>" +
	 			"<CountForGoback></CountForGoback>" +
	 			"<ChooseFile></ChooseFile>" +
	 			"<Cluster></Cluster>" +
	 			"<ClusterType></ClusterType>" +
	 			"<ParentName></ParentName>" +
	 			"<Start></Start>" +
	 			"<Count></Count>" +
	 			"<DisableUppercaseTranslation></DisableUppercaseTranslation>" +
	 			"</DFUBrowseData>" +
	 			"</soap:Body>" +
	 			"</soap:Envelope>";
	 	
	 	soap = new ECLSoap();
		
		soap.setHostname(serverHost);
		soap.setPort(this.serverPort);
		
		String path = "/WsDfu/DFUBrowseData?ver_=1.1";
		
		InputStream is = soap.doSoap(xml, path);
				
		/*
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;
        try {
			while((line = in.readLine()) != null) {
			  System.out.println(line);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		try{
			processFileMeta(is);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public ArrayList<String> processFileMeta(InputStream is) throws Exception{
		ArrayList results = new ArrayList();

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        
        Document dom = db.parse(is);

        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("DFUBrowseDataResponse");
        
        
        if (dsList != null && dsList.getLength() > 0) {
        	
           //ArrayList dsArray = new ArrayList();

           //results = dsArray;

            for (int i = 0; i < dsList.getLength(); i++) {
            	System.out.println("Node:" + dsList.item(i).getNodeName());
                Element ds = (Element) dsList.item(i);
                System.out.println(ds.getFirstChild().getNodeName());
                NodeList rowList = ds.getElementsByTagName("ColumnsHidden");
                System.out.println("Node:" + rowList.getLength());
                if (rowList != null && rowList.getLength() > 0) {
                		
                	
                	
                    ArrayList rowArray = new ArrayList();

                    for (int j = 0; j < rowList.getLength(); j++) {
                    	System.out.println("Node:");
                        Element row = (Element) rowList.item(j);
                        
                        NodeList columnList = row.getChildNodes();
                        for (int k = 0; k < columnList.getLength(); k++) {
                        	System.out.println("Name: " + columnList.item(k).getChildNodes());
                            
                        	NodeList colList = columnList.item(k).getChildNodes();
                        	
                        	for(int c=0; c<colList.getLength(); c++){
                        		System.out.println(colList.item(c).getNodeName() + " : " + colList.item(c).getTextContent());
                        	}
     
                        }  
                    }
                }

            }
        }
        Iterator iterator = results.iterator();
        return results;
	}
	
	public static void main(String[] args){
		FileInfoSoap c = new FileInfoSoap("192.168.80.132", 8010);
		/*String[] test = c.fetchFiles();
		System.out.println("You have " + test.length + " Files");
		for (int i = 0; i<test.length; i++){
			System.out.println(test[i]);
		}*/
		
		c.fetchFileMeta("~thor::foodmart::productsoutput");
		
	}
}
