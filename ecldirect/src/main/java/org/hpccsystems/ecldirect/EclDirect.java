/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.ecldirect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
//import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
//import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.hpccsystems.ecldirect.ECLSoap;

/**
 *
 * @author ChalaAX
 */
public class EclDirect {

    private final String urlString;
    private final String clusterName;
    
    private String serverHost;
    private String serverPort;
    private String jobName;
    private String eclccInstallDir;
    private String includeML;
    
    private String mlPath;
    
    private String wuid;
    
     private String outputName = "";

    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public String getWuid() {
        return wuid;
    }

    public void setWuid(String wuid) {
        this.wuid = wuid;
    }
    
    

    public String getEclccInstallDir() {
        return eclccInstallDir;
    }

    public void setEclccInstallDir(String eclccInstallDir) {
        this.eclccInstallDir = eclccInstallDir;
    }

    public String getIncludeML() {
        return includeML;
    }

    public void setIncludeML(String includeML) {
        this.includeML = includeML;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
    
    public String getMlPath() {
        return mlPath;
    }

    public void setMlPath(String mlPath) {
        this.mlPath = mlPath;
    }

    public String getServerHost() {
        return serverHost;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    

    public EclDirect(String clusterAddress, String clusterName, String clusterPort) {
      if(clusterPort.equals("")){
          //if direct
          //clusterPort = "8008";
          
          //soap uses 8010 default
          clusterPort = "8010";
          
      }
      this.serverHost = clusterAddress;
      this.serverPort = clusterPort;
      urlString = "http://" + clusterAddress + ":"+ clusterPort+"/EclDirect/RunEcl";
      
      System.out.println("URLString: " + urlString);
      this.clusterName = clusterName;
    }

    
    
    
    public ArrayList execute(String eclCode){
        /*
        ECLSoap es = new ECLSoap();
        es.setCluster(clusterName);
        es.setEclccInstallDir(eclccInstallDir);
        es.setHostname(serverHost);
        es.setOutputName(outputName);
        //System.out.println("includeML++++++++++++++++++: " + this.includeML);
        if(this.includeML.equals("true")){
            es.setIncludeML(true);
        }else{
            es.setIncludeML(false);
        }
        es.setJobName(jobName);
        es.setMlPath(mlPath);
        
        es.setPort(Integer.parseInt(this.serverPort));
        */
        ECLSoap es = getECLSoap();
        
        Boolean proceed = es.executeECL(eclCode);
        this.wuid = es.getWuid();
        
        ArrayList results = null;
        try{
            if(proceed){
                InputStream is = es.ResultsSoapCall(wuid,"");
                results = es.parseResults(is);
            }else{
                System.out.println("ECL Failed");
            }
        }catch(Exception e){
             System.out.println(e);
             e.printStackTrace();
        }
        return results;
    }
    
    
    public Boolean execute_noResults(String eclCode){
        /*ECLSoap es = new ECLSoap();
        es.setCluster(clusterName);
        es.setEclccInstallDir(eclccInstallDir);
        es.setHostname(serverHost);
        //System.out.println("includeML++++++++++++++++++: " + this.includeML);
        if(this.includeML.equals("true")){
            es.setIncludeML(true);
        }else{
            es.setIncludeML(false);
        }
        es.setJobName(jobName);
        es.setMlPath(mlPath); 
        es.setOutputName(outputName);
        es.setPort(Integer.parseInt(this.serverPort));
         * 
         */
        ECLSoap es = getECLSoap();
        Boolean proceed = es.executeECL(eclCode);
        this.wuid = es.getWuid();
        return proceed;
    }
    
    
    public ArrayList resultList(){
        ArrayList al = null;
        /*ECLSoap es = new ECLSoap();
        es.setCluster(clusterName);
        es.setEclccInstallDir(eclccInstallDir);
        es.setHostname(serverHost);
        es.setJobName(jobName);
        es.setMlPath(mlPath); 
        es.setOutputName(outputName);
        es.setPort(Integer.parseInt(this.serverPort));
        
         if(this.includeML.equals("true")){
            es.setIncludeML(true);
        }else{
            es.setIncludeML(false);
        }
         * 
         */
        ECLSoap es = getECLSoap();
        InputStream is = es.InfoDetailsCall(this.wuid);
        try{
            al = es.parseResultList(is);
        }catch (Exception e){
         System.out.println(e.toString());   
        }
        
        return al;
    }
    
    
    public ArrayList executeDirect(String eclCode) {
       try {  

            String data = URLEncoder.encode("eclText", "UTF-8") + "=" + URLEncoder.encode(eclCode, "UTF-8");
            data += "&" + URLEncoder.encode("cluster", "UTF-8") + "=" + URLEncoder.encode(clusterName, "UTF-8");

            // Send data
            URL url = new URL(urlString);
            URLConnection conn = url.openConnection();
            conn.setDoOutput(true);

            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(data);
            wr.flush();

            // Get the response
            return parse(conn.getInputStream());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ArrayList parse(InputStream xml) throws Exception {
        ArrayList results = null;

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();


        Document dom = db.parse(xml);
        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("Dataset");
        if (dsList != null && dsList.getLength() > 0) {

            ArrayList dsArray = new ArrayList();

            results = dsArray;

            for (int i = 0; i < dsList.getLength(); i++) {
                Element ds = (Element) dsList.item(i);
                NodeList rowList = ds.getElementsByTagName("Row");

                if (rowList != null && rowList.getLength() > 0) {

                    ArrayList rowArray = new ArrayList();

                    dsArray.add(rowArray);

                    for (int j = 0; j < rowList.getLength(); j++) {
                        Element row = (Element) rowList.item(j);
                        
                        NodeList columnList = row.getChildNodes();
                        
                        ArrayList columnsArray = new ArrayList();
                        rowArray.add(columnsArray);
                        
                        for (int k = 0; k < columnList.getLength(); k++) {
                            System.out.println(columnList.item(k).getNodeName());
                           columnsArray.add(new Column(columnList.item(k).getNodeName(), columnList.item(k).getTextContent()));
                        }
                    }
                }

            }
        }
        
        return results;
    }
    
    public ECLSoap getECLSoap(){
        ECLSoap es = new ECLSoap();
        es.setCluster(clusterName);
        es.setEclccInstallDir(eclccInstallDir);
        es.setHostname(serverHost);
        es.setJobName(jobName);
        es.setMlPath(mlPath); 
        es.setOutputName(outputName);
        es.setPort(Integer.parseInt(this.serverPort));
        return es;
    }
     public String convertInputStreamToString(InputStream ists) throws IOException {
       
        if (ists != null) {
            StringBuilder sb = new StringBuilder();
            String line;
 
            try {
                BufferedReader r1 = new BufferedReader(new InputStreamReader(ists, "UTF-8"));
                while ((line = r1.readLine()) != null) {
                    sb.append(line).append("\n");
                }
            } finally {
                ists.close();
            }
            return sb.toString();
        } else {       
            return "";
        }
    }
}
