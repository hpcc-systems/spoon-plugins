/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ChambeJX
 */
import org.hpccsystems.ecldirect.ECLSoap;
import java.net.*;
import java.io.*;
import java.util.*;

import org.hpccsystems.ecldirect.Column;

public class testECLSoap {

    
     public static void main(String[] args) {
         /*
           String inFile = "spoon-ecl\\spoon-eclCode.ecl";
        String outFile = "spoon-ecl\\spoon-eclOut.ecl";
        String installDir = "C:\\Program Files\\HPCC Systems\\HPCC\\bin\\ver_3_0\\";
         //write ecl to file
        
        String inFilePath = "\"" + installDir + inFile + "\"";
        String outFilePath = "\"" + installDir + outFile + "\"";
        
         ECLSoap es = new ECLSoap();
         //String query = "&lt;Archive build=&quot;community_3.4.0-1&quot; eclVersion=&quot;3.0.0&quot; legacyMode=&quot;0&quot;&gt; &lt;Query attributePath=&quot;_local_directory_.HelloWorld&quot;/&gt; &lt;Module key=&quot;_local_directory_&quot; name=&quot;_local_directory_&quot;&gt;  &lt;Attribute key=&quot;helloworld&quot; name=&quot;helloworld&quot; sourcePath=&quot;C:\\dev\\eclipse\\HelloWorld\\HelloWorld.ecl&quot;&gt;   OUTPUT(&amp;apos;Test6&amp;apos;);  &lt;/Attribute&gt; &lt;/Module&gt;&lt;/Archive&gt;";
         //String compiled_ecl = openFile(installDir+outFile);
         
         String ecl = "IMPORT * FROM ML;"+
                        "IMPORT * FROM ML.Cluster;"+
                        "IMPORT * FROM ML.Types;"+
                        "x2 := DATASET(["+
                        "{1, 1, 1}, {1, 2, 5},"+
                        "{2, 1, 5}, {2, 2, 7},"+
                        "{3, 1, 8}, {3, 2, 1},"+
                        "{4, 1, 0}, {4, 2, 0},"+
                        "{5, 1, 9}, {5, 2, 3},"+
                        "{6, 1, 1}, {6, 2, 4},"+
                        "{7, 1, 9}, {7, 2, 4}],NumericField);"+
                        "c := DATASET(["+
                        "{1, 1, 1}, {1, 2, 5},"+
                        "{2, 1, 5}, {2, 2, 7},"+
                        "{3, 1, 9}, {3, 2, 4}],NumericField);"+
                        "x3 := Kmeans(x2,c);"+
                        "OUTPUT(x3);";
       // String compiled_ecl = es.executeECL(ecl);
        // System.out.print(compiled_ecl);
        try{
          ArrayList dsList = es.executeECL(ecl);
          System.out.println("Test ArrayList");
         
          
          
       
        for (int i = 0; i < dsList.size(); i++) {
            
            ArrayList rowList = (ArrayList) dsList.get(i);
            
            for (int j = 0; j < rowList.size(); j++) {
                ArrayList columnList = (ArrayList) rowList.get(j);
                System.out.println("Row:");
                for (int k = 0; k < columnList.size(); k++) {
                    Column column = (Column) columnList.get(k);
                    System.out.print(column.getName() + "=" + column.getValue() + "|");
                }
                System.out.println("");
            }
        }
          
        }catch (Exception e){
            System.out.println(e.toString());
            
        }
         //hardcodedtest();

         //String wuid = es.createWorkUnit();
         //System.out.println("NEW WUID: " + wuid);
        // es.updateWorkUnit(wuid);
         */
         
     }
    
     
    private static String openFile(String filePath){
        StringBuffer fileData = new StringBuffer(1000);
        System.out.println("++++++++++++++++ Open File: " + filePath);
         try{
        
        BufferedReader reader = new BufferedReader(
                new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
         }catch (Exception e){//Catch exception if any
        System.err.println("Error: " + e.getMessage());
     }
        return fileData.toString();
    }

    
     
     
     public static void hardcodedtest(){
         ECLSoap es = new ECLSoap();
         String wuid = "";
         //es.hardcodedtest();
         
         String query = "&lt;Archive build=&quot;community_3.4.0-1&quot; eclVersion=&quot;3.0.0&quot; legacyMode=&quot;0&quot;&gt; &lt;Query attributePath=&quot;_local_directory_.HelloWorld&quot;/&gt; &lt;Module key=&quot;_local_directory_&quot; name=&quot;_local_directory_&quot;&gt;  &lt;Attribute key=&quot;helloworld&quot; name=&quot;helloworld&quot; sourcePath=&quot;C:\\dev\\eclipse\\HelloWorld\\HelloWorld.ecl&quot;&gt;   OUTPUT(&amp;apos;Test6&amp;apos;);  &lt;/Attribute&gt; &lt;/Module&gt;&lt;/Archive&gt;";
         
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + 
        "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" + 
          "<soapenv:Body>" + 
             "<WUCreateAndUpdate xmlns=\"urn:hpccsystems:ws:wsworkunits\">" + 
                "<Jobname>HelloWorld</Jobname>" + 
                "<Cluster>MyThor</Cluster>" + 
                "<QueryText>" + query + "</QueryText>" + 
               " <ApplicationValues>" + 
                   "<ApplicationValue>" + 
                      "<Application>org.hpccsystems.eclide</Application>" + 
                      "<Name>path</Name>" +
                      "<Value>/HelloWorld/HelloWorld.ecl</Value>" + 
                   "</ApplicationValue>" + 
                "</ApplicationValues>" + 
             "</WUCreateAndUpdate>" + 
          "</soapenv:Body>" + 
        "</soapenv:Envelope>";
        String path = "/WsWorkunits/WUCreateAndUpdate";
        InputStream is = es.doSoap(xml, path);
        
        try{
           // Map response = es.parse(is);
           //System.out.println("NEW-- " + (String)response.get("Wuid"));
           //wuid = (String)response.get("Wuid");
            //response.
         }catch (Exception e){
             System.out.println(e);
         }
        
        
        xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+
               "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"+
                  "<soapenv:Body>"+
                     "<WUSubmit xmlns=\"urn:hpccsystems:ws:wsworkunits\">"+
                        "<Wuid>" + wuid + "</Wuid>"+
                        "<Cluster>hthor</Cluster>"+
                     "</WUSubmit>"+
                  "</soapenv:Body>"+
               "</soapenv:Envelope>";
        
        path = "/WsWorkunits/WUSubmit";
        InputStream is2 = es.doSoap(xml, path);
    }
}
