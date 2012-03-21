import java.net.*;
import java.io.*;

public class PostXml {

  public static void main(String[] args) {
		
    try {
      String xmldata = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + 
	"<env:Envelope " + 
	  "env:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" " + 
	  "xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\" " + 
	  "xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" " + 
	  "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" +
	  "<env:Header/>" +
          "<env:Body>" +
	    "<ans1:WUCreate xmlns:ans1=\"urn:hpccsystems:ws:wsworkunits\">" +
	      "<String_1 xsi:type=\"xsd:string\">your message or e-mail</String_1>" +
	    "</ans1:WUCreate>" +
	  "</env:Body>" +
        "</env:Envelope>";
			
      //Create socket
      String hostname = "192.168.80.129";
      int port = 8010;
      InetAddress  addr = InetAddress.getByName(hostname);
      Socket sock = new Socket(addr, port);
			
      //Send header
      String path = "/WsWorkunits/WUCreate";
      BufferedWriter  wr = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(),"UTF-8"));
      // You can use "UTF8" for compatibility with the Microsoft virtual machine.
      wr.write("POST " + path + " HTTP/1.0\r\n");
      wr.write("Host: www.pascalbotte.be\r\n");
      wr.write("Content-Length: " + xmldata.length() + "\r\n");
      wr.write("Content-Type: text/xml; charset=\"utf-8\"\r\n");
      wr.write("\r\n");
			
      //Send data
      wr.write(xmldata);
      wr.flush();
			
      // Response
      BufferedReader rd = new BufferedReader(new InputStreamReader(sock.getInputStream()));
      String line;
      while((line = rd.readLine()) != null)
	System.out.println(line);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
