package org.hpccsystems.pentaho.job.eclexecute;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVReader;


public class RenderWebDisplay {

	public static void main(String[] args){
		RenderWebDisplay rwd = new RenderWebDisplay();
		String file = "C:\\Spoon Demos\\new\\salt_dataprofiling_quickstart\\out\\Dataprofiling_AllProfiles.csv";
		file = "C:\\Documents and Settings\\ChambeJX.RISK\\Desktop\\toyota\\out_clean2\\NEW\\Dataprofiling_AllProfiles.csv";
		String outFolder = "C:\\Documents and Settings\\ChambeJX.RISK\\Desktop\\toyota\\out_clean\\NEW";
		rwd.processFile(file, outFolder);
		}
	
	public void processFile(String fileName, String outFolder){
		String fileStruct = outFolder + "\\Dataprofiling_OptimizedLayout.csv";
		
		String html = "";
		html += makeHTMLHeader();
		html += this.fileDataToTableRows(fileName);
		html += this.renderCloseTableHTML();
		html += this.renderStructure(fileStruct);
		html += "</body></html>";
		String outFile = outFolder + "\\demoHTML.html";
		
		//write file
		try {
            BufferedWriter out = new BufferedWriter(new FileWriter(outFile));            
            out.write(html);
            out.close();
       
        } catch (IOException e) {
            e.printStackTrace();
        }  
		
		this.openUrl(outFile);
		
	}
	
	private String renderStructure(String fileName){
		String html = "<h2>Record Layout</h2><pre style='background-color:#CECECE; width:800px;'>";
		try{
		 FileInputStream fstream = new FileInputStream(fileName);
         // Get the object of DataInputStream
         DataInputStream in = new DataInputStream(fstream);
         BufferedReader br = new BufferedReader(new InputStreamReader(in));
         String strLine;
         boolean isFirst = true;
         while ((strLine = br.readLine()) != null)   {
        	if(!isFirst)
        		html += strLine + "\r\n";
        	isFirst = false;
         }
         
         in.close();
	     }catch (Exception e){//Catch exception if any
	        System.err.println("Error: " + e.getMessage());
	        e.printStackTrace();
	     }
		html += "</pre>";
		return html;
	}
	private String fileDataToTableRows(String fileName){
	      String html = "\n\r";
	      
	      
	      int line = 1;
	      try{
	    	// Open the file that is the first 
	          FileInputStream fstream = new FileInputStream(fileName);
	          DataInputStream in = new DataInputStream(fstream);
	          BufferedReader br = new BufferedReader(new InputStreamReader(in));
	          
	    	  CSVReader reader = new CSVReader(br,',','"','\\');
	    	  // CSVReader reader = new CSVReader(new FileReader(fileName),',','"');
	    	  
	    	  
	          // Open the file that is the first 
	          // command line parameter
	          //FileInputStream fstream = new FileInputStream(fileName);
	          String [] nextLine;
	          // Get the object of DataInputStream
	          //DataInputStream in = new DataInputStream(fstream);
	          //BufferedReader br = new BufferedReader(new InputStreamReader(in));
	          String strLine;	          
	          int firstRowLen = 0;
	          boolean first = true;
	          boolean second = false;
	          //Read File Line By Line
	          String len = "";
	          String words = "";
	          String characters = "";
	          String patterns = "";
	          String freqTerms = "";
	          String fldno = "";
	          boolean newTable = false;
	          
	          while ((nextLine = reader.readNext()) != null) {
	        	  //System.out.println("LINE: " + line++ + " LEN: " + nextLine.length);
	        	  //if(nextLine[1].equals("customer_key_1") || nextLine[1].equals("name")){
	        		//  System.out.println(nextLine[1] + " " + nextLine.length);
	        	  //}
	        	  String[] lineArr = nextLine;
	          //while ((strLine = br.readLine()) != null)   {
	          // Print the content on the console
	        	  //String regex = "\"(\\([^)]*\\)|[^\"])*\"";

	        	 // String[] lineArr = strLine.split(regex);
	        	  //String[] lineArr = strLine.split(",(?![^()]*+\\))");
/*
	              String[] lineArr = strLine.split("\\,");
	        	  String[] lineArr2 = new String[6];
	        	  String regex = "\"(\\([^)]*\\)|[^\"])*\"";
	        	  Pattern p = Pattern.compile(regex);
	        	  Matcher m = p.matcher(strLine);
	        	  int cnt = 0;
	        	  while(m.find()) {
	        		  System.out.println(strLine.substring(m.start(),m.end()));
	        		  lineArr2[cnt++] = strLine.substring(m.start(),m.end());
	        	  }

	*/        	  
	        	  
	              if(first){
	            	  firstRowLen = lineArr.length;
	            	  first = false;
	            	  second = true;
	              }else{
		           //  html += renderRows(strLineArr,length);
		              	if(lineArr.length >= firstRowLen){
		              	//if(lineArr[2] != ""){
			      			System.out.println("NAME: " + lineArr[1]);
			            	html += "\n\r<tr>" +
			                      "<td>" + lineArr[0] + "</td>" + //fldno
			                      "<td>" + lineArr[1] + "</td>" + //fieldname
			                      "<td>" + lineArr[2] + "</td>"; //cardinality
			                      ;
			                newTable = true;
			                fldno = lineArr[0];
			      			len = lineArr[3];
			      			//System.out.println("DEFINE LEN: |" + len + "|");
			      			words = lineArr[4];
			      			characters = lineArr[5];
			      			patterns = lineArr[6];
			      			freqTerms = lineArr[7];
			      			/*
			                      "<td>" + lineArr[3] + "</td>" + //len
			                      "<td>" + lineArr[4] + "</td>" + //words
			                      "<td>" + lineArr[5] + "</td>" + //characters
			                      "<td>" + lineArr[6] + "</td>" + //patterns
			                      "<td>" + lineArr[7] + "</td>" + //frequent terms
			                      "</tr>";*/

		      			}else{
			      				//its an odd length so thus a sub row
			      			
			      			if(!len.equals("")){
			      				
			      				 //System.out.println("|"+len+"|");
			      				if(newTable){
			      					html += "<td colspan='2'>";
			      					html += "<a href=\"javascript:void(0)\" onclick=\"showAndHide(this, 'tblLen-" + fldno + "');\">[+] Show</a>";
			      					html += "<table id='tblLen-" + fldno + "' class='inner' style='display:none;'><tbody>";
			      					newTable = false;
			      				}
			      				//System.out.println("len------");
				      			int thisLen = lineArr.length;
				      			String subStr = "";
				      			html += "<tr>";
				      			 for(int i =0; i<thisLen; i++){
				      				 subStr += lineArr[i];
				      				
				      				html += "<td>" + lineArr[i] + "</td>";
				      			 }
				      			 html += "</tr>";
				      			 //System.out.println(subStr.length());
				      			 //System.out.println(len.length());
				      			 //if(subStr.length()<=len.length()){
				      			
				      			   len = len.substring(subStr.length(),len.length());
				      			   //System.out.println("|"+len+"|");
			      				 //}else{
			      				//	html += "</tbody></table></td>";
				      			//	 newTable = true;
			      				// }
				      			
				      			 if(len.equals("")){
				      				// System.out.println("new table");
				      				 html += "</tbody></table></td>";
				      				 newTable = true;
				      			 }
			      			}else if(!words.equals("")){
			      				if(newTable){
			      					html += "<td colspan='2'>";
			      					html += "<a href=\"javascript:void(0)\" onclick=\"showAndHide(this, 'tblWords-" + fldno + "');\">[+] Show</a>";
			      					html += "<table id='tblWords-" + fldno + "' class='inner' style='display:none;'><tbody>";
			      					newTable = false;
			      				}
				      			int thisLen = lineArr.length;
				      			String subStr = "";
				      			html += "<tr>";
				      			for(int i =0; i<thisLen; i++){
				      				 subStr += lineArr[i];
				      				
				      				html += "<td>" + lineArr[i] + "</td>";
				      			 }
				      			html += "</tr>";
				      			words = words.substring(subStr.length(),words.length());
				      			
				      			if(words.equals("")){
				      				 html += "</table></td>";
				      				 newTable = true;
				      			 }
			      			}else if(!characters.equals("")){
			      				if(newTable){
			      					html += "<td colspan='2'>";
			      					html += "<a href=\"javascript:void(0)\" onclick=\"showAndHide(this, 'tblChars-" + fldno + "');\">[+] Show</a>";
			      					html += "<table id='tblChars-" + fldno + "' class='inner' style='display:none;'><tbody>";
			      					newTable = false;
			      				}
				      			int thisLen = lineArr.length;
				      			String subStr = "";
				      			html += "<tr>";
				      			for(int i =0; i<thisLen; i++){
				      				 subStr += lineArr[i];
				      				 
				      				html += "<td>" + lineArr[i] + "</td>";
				      			 }
				      			html += "</tr>";
				      			//if(characters.length()>=subStr.length()){
				      				
				      			
				      			characters = characters.substring(subStr.length(),characters.length());
				      			//}else{
				      			//	System.out.println("subStr: " + subStr);
				      			//	characters = characters.substring(subStr.length(),characters.length());
				      			//}
				      			if(characters.equals("")){
				      				 html += "</table></td>";
				      				 newTable = true;
				      			 }
			      			}else if(!patterns.equals("")){
			      				if(newTable){
			      					html += "<td colspan='2'>";
			      					html += "<a href=\"javascript:void(0)\" onclick=\"showAndHide(this, 'tblPatterns-" + fldno + "');\">[+] Show</a>";
			      					html += "<table id='tblPatterns-" + fldno + "' class='inner' style='display:none;'><tbody>";
			      					newTable = false;
			      				}
				      			int thisLen = lineArr.length;
				      			String subStr = "";
				      			html += "<tr>";
				      			for(int i =0; i<thisLen; i++){
				      				 subStr += lineArr[i];
				      				
				      				html += "<td>" + lineArr[i] + "</td>";
				      			 }
				      			html += "</tr>";
				      			patterns = patterns.substring(subStr.length(),patterns.length());
				      			
				      			if(patterns.equals("")){
				      				 html += "</table></td>";
				      				 newTable = true;
				      			 }
			      			}else if(!freqTerms.equals("")){
			      				if(newTable){
			      					html += "<td colspan='2'>";
			      					html += "<a href=\"javascript:void(0)\" onclick=\"showAndHide(this, 'tblFrequent-" + fldno + "');\">[+] Show</a>";
			      					html += "<table id='tblFrequent-" + fldno + "' class='inner' style='display:none;'><tbody>";
			      					newTable = false;
			      				}
				      			int thisLen = lineArr.length;
				      			String subStr = "";
				      			html += "<tr>";
				      			for(int i =0; i<thisLen; i++){
				      				 subStr += lineArr[i];
				      				 
				      				html += "<td>" + lineArr[i] + "</td>";
				      			 }
				      			html += "</tr>";
				      			freqTerms = freqTerms.substring(subStr.length(),freqTerms.length());
				      			if(freqTerms.equals("")){
				      				 html += "</table></td>";
				      				 newTable = true;
				      			 }
			      			}
		      		}
		              second = false;
		              
	              }      
	          }
	         
	          //Close the input stream
	          //in.close();
	    }catch (Exception e){//Catch exception if any
	        System.err.println("Error: " + e.getMessage());
	        e.printStackTrace();
	        System.out.println("LINE: " + line++ + " ");
	     }
	      return html;

	  }

	public String renderRows(String[] lineArr,int firstRowLen){
		String html = "";
		if(lineArr.length == firstRowLen){
			html = "<tr>" +
                "<td>" + lineArr[0] + "</td>" + //fldno
                "<td>" + lineArr[1] + "</td>" + //fieldname
                "<td>" + lineArr[2] + "</td>" + //cardinality
                "<td>" + lineArr[3] + "</td>" + //len
                "<td>" + lineArr[4] + "</td>" + //words
                "<td>" + lineArr[5] + "</td>" + //characters
                "<td>" + lineArr[6] + "</td>" + //patterns
                "<td>" + lineArr[7] + "</td>" + //frequent terms
                "</tr>";
			
			 String[] test = lineArr[3].split(" ");
			// System.out.println("len: " + test.length);
			//System.out.println(firstRowLen + ": " + html);
		}
		return html;
	}

	public String renderCloseTableHTML(){
		String html = "</tbody></table>";
		return html;
	}
	
	public String makeHTMLHeader(){
		
		String output = "";
		
		output = "<html><head>";
		output = "<script src='http://code.jquery.com/jquery-latest.js'></script>";
		output += "<style TYPE='text/css'> <!--";
		
		output += "table.reference {" +
				"background-color:#ffffff;" +
				"border:1px solid #c3c3c3;" +
				"border-collapse:collapse;" +
				"}" +
				"table.reference th {" +
				"background-color:#e5eecc;" +
				"border:1px solid #c3c3c3;" +
				"padding:3px;" +
				"vertical-align:top;" +
				"}" +
				"table.reference td {" +
				"border:1px solid #c3c3c3;" +
				"padding:3px;" +
				"vertical-align:top;" +
				"}" +
				"table.reference tr {" +
				"display: table-row;" +
				"vertical-align: inherit;" +
				"border-color: inherit;" +
				"}" +
				"table.inner{" +
				"width: 100%;" +
				"border-collapse: collapse;" +
				"}" +
				"table.inner tr:hover{" +
				"background-color: lightgoldenrodyellow;" +
				"}" +
				"table.inner td:first-child{" +
				"border: none;" +
				"text-align: left;" +
				"}" +
				"table.inner td:last-child{" +
				"border: none;" +
				"text-align: right;" +
				"}" +
				"table.inner td{" +
				"padding: 0;" +
				"margin: 0;" +
				"}";
		output += "--></style>" + "\n\r";
		
		output += "<script type=\"text/javascript\">" + "\n\r" +
				"//<![CDATA[" + "\n\r" +
				"function showAndHide(lnk, tblId){" + "\n\r" +
				"var options = ['[-] Hide', '[+] Show'];" + "\n\r" +
				"var isVisible = ($(lnk).text() == options[1]);" + "\n\r" +
				"$(lnk).text(isVisible ? options[0] : options[1]);" + "\n\r" +
				"$('#' + tblId).toggle();" + "\n\r" +
				"}" + "\n\r" +
				"//]]>" +"\n\r" +
				"</script>" +"\n\r";
		output += "</head><body>" +"\n\r" ;
		output += "<h2>Data Profiling Report</h2>";
		output += "<table class='reference' style='width:900px'>" +
			    "<thead>" +
			    "    <tr valign='bottom'>" +
			    "        <th rowspan='2'>fldno</th>" +
			    "        <th rowspan='2'>fieldname</th>" +
			    "        <th rowspan='2'>cardinality</th>" +
			    "        <th colspan='2'>len</th>" +
			    "        <th colspan='2'>words</th>" +
			    "        <th colspan='2'>characters</th>" +
			    "        <th colspan='2'>patterns</th>" +
			    "        <th colspan='2'>frequent terms</th>" +
			    "    </tr>" +
			    "    <tr valign='bottom'>" +
			    "        <th rowspan='1'>len</th>" +
			    "        <th rowspan='1'>cnt</th>" +
			    "        <th rowspan='1'>words</th>" +
			    "        <th rowspan='1'>cnt</th>" +
			    "        <th rowspan='1'>c</th>" +
			    "        <th rowspan='1'>cnt</th>" +
			    "        <th rowspan='1'>data pattern</th>" +
			    "        <th rowspan='1'>cnt</th>" +
			    "        <th rowspan='1'>val</th>" +
			    "        <th rowspan='1'>cnt</th>" +
			    "    </tr>" +
			    "</thead><tbody>";
		
		
		
		return output;
	}
	
	public String data_profiling_demo_buildLayout(String res){
		//{if $ecl} 
		String outStr = "";
		if(res != null){   
			outStr = "<h2>Record Layout</h2>" +
			"<pre>" + res + "</pre>";

		}
		return outStr;
	}
	
	

	public void openUrl(String url){
		String os = System.getProperty("os.name");
		Runtime runtime=Runtime.getRuntime();
		try{
			// Block for Windows Platform
			if (os.startsWith("Windows")){
				String cmd = "rundll32 url.dll,FileProtocolHandler "+ url;
				Process p = runtime.exec(cmd);
			
			//Block for Mac OS
			}else if(os.startsWith("Mac OS")){
				Class fileMgr = Class.forName("com.apple.eio.FileManager");
				Method openURL = fileMgr.getDeclaredMethod("openURL", new Class[] {String.class});
				openURL.invoke(null, new Object[] {url});
			
			//Block for UNIX Platform
			}else {
				String[] browsers = {"firefox", "opera", "konqueror", "epiphany", "mozilla", "netscape" };
				String browser = null;
				for (int count = 0; count < browsers.length && browser == null; count++)
					if (runtime.exec(new String[] {"which", browsers[count]}).waitFor() == 0)
						browser = browsers[count];
				
				if (browser == null)
					throw new Exception("Could not find web browser");
				else
					runtime.exec(new String[] {browser, url});
			}
		}catch(Exception x){
			System.err.println("Exception occurd while invoking Browser!");
			x.printStackTrace();
		}
	}
}
