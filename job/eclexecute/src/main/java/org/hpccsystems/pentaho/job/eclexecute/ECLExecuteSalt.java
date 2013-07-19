package org.hpccsystems.pentaho.job.eclexecute;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.ecljobentrybase.ECLJobEntry;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.salt.hygiene.Generate;
import org.hpccsystems.saltui.concept.ConceptEntryBO;
import org.hpccsystems.saltui.hygiene.HygieneEntryBO;
import org.hpccsystems.saltui.hygiene.HygieneEntryList;
import org.hpccsystems.saltui.hygiene.HygieneRuleBO;
import org.hpccsystems.saltui.hygiene.SaltAutoPopulate;
import org.pentaho.di.core.Result;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;

public class ECLExecuteSalt extends ECLJobEntry {

	public static String buildDatasetSalt(Result result, JobMeta jobMeta, String fileName, String error){
		String xmlHygieneBuilder = "";
		String layoutECL = "";
		String serverHost = "";
        String serverPort = "";
        String cluster = "";
        String jobName = "";
        String jobNameNoSpace ="";
        String maxReturn = "";
        String eclccInstallDir = "";
        String mlPath = "";
        String includeML = "";
        String user = "";
        String pass = "";
        String dsECL = "";
        

        String rsDef = "";
        String dsDef = "";
        String recordName = "";
        String dsName = "";
        String logicalFile = "";
        String fileType = "";
        
        String SALTPath = "";
        String includeSALT = "";
        String saltIncludePath = "";
        Boolean isInternalLinking = false;
        
        AutoPopulate ap = new AutoPopulate();
        try{
        //Object[] jec = this.jobMeta.getJobCopies().toArray();

            serverHost = ap.getGlobalVariable(jobMeta.getJobCopies(),"server_ip");
            serverPort = ap.getGlobalVariable(jobMeta.getJobCopies(),"server_port");

            cluster = ap.getGlobalVariable(jobMeta.getJobCopies(),"cluster");
            jobName = ap.getGlobalVariable(jobMeta.getJobCopies(),"jobName");
            jobNameNoSpace = jobName.replaceAll("[^A-Za-z0-9]", "");//jobName.replace(" ", "_"); 
            maxReturn = ap.getGlobalVariable(jobMeta.getJobCopies(),"maxReturn");
            eclccInstallDir = ap.getGlobalVariable(jobMeta.getJobCopies(),"eclccInstallDir");
            mlPath = ap.getGlobalVariable(jobMeta.getJobCopies(),"mlPath");
            includeML = ap.getGlobalVariable(jobMeta.getJobCopies(),"includeML");
            user = ap.getGlobalVariable(jobMeta.getJobCopies(),"user_name");
            pass = ap.getGlobalVariableEncrypted(jobMeta.getJobCopies(),"password");

            isInternalLinking = ap.hasNodeofType(jobMeta.getJobCopies(), "SaltInternalLinking");
            
            SALTPath = ap.getGlobalVariable(jobMeta.getJobCopies(),"SALTPath");
            includeSALT = ap.getGlobalVariable(jobMeta.getJobCopies(),"includeSALT");
            
            //System.out.println("@@@@@@@@@@@@@@@@@@@" + includeML + "@@@@@@");

        }catch (Exception e){
            System.out.println("Error Parsing existing Global Variables ");
            System.out.println(e.toString());
            e.printStackTrace();

        }
		 //insert code here to build spec file on compile
        if(includeSALT.equalsIgnoreCase("true")){
        	saltIncludePath = fileName+ "";
        }
        if(includeSALT.equalsIgnoreCase("true") && !isInternalLinking){
        //System.out.println("----------- insert code here to build spec file on compile");
	        try{
	        	//find all the datasets and build xml files
	        	String[] datasets = ap.parseDatasets(jobMeta.getJobCopies());
	        	
	        	if(datasets.length != 1){
	        		//set error state WE ONLY ALLOW ONE DATASET
	        		
	        	}
	        	String file_name = "";
	        	
	        	for(int i = 0; i < datasets.length; i++){
	        		 //iterate through all the xml files and build a specification file.
	        		//System.out.println("dataset: " + datasets[i]);
	        		RecordList fields = ap.rawFieldsByDataset(datasets[i],jobMeta.getJobCopies());
	        		//have field declaration now we need to build the xml
	        		String fieldCSV = "";
	        		for (Iterator<RecordBO> iterator = fields.getRecords().iterator(); iterator.hasNext();) {
	        			RecordBO obj = (RecordBO) iterator.next();
	 
	        			//TODO: fix this so it uses the ID passed in Hygine
	        			if(!obj.getColumnName().equalsIgnoreCase("spoonClusterID")){
	        				if(fieldCSV.equals("")){
	        					fieldCSV += obj.getColumnName();
	        				}else{
	        					fieldCSV += ","+obj.getColumnName();
	        				}
	        				xmlHygieneBuilder += buildHygieneRule(jobMeta, datasets[i], obj.getColumnName(),obj.getColumnType());
	        			}
	        			//if(obj.getColumnName().equalsIgnoreCase("spoonClusterID")){
	        				//xmlHygieneBuilder +="<hyg:idname>" + "spoonClusterID" + "</hyg:idname>" +"\r\n";
	        			//}
	        			//xmlHygieneBuilder +="<hyg:ridfield>" + "spoonRecordID" + "</hyg:hyg:ridfield>" +"\r\n";
	        			
	        		}
	        		xmlHygieneBuilder +="<hyg:idname>" + "spoonClusterID" + "</hyg:idname>" +"\r\n";
	        		//jobMeta.getJob
	        		file_name = ap.getDatasetsField("record_name", datasets[i],jobMeta.getJobCopies());
	        		
	        		//todo: write layout_<file_name> to file needed for soap
	        		layoutECL = "//spoon generated code\r\n";
	        		layoutECL += "EXPORT layout_" + file_name + " := RECORD\r\n" + resultListToStringStatic(fields);
	        		layoutECL += "UNSIGNED6 spoonClusterID := 0;\r\n";
	        		layoutECL += "UNSIGNED6 spoonRecordID := 0;\r\n";
	        		layoutECL += "\r\nEND;\r\n\r\n";
	        		//xmlHygieneBuilder += buildHygieneRule(datasets[i], "SRC","");
	        		//xmlHygieneBuilder += "<hyg:sourcefield>" + fieldCSV + "</hyg:sourcefield>";
	        		xmlHygieneBuilder += buildOptReports(jobMeta,datasets[i]);
	        		
	        		xmlHygieneBuilder += buildConceptsSalt(jobMeta,datasets[i]);
	        		//dsECL += System.getProperties().getProperty("Dataset-" + datasets[i]);

	    	        
	    	        rsDef = System.getProperties().getProperty("Dataset-" + datasets[i]+"-rsDef");
	    	        dsDef = System.getProperties().getProperty("Dataset-" + datasets[i]+"-dsDef");
	    	        recordName = System.getProperties().getProperty("Dataset-" + datasets[i]+"-rs");
	    	        logicalFile = System.getProperties().getProperty("Dataset-" + datasets[i]+"-logical");
	    	        fileType = System.getProperties().getProperty("Dataset-" + datasets[i]+"-type");
	    	        dsName = System.getProperties().getProperty("Dataset-" + datasets[i]+"-ds");
	    	        
	        	}
	        	
	        	
	        	
	        				
	
				
	        	xmlHygieneBuilder = "<hyg:hygiene-spec xmlns:hyg=\"http://hpccsystems.org/salt/hygiene/bean\">" +"\r\n"+
									    "<hyg:module-name>" + jobNameNoSpace + "module</hyg:module-name>" +"\r\n"+
									    "<hyg:file-name>" + file_name + "</hyg:file-name>" +"\r\n"+
									    "<hyg:dataset-rsdef><![CDATA[" + rsDef + "]]></hyg:dataset-rsdef>" +"\r\n"+
									    "<hyg:dataset-dsdef><![CDATA[" + dsDef + "]]></hyg:dataset-dsdef>" +"\r\n"+
									    "<hyg:dataset-record-name><![CDATA[" + recordName + "]]></hyg:dataset-record-name>" +"\r\n"+
									    "<hyg:dataset-name><![CDATA[" + dsName + "]]></hyg:dataset-name>" +"\r\n"+
									    "<hyg:dataset-logical-file><![CDATA[" + logicalFile + "]]></hyg:dataset-logical-file>" +"\r\n"+
									    "<hyg:dataset-file-type><![CDATA[" + fileType + "]]></hyg:dataset-file-type>" +"\r\n"+
									    xmlHygieneBuilder +
									    "</hyg:hygiene-spec>";
	        	
	        	//xmlHygieneBuilder += buildConceptsSalt();
	        	//System.out.println("-------------------------------------SALT COMPILE--------------------------------");
	        	String path = fileName;
	        	String slash = "/";
                if(fileName.contains("/") && !fileName.contains("\\")){
                	if(fileName.lastIndexOf("/") != (fileName.length()-1)){
                		path += "/";
                		slash = "/";
                	}
                	//path += "salt.spc";
                }else{
                	if(fileName.lastIndexOf("\\") != (fileName.length()-1)){
                		path += "\\";
                		slash = "\\";
                	}
                	//path += "salt.spc";
                }
	        	try {              
	        		
	                
	                BufferedWriter out = new BufferedWriter(new FileWriter(path + "salt.xml"));
	                out.write(xmlHygieneBuilder);
	                out.close();
	                
	                //FileInputStream fis = new FileInputStream(
	               // 		this.fileName + "\\salt.xml");
	                //need to compare xml bevore writting it to see if need to re-compile salt
	        		Generate gen = new Generate();
	        		String spec = gen.generateHygieneSpecFromXMLFile(path + "salt.xml");
	        		BufferedWriter out2 = new BufferedWriter(new FileWriter(path + "salt.spc"));
	                out2.write(spec);
	                out2.close();
	                
	                String modFile = "";
	                //System.out.println("-------------------------------------SALT COMPILE2--------------------------------");
	                
	                boolean compileSuccess = ECLExecuteSalt.compileSalt(SALTPath, path + "salt.spc", fileName,jobNameNoSpace,error,fileName);
	                
	                if(!compileSuccess){
	                	 String SaltError = "Unable to create the SALT files! Please check your salt path in Global Variables, and your output path in Execute.";
	                	 result.setResult(false);
	                	 result.setStopped(true);
	                	 result.setLogText(SaltError);
	                 	 //logError(SaltError);
	                 	 System.out.println(SaltError);
	                 	 error += SaltError;
	                 	 return "";
	                }else{
	                	//logBasic("Salt Compiled");
	                	//System.out.println("Salt Compiled moving on");
	                }
	                
	                //fixEclFiles(this.fileName + "\\" + jobNameNoSpace + "module");
	                
	        		
	            } catch (IOException e) {
	                 e.printStackTrace();
	                 String SaltError = "Unable to create the SALT files! Please check your salt path in Global Variables, and your output path in Execute.";
                	 result.setResult(false);
                	 result.setStopped(true);
                	 result.setLogText(SaltError);
                 	 //logError(SaltError);
                 	 System.out.println(SaltError);
                 	 error += SaltError;
                 	 return "";
	            }   
	        	try {              

	                BufferedWriter out = new BufferedWriter(new FileWriter(path + jobNameNoSpace + "module" + slash + "layout_" + file_name + ".ecl"));
	                out.write(layoutECL);
	                out.close();
	            } catch (IOException e) {
	                 e.printStackTrace();
	            }   
	        }catch (Exception e){
	        	System.out.println("--------------FAILED---------------");
	        	System.out.println(e.toString());
	            e.printStackTrace();
	        }
        }
        
	    return saltIncludePath;  	
	}
	
	public static String buildConceptsSalt(JobMeta jobMeta, String datasetName){
		String xml = "";
		
		 List<JobEntryCopy> jobs = jobMeta.getJobCopies();
	        try{
		        SaltAutoPopulate sap = new SaltAutoPopulate();
		        HashMap<String,String> conceptMap = sap.getConcepts(jobs, datasetName);
		        if(conceptMap.get("entryList") != null){
		        	String entryList = conceptMap.get("entryList");
		        	
		        	//we now need to parse the datastructure out
		        	/*System.out.println("**************************************************");
		        	System.out.println("**************************************************");
		        	System.out.println("**************************************************");
		        	System.out.println(entryList);
		        	System.out.println("**************************************************");
		        	System.out.println("**************************************************");
		        	System.out.println("**************************************************");
		        	*/
		        	
		        	ConceptEntryBO conceptEntry = new ConceptEntryBO();
		        	xml = conceptEntry.fromCSVtoXML(entryList);
		        	System.out.println(xml);
		        }
	        }catch (Exception e){
	        	e.printStackTrace();
	        	
	        }    

		
		return xml;
	}
	/*
	public static String ConceptSaltXML(){
		String xml = ""+
		"<hyg:concept-def>\r\n"+
		"	<hyg:concept-name>testConcept</hyg:concept-name>\r\n"+
		"	<hyg:effectOnSpecificity>testConcept</hyg:effectOnSpecificity>\r\n"+
		"	<hyg:threshold>ccc</hyg:threshold>\r\n"+
		"	<hyg:useBagOfWords>true</hyg:useBagOfWords>\r\n"+
		"	<hyg:reOrderType>2</hyg:reOrderType>\r\n"+
		"	<hyg:segmentType>3</hyg:segmentType>\r\n"+
		"	<hyg:scale>4</hyg:scale>\r\n"+
		"	<hyg:specificity>5</hyg:specificity>\r\n"+
		"	<hyg:switchValue>6</hyg:switchValue>\r\n"+
		"	<hyg:concept-fields>\r\n"+
		"		<hyg:conceptFieldname>fieldName</hyg:conceptFieldname>\r\n"+
		"		<hyg:nonNull>true</hyg:nonNull>\r\n"+
		"	</hyg:concept-fields>\r\n"+
		"	<hyg:concept-fields>\r\n"+
		"		<hyg:conceptFieldname>fieldName2</hyg:conceptFieldname>\r\n"+
		"		<hyg:nonNull>false</hyg:nonNull>\r\n"+
		"	</hyg:concept-fields>\r\n"+
		"	<hyg:concept-fields>\r\n"+
		"		<hyg:conceptFieldname>fieldName3</hyg:conceptFieldname>\r\n"+
		"		<hyg:nonNull>true</hyg:nonNull>\r\n"+
		"	</hyg:concept-fields>\r\n"+
		"</hyg:concept-def>\r\n";
		
		return xml;
	}
	*/
	public static void main(String[] args){
		compileSalt("C:\\tools\\SALT", "C:\\Spoon Demos\\new\\saltdemos\\dataprofile_out\\salt.spc","C:\\Spoon Demos\\new\\saltdemos\\dataprofile_out","SpoonDataProfile","","C:\\Spoon Demos\\new\\saltdemos\\dataprofile_out");
	}
	
	  public static boolean compileSalt(String saltPath, String spcFile, String outputDir, String jobNameNoSpace, String error, String fileName){
		//debug
		System.out.println("Compiling SALT -- START");
		System.out.println("saltPath: " + saltPath);
		System.out.println("spcFile: " + spcFile);
		System.out.println("outputDir: " + outputDir);
		System.out.println("jobNameNoSpace: " + jobNameNoSpace);
		System.out.println("error: " +error);
		System.out.println("fileName: " + fileName);
		
		String saltExe = "salt.exe";
		if (!System.getProperty("os.name").startsWith("Windows")) {
        	saltExe = "salt";
        }
		String saltSlash = "\\";
		if(saltPath.contains("/") && !saltPath.contains("\\")){
			saltSlash = "/";
		}
		String fileNameSlash = "\\";
		if(fileName.contains("/") && !fileName.contains("\\")){
			fileNameSlash = "/";
		}
		String saltExePath = saltPath + saltSlash + saltExe;
		boolean saltExists = (new File(saltExePath)).exists();
		boolean outExists = (new File(outputDir).exists());
		boolean success = false;
		//debug
		System.out.println("Salt EXE Exists(" + saltExePath + "): " + saltExists);
		System.out.println("Output Dir Exists("+outputDir+"): " + outExists);
		if(saltExists && outExists){
			System.out.println("salt and out exists -- Starting salt compile");
			//String cmd = "\"" + saltPath + "\\" + saltExe + "\" -ga -D\"" + outputDir + "\" \"" + spcFile + "\"";
			//System.out.println("-->" + cmd + "<--");
		 	try{
		 		System.out.println("entered try");
		 		String c = saltExePath;
		 		ArrayList<String> paramsAL = new ArrayList<String>();
	            paramsAL.add(c);
	            paramsAL.add("-ga");
	            //paramsAL.add("-D");
	            paramsAL.add("-D"+outputDir);
	            paramsAL.add(spcFile);
	            String [] params = new String[paramsAL.size()];
	            paramsAL.toArray(params);
	            System.out.println("build process builder");
		 		ProcessBuilder pb = new ProcessBuilder(params);
		 		
		 		System.out.println("PB Command: " + pb.command().toString());
	            pb.redirectErrorStream(true); // merge stdout, stderr of process
	            //File path = new File(saltPath);
	           // pb.directory(path);
	            Process p = pb.start();
	          //  System.out.println(pb.command());
	            
	            InputStream iError = p.getErrorStream();
	            InputStreamReader isrError = new InputStreamReader(iError);
	            BufferedReader brErr = new BufferedReader(isrError);
	            String lineErr = "";
	            String fullLineErr = "";
	            while((lineErr = brErr.readLine()) != null){
	                System.out.println("#####"+lineErr);
	                fullLineErr += lineErr;
	            }
	            
	            InputStream is = p.getInputStream();
	            InputStreamReader isr = new InputStreamReader(is);
	            BufferedReader br = new BufferedReader(isr);
	            String line = "";
	            String fullLine = "";
	            while((line = br.readLine()) != null){
	                System.out.println(line);
	                fullLine += line;
	            }
	            
	            System.out.println("SALT ERROR:");
				System.out.println(fullLineErr);
				System.out.println("SALT Results");
				System.out.println(fullLine);

				//block until salt compile is completed
	                int b = 0;
	                while(!(new File(fileName + fileNameSlash + jobNameNoSpace + "module")).exists() && b < 150){
	                	Thread.sleep(1000);
	                	b++;
	                }
	                System.out.println("Finished salt compile");
					if((new File(fileName + fileNameSlash + jobNameNoSpace + "module")).exists()){
						success = true;
					}
				 System.out.println("SALT Success Status: " + success);
				 
                    
		 	}catch (Exception e){
	            System.out.println(e.toString());
	            e.printStackTrace();
	            error += "Couldn't Locate the Salt Install and/or the output Directory Please Verify settings!";
	            return false;
	        }
		}else{
			//salt install doesn't exist
			if(!saltExists){error += "Salt not found in provided path, please check Global Variables! \r\n";}
			if(!outExists){error += "Output Directory defined in Execute step doesn't exist";}
			success = false;
		}
		System.out.println("Compiling SALT -- END");
		return success;
	}
	/*
	public static boolean compileSalt(String saltPath, String spcFile, String outputDir, String jobNameNoSpace, String error, String fileName){
		boolean saltExists = (new File(saltPath + "\\salt.exe")).exists();
		boolean outExists = (new File(outputDir).exists());
		boolean success = false;
		
		System.out.println(saltPath);
		System.out.println(spcFile);
		System.out.println(outputDir);
		System.out.println(jobNameNoSpace);
		System.out.println(error);
		System.out.println(fileName);
		
		if(saltExists && outExists){
			String cmd = "\"" + saltPath + "\\salt.exe\" -ga -D\"" + outputDir + "\" \"" + spcFile + "\"";
			//System.out.println("-->" + cmd + "<--");
		 	try{
				 System.out.println("runtime");
				 File path = new File(saltPath);
				 Runtime rt = java.lang.Runtime.getRuntime();
				 Process p = rt.exec(cmd, null, path);
				 System.out.println("completed salt exe");
				//block until salt compile is completed
	                int b = 0;
	                while(!(new File(fileName + "\\" + jobNameNoSpace + "module")).exists() && b < 150){
	                	Thread.sleep(1000);
	                	b++;
	                }
	                
				// if(p.exitValue() == 0){
					 if((new File(fileName + "\\" + jobNameNoSpace + "module")).exists()){
						 success = true;
					 }
				// }
				 
				 
                    
		 	}catch (Exception e){
	            System.out.println(e.toString());
	            e.printStackTrace();
	            error += "Couldn't Locate the Salt Install and/or the output Directory Please Verify settings!";
	            return false;
	        }
		}else{
			//salt install doesn't exist
			if(!saltExists){error += "Salt not found in provided path, please check Global Variables! \r\n";}
			if(!outExists){error += "Output Directory defined in Execute step doesn't exist";}
			success = false;
		}
		
		return success;
	}
	*/
	public static String buildHygieneRule(JobMeta jobMeta, String datasetName, String columnName, String columnType){
		String xml = "";
       // JobMeta jobMeta = super.parentJob.getJobMeta();
        List<JobEntryCopy> jobs = jobMeta.getJobCopies();
        
		xml += "<hyg:field-rule>"+"\r\n";
		xml += "    <hyg:field-name>" + columnName + "</hyg:field-name>"+"\r\n";
		xml += "    <hyg:field-type>" + columnType + "</hyg:field-type>"+"\r\n";
		
		//see if hygine rule exists for field
		//if rule exists build it
		try{
	        SaltAutoPopulate sap = new SaltAutoPopulate();
	        String[] rules = sap.getRule(jobs,datasetName, columnName);
	        HygieneEntryList hel = sap.getEntryList();
	        ArrayList<HygieneEntryBO> entries = hel.getEntries();
	        
	        for(int i =0; i<entries.size(); i++){
	        	HygieneEntryBO entry = entries.get(i);
	        	if(columnName.equalsIgnoreCase(entry.getField())){
	        		System.out.println("Field with Rule: " + entry.getField());
	        		//we have a column match and it has a rule 
	        		//look up rule
	        		HygieneRuleBO rule = sap.fieldTypeList.get(sap.fieldTypeList.getIndex(entry.getRuleName()));
	        		//we have the rule time to build the xml
	        		if(rule.isCaps()){
	        			xml += "    <hyg:caps>true</hyg:caps>"+"\r\n";
	        		}
	        		if(rule.isLefttrim()){
	        			xml += "    <hyg:left-trim>true</hyg:left-trim>"+"\r\n";
	        		}
	        		if(!rule.getAllow().equals("")){
	        			xml += "    <hyg:allow>"+rule.getAllow()+"</hyg:allow>"+"\r\n";
	        		}
	        		if(!rule.getSpaces().equals("")){
	        			xml += "    <hyg:spaces>"+rule.getSpaces()+"</hyg:spaces>"+"\r\n";
	        		}
	        		if(!rule.getOnfail().equals("")){
	        			xml += "    <hyg:on-fail>"+rule.getOnfail()+"</hyg:on-fail>"+"\r\n";
	        		}
	        		if(!rule.getIgnore().equals("")){
	        			//not implemented
	        			xml += "    <hyg:ignore>"+rule.getIgnore()+"</hyg:ignore>"+"\r\n";
	        		}
	        		if(!rule.getLengths().equals("")){
	        			//not implemented
	        			xml += "    <hyg:lengths>"+rule.getLengths()+"</hyg:lengths>"+"\r\n";
	        		}
	        		if(!rule.getDisallowedQuotes().equals("")){
	        			//not implemented
	        			xml += "    <hyg:disallowed-quotes>"+rule.getDisallowedQuotes()+"</hyg:disallowed-quotes>"+"\r\n";
	        		}
	        		//if(!rule.getLike().equals("")){
	        			//not implemented
	        		//	xml += "    <hyg:like>"+rule.getLike()+"</hyg:like>"+"\r\n";
	        		//}
	        		if(!rule.getWords().equals("")){
	        			//not implemented
	        			xml += "    <hyg:words>"+rule.getWords()+"</hyg:words>"+"\r\n";
	        		}
	        		
	        		
	        	}
	        }
        
        }catch (Exception e){
        	System.out.println("~~~~~~~~~~~~~~~~~~~~ERROR~~~~~~~~~~~~~~~~~~~~~~~~~~");
        	System.out.println (e.toString());

        }
		//Close off the XML Tag
		xml += "</hyg:field-rule>"+"\r\n";
		
		return xml;
	}
	
	public static String buildOptReports(JobMeta jobMeta, String datasetName){
		String xml = "";
		//JobMeta jobMeta = super.parentJob.getJobMeta();
        List<JobEntryCopy> jobs = jobMeta.getJobCopies();
        try{
	        SaltAutoPopulate sap = new SaltAutoPopulate();
	        HashMap hm = sap.getHygine(jobs, datasetName);
	        if(hm.get("srcField") != null && !hm.get("srcField").equals("null")){
	        	xml += "<hyg:sourcefield>" + hm.get("srcField") + "</hyg:sourcefield>\r\n";
	        }
        }catch (Exception e){
        	e.printStackTrace();
        	
        }    
		return xml;
	}
}
