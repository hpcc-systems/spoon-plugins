package org.hpccsystems.salt.hygiene;

import org.apache.xmlbeans.XmlException;



import org.hpccsystems.salt.hygiene.bean.ConceptFields;
import org.hpccsystems.salt.hygiene.bean.FieldHygieneRule;
import org.hpccsystems.salt.hygiene.bean.HygieneSpecDocument;

import com.hpccsystems.salt.jaxb.ConceptDef;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * User: Arjuna Chala
 * Date: 10/15/12
 * Time: 4:58 PM
 */
public class Generate {

    public String generateHygieneSpec(String in) throws XmlException {
        HygieneSpecDocument doc = HygieneSpecDocument.Factory.parse(in);
        HygieneSpecDocument.HygieneSpec spec = doc.getHygieneSpec();
        if (null != spec) {
        	
            StringBuilderEx specStr = new StringBuilderEx();
            StringBuilderEx likes = new StringBuilderEx();
            StringBuilderEx fields = new StringBuilderEx();

            specStr.appendLine("MODULE:" + spec.getModuleName());
            specStr.appendLine("FILENAME:" + spec.getFileName());
           // specStr.appendLine("IDFIELD:" + spec.getIdfield());
            System.out.println(spec.getIdfieldExists());
            if(spec.getIdfieldExists() != null && spec.getIdfieldExists().equalsIgnoreCase("true")){
            	System.out.println("if");
            	 if(spec.getIdname() != null){
 	            	specStr.appendLine("IDFIELD:EXISTS:" + spec.getIdname());
 	            }
            }else{
            	System.out.println("else");
	            if(spec.getIdname() != null){
	            	specStr.appendLine("IDNAME:" + spec.getIdname());
	            	specStr.appendLine("IDFIELD:" + spec.getIdname());
	            }
            }
            //todo:: make not hardcoded and ADD EXISTS
            //not currently needed for specification
           
           if(spec.getRidfield() != null && !spec.getRidfield().equals("")){
            	specStr.appendLine("RIDFIELD:" + spec.getRidfield());
            }
            specStr.appendLine("RECORDS:20000");
            specStr.appendLine("POPULATION:10000");
            specStr.appendLine("NINES:3");

            FieldHygieneRule rules[] = spec.getFieldRuleArray();
            for (int i = 0; i < rules.length; i++) {
                FieldHygieneRule hrule = rules[i];

                StringBuilderEx likesLine = new StringBuilderEx();

                if (hrule.getLeftTrim()) {
                    likesLine.append(":LEFTTRIM");
                }
                if (hrule.getRightTrim()) {
                    likesLine.append(":RIGHTTRIM");
                }
                if (hrule.getCaps()) {
                    likesLine.append(":CAPS");
                }
                if (hrule.getAllow() != null && hrule.getAllow().length() > 0) {
                    likesLine.append(":ALLOW(" + hrule.getAllow() + ")");
                }
                if (hrule.getOnFail() != null && hrule.getOnFail().length() > 0) {
                    likesLine.append(":ONFAIL(" + hrule.getOnFail() + ")");
                }
                if (likesLine.length() > 0) {
                    likesLine.insert(0, "FIELDTYPE:LIKE_" + hrule.getFieldName());
                    likes.appendLine(likesLine.toString());
                }

                if(hrule.getFieldSpecificity() == null){
                	hrule.setFieldSpecificity("0");
                }
                if (likesLine.length() > 0) {
                    fields.appendLine("FIELD:" + hrule.getFieldName() + ":LIKE(LIKE_"
                            + hrule.getFieldName() + "):" + hrule.getFieldSpecificity() + ",0");
                } else if (!hrule.getFieldType().equals("")){
                    fields.appendLine("FIELD:" + hrule.getFieldName() + ":TYPE("
                            + hrule.getFieldType() + "):" + hrule.getFieldSpecificity() + ",0");
                }else{
                	fields.appendLine("FIELD:" + hrule.getFieldName() + ":0,0");
                }


            }
            
            specStr.appendLine(likes.toString());
            specStr.appendLine(fields.toString());
            org.hpccsystems.salt.hygiene.bean.ConceptDef[] concepts = spec.getConceptDefArray();
            for (int i = 0; i < concepts.length; i++) {
            	String conceptStr = "";
            	
            	conceptStr += "CONCEPT:" + concepts[i].getConceptName();
            	
            	ConceptFields[] cFields = concepts[i].getConceptFieldsArray();
            	for(int j =0; j < cFields.length; j++){
            		conceptStr += ":" + cFields[j].getConceptFieldname();
            		if(cFields[j].getNonNull() != null && cFields[j].getNonNull().equalsIgnoreCase("true")){
            			System.out.println("test: " + cFields[j].getNonNull());
            			conceptStr += "+";
            		}
            	}
            	//optional field
            	//if(!segmentType.equals("")){
            	if(concepts[i].getSegmentType() != null && !concepts[i].getSegmentType().equals("")){
            		conceptStr += ":SEGTYPE(" + concepts[i].getSegmentType() + ")";
            	}
            	//optional field
            	if(concepts[i].getScale() != null && !concepts[i].getScale().equals("")){
            		conceptStr += ":SCALE(" + concepts[i].getScale() + ")";
            	}
            	//optional field
            	if(concepts[i].getUseBagOfWords() != null && concepts[i].getUseBagOfWords().equalsIgnoreCase("true")){
            		conceptStr += ":BAGOFWORDS";
            	}
            	//required field set to 0 if ""
            	if(concepts[i].getSpecificity() != null){
            		conceptStr += ":" + concepts[i].getSpecificity();
            		
            	}else{
            		conceptStr += ":0";
            	}
            	//required field set to 0 if ""
            	if(concepts[i].getSwitchValue() != null){
            		conceptStr += "," + concepts[i].getSwitchValue();
            		
            	}else{
            		conceptStr += ",0";
            	}
            	specStr.appendLine(conceptStr);
            }
           
            
            
            
            if(spec.getSourcefield()!= null && !spec.getSourcefield().equalsIgnoreCase("null")){
            	specStr.appendLine("SOURCEFIELD:" + spec.getSourcefield() + ":CONSISTENT");
            }

            return specStr.toString();
        } else {
            return null;
        }
    }
    
    public String generateHygieneSpecFromXMLFile(String filename){
    	String spec = "";
    	try{
    		String fileContent = readFileAsString(filename);
    		spec = generateHygieneSpec(fileContent);
    	}catch (Exception e){
    		System.out.println("failed to open file");
    	}
    	return spec;
        
    }

    public static void main(String[] args) throws Exception {
        //String fileContent = readFileAsString("../../../../../xsd/SALT-Hygiene.xml");
        String src = "C:/Documents and Settings/ChambeJX.RISK/My Documents/spoon-plugins/spoon-plugins/eclsalt/src/main/xsd/concept_test.xml";
        
       // src = "C:/Spoon Demos/new/salt/out_hygine/salt.xml";
    	try{
    		String fileContent = readFileAsString(src);
    		//System.out.println(fileContent);
    		Generate gen = new Generate();
    		System.out.println(gen.generateHygieneSpec(fileContent));
    	}catch (Exception e){
    		e.printStackTrace();
    	}
        HygieneSpecDocument doc = HygieneSpecDocument.Factory.newInstance();
        HygieneSpecDocument.HygieneSpec spec = doc.addNewHygieneSpec();
        spec.setModuleName("HelloWorld");
        spec.setFileName("HelloWorld_File");
        // spec.setIdfield("HelloWorld_File_ID");
        spec.setIdname("HellowWorldIDName");
        spec.setSourcefield("test");
        FieldHygieneRule rule = spec.addNewFieldRule();
        rule.setFieldName("First");
        rule.setLeftTrim(true);
        
        //ConceptDef cd = spec.add //new ConceptDef();

        System.out.println(doc.toString());
    }

    public static String readFileAsString(String filePath)
            throws java.io.IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(
                new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }
  
    
}
