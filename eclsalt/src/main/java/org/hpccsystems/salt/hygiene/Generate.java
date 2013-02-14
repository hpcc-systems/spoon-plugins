package org.hpccsystems.salt.hygiene;

import org.apache.xmlbeans.XmlException;



import org.hpccsystems.salt.hygiene.bean.FieldHygieneRule;
import org.hpccsystems.salt.hygiene.bean.HygieneSpecDocument;

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
            if(spec.getIdname() != null){
            	specStr.appendLine("IDNAME:" + spec.getIdname());
            }
            

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

                if (likesLine.length() > 0) {
                    fields.appendLine("FIELD:" + hrule.getFieldName() + ":LIKE(LIKE_"
                            + hrule.getFieldName() + "):0,0");
                } else {
                    fields.appendLine("FIELD:" + hrule.getFieldName() + ":TYPE("
                            + hrule.getFieldType() + "):0,0");
                }


            }

            specStr.appendLine(likes.toString());
            specStr.appendLine(fields.toString());

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
        String fileContent = readFileAsString("C:/Documents and Settings/ChambeJX.RISK/My Documents/spoon-plugins/spoon-plugins/eclsalt/src/main/xsd/SALT-Hygiene.xml");
        Generate gen = new Generate();
        System.out.println(gen.generateHygieneSpec(fileContent));

        HygieneSpecDocument doc = HygieneSpecDocument.Factory.newInstance();
        HygieneSpecDocument.HygieneSpec spec = doc.addNewHygieneSpec();
        spec.setModuleName("HelloWorld");
        spec.setFileName("HelloWorld_File");
       // spec.setIdfield("HelloWorld_File_ID");
       spec.setIdname("HellowWorldIDName");
        FieldHygieneRule rule = spec.addNewFieldRule();
        rule.setFieldName("First");
        rule.setLeftTrim(true);

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
