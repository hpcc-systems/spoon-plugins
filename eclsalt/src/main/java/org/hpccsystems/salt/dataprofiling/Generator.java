package org.hpccsystems.salt.dataprofiling;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import com.hpccsystems.salt.jaxb.FieldDef;
import com.hpccsystems.salt.jaxb.SaltSpecification;
import com.hpccsystems.salt.jaxb.SaltSpecification.Fields;

public class Generator {

	private SaltSpecification spec = null;

	public Generator(InputStream xml) throws Exception {
		JAXBContext jc = JAXBContext.newInstance(SaltSpecification.class);
		Unmarshaller jaxbUnmarshaller = jc.createUnmarshaller();
		spec = (SaltSpecification) jaxbUnmarshaller.unmarshal(xml);
	}

	public String toSALT() {
		StringBuilderEx sb = new StringBuilderEx();
		sb.appendLine("MODULE:" + spec.getModuleName());
		sb.appendLine("FILENAME:" + spec.getFileName());

		Fields fields = spec.getFields();
	
		if (fields != null) {
			
			processFields(sb, fields);

		}

		return sb.toString();
	}

	private void processFields(StringBuilderEx sb, Fields fields) {
		List<FieldDef> fieldList = fields.getFielddef();
		for (FieldDef fld : fieldList) {
			sb.appendLine("FIELD:" + fld.getName() + ":TYPE("
					+ fld.getDatatype() + "):0,0");
		}
	}

	public static void main(String args[]) throws Exception {

		FileInputStream fis = new FileInputStream(
				"C:/Documents and Settings/ChambeJX.RISK/My Documents/spoon-plugins/spoon-plugins/eclsalt/src/main/xsd/SALT-Hygiene.xml");

		Generator gen = new Generator(fis);

		System.out.println(gen.toSALT());

	}

}
