/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.ecldirect;

/**
 *
 * @author ChalaAX
 */
public class Iterate implements EclCommand {

    private String name;
    private String record;
    private String recordName;
    private String transform;
    private String transformCall;
    private String transformName;
    private String recordsetName;
    private String recordset;
    private Boolean runLocal;
    private String returnType;

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }
    
    

    public String getTransformCall() {
        return transformCall;
    }

    public void setTransformCall(String transformCall) {
        this.transformCall = transformCall;
    }

    
    public String getRecordName() {
        return recordName;
    }

    public void setRecordName(String recordName) {
        this.recordName = recordName;
    }

    public String getRecordsetName() {
        return recordsetName;
    }

    public void setRecordsetName(String recordsetName) {
        this.recordsetName = recordsetName;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    public Boolean getRunLocal() {
        return runLocal;
    }

    public void setRunLocal(Boolean runLocal) {
        this.runLocal = runLocal;
    }

    public String getTransformName() {
        return transformName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
    }



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRecordset() {
        return recordset;
    }

    public void setRecordset(String recordset) {
        this.recordset = recordset;
    }

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    

    @Override
    public String ecl() {
        //ITERATE(recordset, transform [, LOCAL ] )
        
         
        String ecl = "";
        if(record != null && !record.equals("") && !record.equals("null")){
            ecl += recordName + " := RECORD\r\n" + record + "\r\nEND; \r\n\r\n";
        }
        if(recordset != null && !recordset.equals("") && !recordset.equals("null")){
            
            ecl += recordsetName + " := DATASET(["+recordset+"],"+recordName+");\r\n";
        }
        //ecl += returnType + " " + transformName + "(" + returnType + " L, " + returnType + " R) := TRANSFORM \r\n" + transform + "\r\nEND;\r\n\r\n";
      ecl += transformName + " := TRANSFORM \r\n" + transform + "\r\nEND;\r\n\r\n";
     
        ecl += name + " := ITERATE(" + recordsetName + "," + transformCall;
        //add local if its set if not its optional
        if (runLocal != null && runLocal) {
           ecl += ", local";
        }
        //close out the ecl call
        ecl += ");\r\n\r\n";
        
        return ecl;
    }

    @Override
    public CheckResult check() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
