
package org.hpccsystems.pentaho.job.ecladdcount;

import java.util.List;
import org.hpccsystems.ecljobentrybase.ECLJobEntry;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

public class ECLAddCount
    extends ECLJobEntry
{

    private String inDS = ("");
    private String OutDS = ("");
    public int margin = Const.MARGIN;

    public String getinDS() {
        return (inDS);
        

    }

    public void setinDS(String inds) {
        this.inDS = inds;
    }

    public String getOutDS() {
        return (OutDS);
        

    }

    public void setOutDS(String outds) {
        this.OutDS = outds;
    }

    @Override
    public Result execute(Result prevResult, int k)
        throws KettleException
    {
        Result result = (prevResult);
        if(result.isStopped()){
        	return result;
        }
        else{
        String ecl = "";
        ecl += "Rec_my := RECORD\n";
        ecl += "INTEGER uid;\n";
        ecl += ""+inDS+";\n";
        ecl += "END;\n";
        ecl += "\n";
        ecl += "Rec_my Trans("+inDS+" L, INTEGER C) := TRANSFORM\n";
        ecl += "SELF.uid := C;\n";
        ecl += "SELF := L;\n";
        ecl += "END;\n";
        ecl += "\n";
        ecl += ""+OutDS+" := PROJECT("+inDS+",Trans(LEFT,COUNTER));\n";
        ecl += "OUTPUT("+OutDS+");\n";
        result.setResult(true);
        RowMetaAndData data = (new RowMetaAndData());
        (data).addValue("ecl", Value.VALUE_TYPE_STRING, (ecl));
        

        List list = (result.getRows());
        list.add(data);
        String eclCode = parseEclFromRowData(list);
        result.setRows(list);
        result.setLogText("ECLRandom executed, ECL code added");
        return (result);
        }
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository repository)
        throws KettleXMLException
    {
        super.loadXML(node, list, list1);
        try{
        if (XMLHandler.getNodeValue((XMLHandler.getSubNode(node, "inDS")))!= (null)) {
            setinDS(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "inDS")));
        }
        if (XMLHandler.getNodeValue((XMLHandler.getSubNode(node, "OutDS")))!= (null)) {
            setOutDS(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "OutDS")));
        }
        }
        catch (Exception e) {
        	throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }
    }

    public String getXML() {
        String retval = "";
        retval += super.getXML();
        retval += "		<inDS><![CDATA[" + inDS + "]]></inDS>" + Const.CR;
        retval += "		<OutDS><![CDATA[" + OutDS + "]]></OutDS>" + Const.CR;
        return retval;
    }

    public void loadRep(Repository rep, ObjectId id_jobEntry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
        throws KettleException
    {
        try{
        	if(rep.getStepAttributeString(id_jobEntry, "inDS") != null)
        		inDS = rep.getStepAttributeString(id_jobEntry, "inDS"); //$NON-NLS-1$
        	if(rep.getStepAttributeString(id_jobEntry, "OutDS") != null)
        		OutDS = rep.getStepAttributeString(id_jobEntry, "OutDS"); //$NON-NLS-1$
        }
        catch (Exception e) {
        	throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job)
        throws KettleException
    {
        try{
        	rep.saveStepAttribute(id_job, getObjectId(), "inDS", inDS); //$NON-NLS-1$
        	rep.saveStepAttribute(id_job, getObjectId(), "OutDS", OutDS); //$NON-NLS-1$
        }
        catch (Exception e) {
        	throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    public boolean evaluates() {
        return true;
    }

    public boolean isUnconditional() {
        return true;
    }

}
