/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecljobentry;

import java.util.ArrayList;
import java.util.List;
//import org.hpccsystems.ecldirect.Output;
import org.hpccsystems.ecldirect.EclDirect;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.ecldirect.Column;
import java.io.*;
import org.hpccsystems.ecldirect.ECLSoap;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.core.*;
import org.pentaho.di.core.gui.SpoonFactory;


import org.hpccsystems.eclguifeatures.*;
import org.pentaho.di.job.JobMeta;

/**
 *
 * @author ChalaAX
 */
public class ECLJobEntry extends JobEntryBase implements Cloneable, JobEntryInterface {
    
	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
		return null;
	}

	

	@Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
            super.loadXML(node, list, list1);
    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
      

        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {

    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {

    }
    
    
    
    
   
}
