/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecldedup;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author ChambersJ
 */
public class ECLDedupDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLDedup jobEntry;
    
    private Text jobEntryName;

    private Text recordsetName;
    private Combo recordset;
    private Text condition;
    private Combo runLocal;//optional
    

    private Combo isAll;
    private Combo isHash;
    private Text keep;
    private Text keeper;
    
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public ECLDedupDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLDedup) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Dedup");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };
        
        String datasets[] = null;
        AutoPopulate ap = new AutoPopulate();
        try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
            
            datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;


        shell.setLayout(formLayout);
        shell.setText("Dedup");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Dedup");

        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;

        // Stepname line
        Group generalGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(generalGroup);
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        FormData generalGroupFormat = new FormData();
        generalGroupFormat.top = new FormAttachment(0, margin);
        generalGroupFormat.width = 400;
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);

        //All other contols
        //Distribute Declaration
        Group iterateGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(iterateGroup);
        iterateGroup.setText("Dedup Details");
        iterateGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 225;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        iterateGroup.setLayoutData(datasetGroupFormat);

        //name = buildText("Distribute Name", null, lsMod, middle, margin, distributeGroup);


                    

        recordsetName = buildText("Result Recordset", null, lsMod, middle, margin, iterateGroup);
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, iterateGroup,datasets);
        condition = buildText("Condition", recordset, lsMod, middle, margin, iterateGroup);
        
    /*private Text isAll;
    private Text isHash;
    private Text keep;
    private Text keeper;
     */   
        isAll = buildCombo("All", condition, lsMod, middle, margin, iterateGroup,new String[]{"false", "true"});
        isHash = buildCombo("Hash", isAll, lsMod, middle, margin, iterateGroup,new String[]{"false", "true"});
        keep = buildText("Keep", isHash, lsMod, middle, margin, iterateGroup);
        keeper = buildText("Keeper", keep, lsMod, middle, margin, iterateGroup);
       
        
        runLocal = buildCombo("RUNLOCAL", keeper, lsMod, middle, margin, iterateGroup,new String[]{"false", "true"});
     
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, iterateGroup);

        // Add listeners
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, cancelListener);
        wOK.addListener(SWT.Selection, okListener);

        lsDef = new SelectionAdapter() {

            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };


        // Detect X or ALT-F4 or something that kills this window...

        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });
        


        //if (jobEntry.getJobName() != null) {
        //    jobEntryName.setText(jobEntry.getJobName());
        //}
        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
        
      /* private Text recordset;
    private Text condition;
    private Text transform;
    private Text fieldlist;
    private Text group;
    private Text runLocal;*/

        if (jobEntry.getRecordsetName() != null) {
            recordsetName.setText(jobEntry.getRecordsetName());
        }
        
        if (jobEntry.getRecordset() != null) {
            recordset.setText(jobEntry.getRecordset());
        }
        
        if (jobEntry.getCondition() != null) {
            condition.setText(jobEntry.getCondition());
        }
        
            /*private Text isAll;
    private Text isHash;
    private Text keep;
    private Text keeper;
     */
         if (jobEntry.getIsAllString() != null) {
            isAll.setText(jobEntry.getIsAllString());
        }
          if (jobEntry.getIsHashString() != null) {
            isHash.setText(jobEntry.getIsHashString());
        }
          

        
        if (jobEntry.getKeep() != null) {
            keep.setText(jobEntry.getKeep());
        }
       
        if (jobEntry.getKeeper() != null) {
            keeper.setText(jobEntry.getKeeper());
        }
        if (jobEntry.getRunLocalString() != null) {
            runLocal.setText(jobEntry.getRunLocalString());
        }
         


        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

    }

    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	if(this.recordsetName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Result Recordset\"!\r\n";
    	}
    	
    	//if all,keep,keeper than require condition
    	
    	if(this.isAll.getText().equals("true") || !this.keeper.getText().equals("") || !this.keep.getText().equals("")){
    		if(this.condition.getText().equals("")){
    			isValid = false;
        		errors += "You must provide a \"Condition\" if one of the following is true:\r\n" +
        				"\t-All is True\r\n" +
        				"\t-Keep has a value\r\n" +
        				"\t-keeper has a value\r\n";
    		}
    	}
    	
    	//if hash require all
    	if(this.isHash.getText().equals("true")){
    		if(!this.isAll.getText().equals("true")){
    			isValid = false;
        		errors += "\"All\" must be true if \"Hash\" is true!\r\n";
    		}
    	}
    	
    	if(!isValid){
    		ErrorNotices en = new ErrorNotices();
    		errors += "\r\n";
    		errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
    		isValid = en.openValidateDialog(getParent(),errors);
    	}
    	return isValid;
    	
    }

    private void ok() {
    	if(!validate()){
    		return;
    	}
        //jobEntry.setJobName(jobEntryName.getText());
        jobEntry.setName(jobEntryName.getText());
                                            /*
                private String recordset;
                private String condition;
                private String transform;
                private String fieldlist;
                private String GROUP;
                private Boolean runLocal;
             */
        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRecordset(recordset.getText());
        jobEntry.setCondition(condition.getText());
        
                    /*private Text isAll;
    private Text isHash;
    private Text keep;
    private Text keeper;
     */
        
        jobEntry.setIsAllString(isAll.getText());
        jobEntry.setIsHashString(isHash.getText());
        jobEntry.setKeep(keep.getText());
        jobEntry.setKeeper(keeper.getText());
        jobEntry.setRunLocalString(runLocal.getText());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }
}
