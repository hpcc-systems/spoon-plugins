/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltspecification;

import java.util.Iterator;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
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
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Table;
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

import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.recordlayout.CreateTable;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.ecljobentrybase.*;
/**
 *
 * @author ChambersJ
 */
public class SALTSpecificationDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private SALTSpecification jobEntry;
    private Text jobEntryName;
    private Text recordName;
   // private Text recordDef;
    private Text recordSet;
    private Text fileName;
    private Text datasetName;
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
    private Combo fileType;
    
    private Table table;
    private TableViewer tableViewer;

    // Create a RecordList and assign it to an instance variable
    private RecordList recordList = new RecordList();
    private CreateTable ct = null;

    public SALTSpecificationDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (SALTSpecification) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Dataset");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        ct = new CreateTable(shell);
        
        TabFolder tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        FormData data = new FormData();
        
        data.height = 500;
        data.width = 650;
        tabFolder.setLayoutData(data);
        
        Composite compForGrp = new Composite(tabFolder, SWT.NONE);
        //compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
        compForGrp.setBackground(new Color(tabFolder.getDisplay(),255,255,255));
        compForGrp.setLayout(new FormLayout());
        
        TabItem item1 = new TabItem(tabFolder, SWT.NULL);
        
        item1.setText ("General");
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Dataset");

        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;

        // Stepname line
        Group generalGroup = new Group(compForGrp, SWT.SHADOW_NONE);
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
        //Dataset Declaration
        Group datasetGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(datasetGroup);
        datasetGroup.setText("Dataset Details");
        datasetGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 220;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        datasetGroup.setLayoutData(datasetGroupFormat);

        datasetName = buildText("Dataset Name", null, lsMod, middle, margin, datasetGroup);
        fileName = buildText("Logical File Name", datasetName, lsMod, middle, margin, datasetGroup);
        fileType = buildCombo("File Type", fileName, lsMod, middle, margin, datasetGroup,new String[]{"", "CSV", "THOR"});

        recordSet = buildMultiText("Manual Record Entry", fileType, lsMod, middle, margin, datasetGroup);

        //Record Declaration
        Group recordGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(recordGroup);
        recordGroup.setText("Define Dataset Record");
        recordGroup.setLayout(groupLayout);
        FormData recordGroupFormat = new FormData();
        recordGroupFormat.top = new FormAttachment(datasetGroup, margin);
        recordGroupFormat.height = 45;
        recordGroupFormat.width = 400;
        recordGroupFormat.left = new FormAttachment(middle, 0);
        recordGroup.setLayoutData(recordGroupFormat);

        recordName = buildText("Record Name", null, lsMod, middle, margin, recordGroup);
        
       // recordDef = buildMultiText("Record Definition", recordName, lsMod, middle, margin, recordGroup);
        
        
           item1.setControl(compForGrp);
        
        
       
        
         if(jobEntry.getRecordList() != null){
            recordList = jobEntry.getRecordList();
            ct.setRecordList(jobEntry.getRecordList());
            
            if(recordList.getRecords() != null && recordList.getRecords().size() > 0) {
                    System.out.println("Size: "+recordList.getRecords().size());
                    for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO obj = (RecordBO) iterator.next();
                    }
            }
        }
        TabItem item2 = ct.buildDefTab("Fields", tabFolder);
        ct.redrawTable(true);
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tabFolder);

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

        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
        if (jobEntry.getLogicalFileName() != null) {
            fileName.setText(jobEntry.getLogicalFileName());
        }
        if (jobEntry.getDatasetName() != null) {
            datasetName.setText(jobEntry.getDatasetName());
        }
        if (jobEntry.getRecordName() != null) {
            recordName.setText(jobEntry.getRecordName());
        }
        //if (jobEntry.getRecordDef() != null) {
        //    recordDef.setText(jobEntry.getRecordDef());
        //}
        //
        if (jobEntry.getRecordSet() != null) {
            recordSet.setText(jobEntry.getRecordSet());
        }
        if (jobEntry.getFileType() != null) {
            fileType.setText(jobEntry.getFileType());
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
    		isValid = false;
    		errors += "\"Job Entry Name\" is a required field!\r\n";
    	}
    	
    	if(this.datasetName.getText().equals("")){
    		isValid = false;
    		errors += "\"Dataset Name\" is a required field!\r\n";
    	}
    	String fields = jobEntry.resultListToString(ct.getRecordList());
    	if(!this.recordSet.getText().equals("")){
    		//manual entry
    		
    		//we can't have fields or logical file name or File Type
    		if(!this.fileName.getText().equals("")){
    			isValid = false;
        		errors += "\"Logical File Name\" is not allowed for manual entry!\r\n";
    		}
    		if(!this.fileType.getText().equals("")){
    			isValid = false;
        		errors += "\"File Type\" is not allowed for manual entry!\r\n";
    		}
    		if(!fields.equals("")){
    			isValid = false;
        		errors += "Values on the \"Fields Tab\" is not allowed for manual entry!\r\n";
    		}
    	}else{
    		//using fields
    		if(this.fileName.getText().equals("")){
    			isValid = false;
        		errors += "\"Logical File Name\" is a required field!\r\n";
    		}
    		if(this.fileType.getText().equals("")){
    			isValid = false;
        		errors += "\"File Type\" is a required field!\r\n";
    		}
    		if(this.recordName.getText().equals("")){
    			isValid = false;
        		errors += "\"Record Name\" is a required field!\r\n";
    		}
    		if(fields.equals("")){
    			isValid = false;
        		errors += "Values on the \"Fields Tab\" is a required!\r\n";
    		}else{
    			//we need to validate the fields
    			String e = jobEntry.fieldsValid(ct.getRecordList());
    			if(!e.equals("")){
    				errors += e;
    				isValid = false;
    			}
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
    	
        jobEntry.setName(jobEntryName.getText());
        jobEntry.setLogicalFileName(fileName.getText());
        jobEntry.setDatasetName(datasetName.getText());
        jobEntry.setRecordName(recordName.getText());
       // jobEntry.setRecordDef(recordDef.getText());
        jobEntry.setRecordSet(recordSet.getText());
        jobEntry.setRecordList(ct.getRecordList());
        jobEntry.setFileType(fileType.getText());
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}
