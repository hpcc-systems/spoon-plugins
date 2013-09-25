/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecltable;

import java.util.Iterator;

import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
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

import org.hpccsystems.eclguifeatures.*;

import org.hpccsystems.recordlayout.CreateTable;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.ecljobentrybase.*;


/**
 *
 * @author ChambersJ
 */
public class ECLTableDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLTable jobEntry;
    
    private Text jobEntryName;

    private Text recordsetName;
    private Combo recordset;
    

   // private Text format;
    private Text expression;
    private Combo size; //FEW,MANY
    private Combo isUnsorted;
    private Combo runLocal;
    private Combo isKeyed;
    private Combo isMerge;

    private Table table;
    private TableViewer tableViewer;

    // Create a RecordList and assign it to an instance variable
    private RecordList recordList = new RecordList();
    private CreateTable ct = null;
    
    
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public ECLTableDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLTable) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Table");
        }
    }
    /*
     * person2DS.lastName;
INTEGER4 PersonCount := 1;

(non-Javadoc)
     * @see org.hpccsystems.ecljobentrybase.ECLJobEntryDialog#open()
     */

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        ct = new CreateTable(shell);
        ct.setIncludeCopyParent(true);
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
        shell.setText("Define an ECL Table");

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

        Group tableGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(tableGroup);
        tableGroup.setText("Table Details");
        tableGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 420;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        tableGroup.setLayoutData(datasetGroupFormat);
        
        recordsetName = buildText("Resulting Recordset", null, lsMod, middle, margin, tableGroup);
        
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, tableGroup, datasets);
        recordset.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				AutoPopulate ap = new AutoPopulate();
				try{
					RecordList fields = ap.rawFieldsByDataset(recordset.getText(), jobMeta.getJobCopies());
					ct.setParentLayout(fields);
				}catch (Exception e){
					System.out.println(e);
				}
			}
        	
        });
        expression = buildMultiText("Expression", recordset, lsMod, middle, margin, tableGroup);
        //format = buildMultiText("Format", expression, lsMod, middle, margin, tableGroup);
        size = buildCombo("Size", expression, lsMod, middle, margin, tableGroup, new String[]{"","FEW", "MANY"});
        isUnsorted = buildCombo("Unsorted", size, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
        isKeyed = buildCombo("Keyed", isUnsorted, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
        isMerge = buildCombo("Merge", isKeyed, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
        runLocal = buildCombo("RUNLOCAL", isMerge, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});


        
        
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

        //if (jobEntry.getFormat() != null) {
         //   format.setText(jobEntry.getFormat());
        //}
        if (jobEntry.getExpression() != null) {
            expression.setText(jobEntry.getExpression());
        }
        if (jobEntry.getSize() != null) {
            size.setText(jobEntry.getSize());
        }
        
        //no need to check null since never null
            isUnsorted.setText(jobEntry.getIsUnsortedString());
            runLocal.setText(jobEntry.getRunLocalString());
            isKeyed.setText(jobEntry.getIsKeyedString());
            isMerge.setText(jobEntry.getIsMergeString());

        if (jobEntry.getRecordset() != null) {
            recordset.setText(jobEntry.getRecordset());
        }
        if (jobEntry.getRecordsetName() != null) {
            recordsetName.setText(jobEntry.getRecordsetName());
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
    
    
    public JobEntryInterface open2() {
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
        shell.setText("Table");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Table");

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
        Group tableGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(tableGroup);
        tableGroup.setText("Table Details");
        tableGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 420;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        tableGroup.setLayoutData(datasetGroupFormat);

        //name = buildText("Distribute Name", null, lsMod, middle, margin, distributeGroup);

        
        recordsetName = buildText("Resulting Recordset", null, lsMod, middle, margin, tableGroup);
        
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, tableGroup, datasets);
        expression = buildMultiText("Expression", recordset, lsMod, middle, margin, tableGroup);
        //format = buildMultiText("Format", expression, lsMod, middle, margin, tableGroup);
        size = buildCombo("Size", expression, lsMod, middle, margin, tableGroup, new String[]{"","FEW", "MANY"});
        isUnsorted = buildCombo("Unsorted", size, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
        isKeyed = buildCombo("Keyed", isUnsorted, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
        isMerge = buildCombo("Merge", isKeyed, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
        runLocal = buildCombo("RUNLOCAL", isMerge, lsMod, middle, margin, tableGroup, new String[]{"false", "true"});
     

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tableGroup);

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
       // if (jobEntry.getFormat() != null) {
       //     format.setText(jobEntry.getFormat());
       // }
        if (jobEntry.getExpression() != null) {
            expression.setText(jobEntry.getExpression());
        }
        if (jobEntry.getSize() != null) {
            size.setText(jobEntry.getSize());
        }
        
        //no need to check null since never null
            isUnsorted.setText(jobEntry.getIsUnsortedString());
            runLocal.setText(jobEntry.getRunLocalString());
            isKeyed.setText(jobEntry.getIsKeyedString());
            isMerge.setText(jobEntry.getIsMergeString());

        if (jobEntry.getRecordset() != null) {
            recordset.setText(jobEntry.getRecordset());
        }
        if (jobEntry.getRecordsetName() != null) {
            recordsetName.setText(jobEntry.getRecordsetName());
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
    	
    	//only need to require a entry name
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	if(this.recordsetName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Resulting Recordset\"!\r\n";
    	}
    	
    	//recordset
    	if(this.recordset.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Recordset\"!\r\n";
    	}
    	
    	//format
    	//if(this.format.getText().equals("")){
    		//one is required.
    	//	isValid = false;
    	//	errors += "You must provide a \"Format\"!\r\n";
    	//}
    	
    	//few|many only allowed with expression
    	if(this.expression.getText().equals("")){
    		//don't allow few|many
    		if(!this.size.getText().equals("")){
        		//one is required.
        		isValid = false;
        		errors += "You must provide an \"Expression\" in order to provide a \"Size\"!\r\n";
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
        
        //jobEntry.setFormat(format.getText());
        jobEntry.setExpression(expression.getText());
        jobEntry.setSize(size.getText());
        jobEntry.setIsUnsortedString(isUnsorted.getText());
        jobEntry.setIsKeyedString(isKeyed.getText());
        jobEntry.setIsMergeString(isMerge.getText());
        

        jobEntry.setRecordset(recordset.getText());
        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRunLocalString(runLocal.getText());
        jobEntry.setRecordList(ct.getRecordList());
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }


}
