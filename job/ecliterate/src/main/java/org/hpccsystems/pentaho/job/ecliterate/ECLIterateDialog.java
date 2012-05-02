/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecliterate;

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

/**
 *
 * @author ChalaAX
 */
public class ECLIterateDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLIterate jobEntry;
    
    private Text jobEntryName;

    private Text transform;
     private Text transformName;
    private Text recordset;//Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    private Combo runLocal;
    
    private Text record;
    private Text recordName;
    private Text recordsetName;
    private Text recordsetNameIterate;
    
    private Text transformCall;
    private Text returnType;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public ECLIterateDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLIterate) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Iterate");
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

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;


        shell.setLayout(formLayout);
        shell.setText("Iterate");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Iterate");

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
        Group transformGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(transformGroup);
        transformGroup.setText("Transform Details");
        transformGroup.setLayout(groupLayout);
        FormData transformGroupFormat = new FormData();
        transformGroupFormat.top = new FormAttachment(generalGroup, margin);
        transformGroupFormat.width = 400;
        transformGroupFormat.height = 175;
        transformGroupFormat.left = new FormAttachment(middle, 0);
        transformGroup.setLayoutData(transformGroupFormat);
        
        transformName = buildText("Transform Name", null, lsMod, middle, margin, transformGroup);
        transform = buildMultiText("Transform", transformName, lsMod, middle, margin, transformGroup);


        Group recordGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(recordGroup);
        recordGroup.setText("Record Details");
        recordGroup.setLayout(groupLayout);
        FormData recordGroupFormat = new FormData();
        recordGroupFormat.top = new FormAttachment(transformGroup, margin);
        recordGroupFormat.width = 400;
        recordGroupFormat.height = 300;
        recordGroupFormat.left = new FormAttachment(middle, 0);
        recordGroup.setLayoutData(recordGroupFormat);
        
        //name = buildText("Distribute Name", null, lsMod, middle, margin, distributeGroup);


        recordName = buildText("Record Name", null, lsMod, middle, margin, recordGroup);
        record = buildMultiText("Record", recordName, lsMod, middle, margin, recordGroup);

        recordsetName = buildText("Recordset Name", record, lsMod, middle, margin, recordGroup);
        recordset = buildMultiText("Recordset", recordsetName, lsMod, middle, margin, recordGroup);
        
        Group iterateGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(iterateGroup);
        iterateGroup.setText("Iterate Details");
        iterateGroup.setLayout(groupLayout);
        FormData iterateGroupFormat = new FormData();
        iterateGroupFormat.top = new FormAttachment(recordGroup, margin);
        iterateGroupFormat.width = 400;
        iterateGroupFormat.height = 95;
        iterateGroupFormat.left = new FormAttachment(middle, 0);
        iterateGroup.setLayoutData(iterateGroupFormat);
        
        //returnType = buildText("Return Type", null, lsMod, middle, margin, iterateGroup);
        recordsetNameIterate = buildText("Resulting Recordset", null, lsMod, middle, margin, iterateGroup);
        transformCall = buildText("Transform Call", recordsetNameIterate, lsMod, middle, margin, iterateGroup);
        runLocal = buildCombo("RUNLOCAL", transformCall, lsMod, middle, margin, iterateGroup,new String[]{"false", "true"});
     
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
        

        

        if (jobEntry.getTransformName() != null) {
            transformName.setText(jobEntry.getTransformName());
        }
        if (jobEntry.getTransform() != null) {
            transform.setText(jobEntry.getTransform());
        }
        if (jobEntry.getRecordset() != null) {
            recordset.setText(jobEntry.getRecordset());
        }
        if (jobEntry.getRecordsetNameIterate() != null) {
            recordsetNameIterate.setText(jobEntry.getRecordsetNameIterate());
        }
        if (jobEntry.getRecordsetName() != null) {
            recordsetName.setText(jobEntry.getRecordsetName());
        }
        if (jobEntry.getRecord() != null) {
            record.setText(jobEntry.getRecord());
        }
        if (jobEntry.getRecordName() != null) {
            recordName.setText(jobEntry.getRecordName());
        }
        
        if (jobEntry.getReturnType() != null) {
            //returnType.setText(jobEntry.getReturnType());
        }
        
       // if (jobEntry.getRunLocalString() != null) {
            runLocal.setText(jobEntry.getRunLocalString());
        //}
        if (jobEntry.getTransformCall() != null) {
            transformCall.setText(jobEntry.getTransformCall());
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

    private Text buildText(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // text field
        Text text = new Text(groupBox, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(text);
        text.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        text.setLayoutData(fieldFormat);

        return text;
    }

    private Text buildMultiText(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // text field
        Text text = new Text(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
        props.setLook(text);
        text.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        fieldFormat.height = 100;
        text.setLayoutData(fieldFormat);

        return text;
    }

    private Combo buildCombo(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox, String[] items) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // combo field
        Combo combo = new Combo(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER);
        props.setLook(combo);
        combo.setItems(items);
        combo.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        fieldFormat.height = 50;
        combo.setLayoutData(fieldFormat);

        return combo;
    }

    private void ok() {
                    /*
     * private Text name;
    private Text datasetName;
    private Text expression;
    private Text index;
    private Text joinCondition;
    private Text skew;
    */
        //jobEntry.setJobName(jobEntryName.getText());
        jobEntry.setName(jobEntryName.getText());
        /*
    private Text record;
    private Text recordName;
    private Text recordsetName;
    */
        jobEntry.setRecordsetNameIterate(recordsetNameIterate.getText());
        jobEntry.setTransformName(transformName.getText());
        jobEntry.setTransform(transform.getText());
        jobEntry.setRecordset(recordset.getText());
        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRecord(record.getText());
        jobEntry.setRecordName(recordName.getText());
        jobEntry.setRunLocalString(runLocal.getText());
        jobEntry.setTransformCall(transformCall.getText());
       // jobEntry.setReturnType(returnType.getText());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

    public void dispose() {
        WindowProperty winprop = new WindowProperty(shell);
        props.setScreen(winprop);
        shell.dispose();
    }
}
