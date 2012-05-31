/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclrollup;

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

/**
 *
 * @author ChalaAX
 */
public class ECLRollupDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLRollup jobEntry;
    
    private Text jobEntryName;

    private Text recordsetName;
    private Combo recordset;
    private Text condition;
    private Text transformName;
    private Text transform;
    private Text transformCall;
    private Text fieldlist;
    private Text group;
    private Combo runLocal;//optional
    
    
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public boolean isBackupChanged() {
        return backupChanged;
    }

    public void setBackupChanged(boolean backupChanged) {
        this.backupChanged = backupChanged;
    }

    public Text getCondition() {
        return condition;
    }

    public void setCondition(Text condition) {
        this.condition = condition;
    }

    public Text getFieldlist() {
        return fieldlist;
    }

    public void setFieldlist(Text fieldlist) {
        this.fieldlist = fieldlist;
    }

    public Text getGroup() {
        return group;
    }

    public void setGroup(Text group) {
        this.group = group;
    }

    public ECLRollup getJobEntry() {
        return jobEntry;
    }

    public void setJobEntry(ECLRollup jobEntry) {
        this.jobEntry = jobEntry;
    }

    public Text getJobEntryName() {
        return jobEntryName;
    }

    public void setJobEntryName(Text jobEntryName) {
        this.jobEntryName = jobEntryName;
    }

    public SelectionAdapter getLsDef() {
        return lsDef;
    }

    public void setLsDef(SelectionAdapter lsDef) {
        this.lsDef = lsDef;
    }

    public Combo getRecordset() {
        return recordset;
    }

    public void setRecordset(Combo recordset) {
        this.recordset = recordset;
    }

    public Combo getRunLocal() {
        return runLocal;
    }

    public void setRunLocal(Combo runLocal) {
        this.runLocal = runLocal;
    }

    public Text getTransform() {
        return transform;
    }

    public void setTransform(Text transform) {
        this.transform = transform;
    }

    public Text getTransformCall() {
        return transformCall;
    }

    public void setTransformCall(Text transformCall) {
        this.transformCall = transformCall;
    }

    public Text getTransformName() {
        return transformName;
    }

    public void setTransformName(Text transformName) {
        this.transformName = transformName;
    }

    public Button getwCancel() {
        return wCancel;
    }

    public void setwCancel(Button wCancel) {
        this.wCancel = wCancel;
    }

    public Button getwOK() {
        return wOK;
    }

    public void setwOK(Button wOK) {
        this.wOK = wOK;
    }

    public ECLRollupDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLRollup) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Rollup");
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
        shell.setText("Rollup");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Rollup");

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
        iterateGroup.setText("Rollup Details");
        iterateGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 500;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        iterateGroup.setLayoutData(datasetGroupFormat);

        //name = buildText("Distribute Name", null, lsMod, middle, margin, distributeGroup);


                    

        recordsetName = buildText("Resulting Recordset", null, lsMod, middle, margin, iterateGroup);
       
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, iterateGroup,datasets);
        condition = buildText("Condtion", recordset, lsMod, middle, margin, iterateGroup);
        transformName = buildText("Transform Name", condition, lsMod, middle, margin, iterateGroup);
        
        transform = buildMultiText("Transform", transformName, lsMod, middle, margin, iterateGroup);
        fieldlist = buildMultiText("Fieldlist", transform, lsMod, middle, margin, iterateGroup);
        group = buildText("Group", fieldlist, lsMod, middle, margin, iterateGroup);
        
        transformCall = buildMultiText("Transform Call", group, lsMod, middle, margin, iterateGroup);
        runLocal = buildCombo("RUNLOCAL", transformCall, lsMod, middle, margin, iterateGroup, new String[]{"false", "true"});
     
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

        if (jobEntry.getRecordset() != null) {
            recordset.setText(jobEntry.getRecordset());
        }
        if (jobEntry.getRecordsetName() != null) {
            recordsetName.setText(jobEntry.getRecordsetName());
        }
        
        if (jobEntry.getCondition() != null) {
            condition.setText(jobEntry.getCondition());
        }
        
        
        if (jobEntry.getTransform() != null) {
            transform.setText(jobEntry.getTransform());
        }
        if (jobEntry.getTransformCall() != null) {
            transformCall.setText(jobEntry.getTransformCall());
        }
        if (jobEntry.getTransformName() != null) {
            transformName.setText(jobEntry.getTransformName());
        }
        
        if (jobEntry.getFieldlist() != null) {
            fieldlist.setText(jobEntry.getFieldlist());
        }
       
        if (jobEntry.getGroup() != null) {
            group.setText(jobEntry.getGroup());
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
        jobEntry.setTransformName(transformName.getText());
        jobEntry.setTransform(transform.getText());
        jobEntry.setTransformCall(transformCall.getText());
        jobEntry.setFieldlist(fieldlist.getText());
        jobEntry.setGroup(group.getText());
        jobEntry.setRunLocalString(runLocal.getText());

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
