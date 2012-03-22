/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecljoin;

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
import org.eclipse.swt.events.*;

/**
 *
 * @author ChalaAX
 */
public class ECLJoinDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLJoin jobEntry;    
    private Text jobEntryName;

    private Text joinCondition;
    private Combo joinType;
    private Combo leftRecordSet;
    private Combo rightRecordSet;
    private Text joinRecordSet;
    
    private Combo leftJoinCondition;
    private Combo rightJoinCondition;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public ECLJoinDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLJoin) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Join");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();
        String datasets[] = null;
        AutoPopulate ap = new AutoPopulate();
        try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
            
            datasets = ap.parseDatasets(this.jobMeta.getJobCopies());
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        
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
        shell.setText("Join");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Join");

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
        //Join Declaration
        Group joinGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(joinGroup);
        joinGroup.setText("Join Details");
        joinGroup.setLayout(groupLayout);
        FormData joinGroupFormat = new FormData();
        joinGroupFormat.top = new FormAttachment(generalGroup, margin);
        joinGroupFormat.width = 400;
        joinGroupFormat.height = 180;
        joinGroupFormat.left = new FormAttachment(middle, 0);
        joinGroup.setLayoutData(joinGroupFormat);

        
        this.leftRecordSet = buildCombo("Left Recordset Name", null, lsMod, middle, margin, joinGroup,datasets);
        this.rightRecordSet = buildCombo("Right Recordset Name", this.leftRecordSet, lsMod, middle, margin, joinGroup,datasets);
       // this.joinCondition = buildText("Join Condition", this.rightRecordSet, lsMod, middle, margin, joinGroup);
        
        this.leftJoinCondition = buildCombo("Left Join Condition", this.rightRecordSet, lsMod, middle, margin, joinGroup, new String[]{""});
        this.rightJoinCondition = buildCombo("Right Join Condition", this.leftJoinCondition, lsMod, middle, margin, joinGroup, new String[]{""});
        
        this.joinType = buildCombo("Join Type", this.rightJoinCondition, lsMod, middle, margin, joinGroup, new String[]{"INNER","LEFT OUTER", "RIGHT OUTER", "FULL OUTER", "LEFT ONLY", "RIGHT ONLY","FULL ONLY"});
        
        this.joinRecordSet = buildText("Resulting Recordset", this.joinType, lsMod, middle, margin, joinGroup);
        

       
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, joinGroup);

        
        leftRecordSet.addModifyListener(new ModifyListener(){
            public void modifyText(ModifyEvent e){
                System.out.println("left RS changed");
                AutoPopulate ap = new AutoPopulate();
                try{
                    System.out.println("Load items for select");
                    String[] items = ap.fieldsByDataset( leftRecordSet.getText(),jobMeta.getJobCopies());
                    System.out.println("++++++"+items.length+"+++++");
                    leftJoinCondition.setItems(items);
                    System.out.println("itemsSet");
                    leftJoinCondition.redraw();
                    System.out.println("new Join condition vals loaded");
                }catch (Exception ex){
                    System.out.println("failed to load record definitions");
                    System.out.println(ex.toString());
                    ex.printStackTrace();
                }
               // leftJoinCondition.setItems(items);
            }
        });
        
        rightRecordSet.addModifyListener(new ModifyListener(){
            public void modifyText(ModifyEvent e){
                System.out.println("left RS changed");
                AutoPopulate ap = new AutoPopulate();
                try{
                    System.out.println("Load items for select");
                    String[] items = ap.fieldsByDataset( rightRecordSet.getText(),jobMeta.getJobCopies());
                    System.out.println("++++++"+items.length+"+++++");
                    rightJoinCondition.setItems(items);
                    System.out.println("itemsSet");
                    rightJoinCondition.redraw();
                    System.out.println("new Join condition vals loaded");
                }catch (Exception ex){
                    System.out.println("failed to load record definitions");
                    System.out.println(ex.toString());
                    ex.printStackTrace();
                }
               // leftJoinCondition.setItems(items);
            }
        });
        //this.leftRecordSet.addListener(SWT.Selection, leftRecordSetListener);
        //this.leftRecordSet.addModifyListener(leftRecordSetListener);
      
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
       
        //if (jobEntry.getJoinCondition() != null) {
            //this.joinCondition.setText(jobEntry.getJoinCondition());
        //}
        if (jobEntry.getJoinType() != null) {
            this.joinType.setText(jobEntry.getJoinType());
        }
        if (jobEntry.getLeftRecordSet() != null) {
            this.leftRecordSet.setText(jobEntry.getLeftRecordSet());
        }
        if (jobEntry.getRightRecordSet() != null) {
            this.rightRecordSet.setText(jobEntry.getRightRecordSet());
        }
        if (jobEntry.getJoinRecordSet() != null) {
            this.joinRecordSet.setText(jobEntry.getJoinRecordSet());
        }
        
        if (jobEntry.getLeftJoinCondition() != null) {
            this.leftJoinCondition.setText(jobEntry.getLeftJoinCondition());
        }
        if (jobEntry.getRightJoinCondition() != null) {
            this.rightJoinCondition.setText(jobEntry.getRightJoinCondition());
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
         *      private Text joinCondition;
                private Text joinType;
                private Text leftRecordSet;
                private Text rightRecordSet;
         */
        jobEntry.setName(jobEntryName.getText());
        //jobEntry.setJoinCondition(this.joinCondition.getText());
        jobEntry.setLeftJoinCondition(this.leftJoinCondition.getText());
        jobEntry.setRightJoinCondition(this.rightJoinCondition.getText());
        jobEntry.setJoinType(this.joinType.getText());
        jobEntry.setLeftRecordSet(this.leftRecordSet.getText());
        jobEntry.setRightRecordSet(this.rightRecordSet.getText());
        jobEntry.setJoinRecordSet(this.joinRecordSet.getText());
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
