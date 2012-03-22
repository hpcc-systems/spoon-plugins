/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclindex;

import java.util.Iterator;
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
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;

/**
 *
 * @author ChalaAX
 */
public class ECLIndexDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLIndex jobEntry;    
    private Text jobEntryName;
    
    private Text baserecset;
    private Text keys;
    private Text payload;
    private Text indexfile;
    private Combo sorted;
    private Combo preload;
    private Combo opt;
    private Combo compressed;
    private Combo distributed;
    private Text index;
    private Text newindexfile;
  
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
    private CreateTable keysCT = null;
    private CreateTable payloadCT = null;
    
    private RecordList keysRecordList = new RecordList();
    private RecordList payloadRecordList = new RecordList();

    public ECLIndexDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLIndex) jobEntryInt;
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
        keysCT = new CreateTable(shell);
        payloadCT = new CreateTable(shell);
        
        TabFolder tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        FormData data = new FormData();
        
        data.height = 400;
        data.width = 640;
        tabFolder.setLayoutData(data);
        
        Composite compForGrp = new Composite(tabFolder, SWT.NONE);
	//compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
        compForGrp.setBackground(new Color(tabFolder.getDisplay(),255,255,255));
        compForGrp.setLayout(new FormLayout());
                
                
        
        TabItem generalTab = new TabItem(tabFolder, SWT.NULL);
         generalTab.setText ("General");
        
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
        //Join Declaration
        Group indexGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(indexGroup);
        indexGroup.setText("Join Details");
        indexGroup.setLayout(groupLayout);
        FormData joinGroupFormat = new FormData();
        joinGroupFormat.top = new FormAttachment(generalGroup, margin);
        joinGroupFormat.width = 400;
        joinGroupFormat.height = 250;
        joinGroupFormat.left = new FormAttachment(middle, 0);
        indexGroup.setLayoutData(joinGroupFormat);

        
        //this.leftRecordSet = buildCombo("Left Recordset Name", null, lsMod, middle, margin, indexGroup,datasets);
        //this.joinRecordSet = buildText("Resulting Recordset", this.joinType, lsMod, middle, margin, indexGroup);
        this.baserecset = buildText("Basereset", null, lsMod, middle, margin, indexGroup);
        this.indexfile = buildText("Index File", baserecset, lsMod, middle, margin, indexGroup);
        this.sorted = buildCombo("Is Sorted", indexfile, lsMod, middle, margin, indexGroup, new String[]{"", "Yes", "No"});
        this.preload = buildCombo("Is Preload", sorted, lsMod, middle, margin, indexGroup, new String[]{"", "Yes", "No"});
        this.opt = buildCombo("Is OPT", preload, lsMod, middle, margin, indexGroup, new String[]{"", "Yes", "No"});
        this.compressed = buildCombo("Is Compressed", opt, lsMod, middle, margin, indexGroup, new String[]{"", "LZW", "ROW", "FIRST"});
        this.distributed = buildCombo("Is Distributed", compressed, lsMod, middle, margin, indexGroup, new String[]{"", "Yes", "No"});
        
        this.index = buildText("Resulting Recordset", distributed, lsMod, middle, margin, indexGroup);
        this.newindexfile = buildText("Resulting Recordset", index, lsMod, middle, margin, indexGroup);
        
         generalTab.setControl(compForGrp);
         
        if(jobEntry.getKeys() != null){
            keysRecordList = jobEntry.getKeys();
            keysCT.setRecordList(jobEntry.getKeys());
            
            if(keysRecordList.getRecords() != null && keysRecordList.getRecords().size() > 0) {
                    System.out.println("Size: "+keysRecordList.getRecords().size());
                    for (Iterator<RecordBO> iterator = keysRecordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO obj = (RecordBO) iterator.next();
                    }
            }
        }
        
        TabItem keysTab = keysCT.buildDefTab("Keys", tabFolder);
        
        
        if(jobEntry.getPayload() != null){
            payloadRecordList = jobEntry.getPayload();
            payloadCT.setRecordList(jobEntry.getPayload());
            
            if(payloadRecordList.getRecords() != null && payloadRecordList.getRecords().size() > 0) {
                    System.out.println("Size: "+payloadRecordList.getRecords().size());
                    for (Iterator<RecordBO> iterator = payloadRecordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO obj = (RecordBO) iterator.next();
                    }
            }
        }
        TabItem payloadTab = payloadCT.buildDefTab("Payload", tabFolder);
        keysCT.redrawTable(true);
       
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
 /*
             *     private String baserecset;
    private String keys;
    private String payload;
    private String indexfile;
    private String sorted;
    private String preload;
    private String opt;
    private String compressed;
    private String distributed;
    private String index;
    private String newindexfile;
             * 
             */
        if (jobEntry.getBaserecset() != null) {
            baserecset.setText(jobEntry.getBaserecset());
        }
        /*if (jobEntry.getKeys() != null) {
            keys.setText(jobEntry.getKeys());
        }*/
        if (jobEntry.getIndexfile() != null) {
            indexfile.setText(jobEntry.getIndexfile());
        }
        if (jobEntry.getSorted() != null) {
            sorted.setText(jobEntry.getSorted());
        }
        if (jobEntry.getPreload() != null) {
            preload.setText(jobEntry.getPreload());
        }
        if (jobEntry.getOpt() != null) {
            opt.setText(jobEntry.getOpt());
        }
        if (jobEntry.getCompressed() != null) {
            compressed.setText(jobEntry.getCompressed());
        }
        if (jobEntry.getDistributed() != null) {
            distributed.setText(jobEntry.getDistributed());
        }
        if (jobEntry.getIndex() != null) {
            index.setText(jobEntry.getIndex());
        }
        if (jobEntry.getNewindexfile() != null) {
            newindexfile.setText(jobEntry.getNewindexfile());
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

        jobEntry.setName(jobEntryName.getText());
               /*
             *     private String baserecset;
    private String keys;
    private String payload;
    private String indexfile;
    private String sorted;
    private String preload;
    private String opt;
    private String compressed;
    private String distributed;
    private String index;
    private String newindexfile;
             * 
             */
        jobEntry.setBaserecset(baserecset.getText());
        //jobEntry.setKeys(keys.getText());
        jobEntry.setIndexfile(indexfile.getText());
        jobEntry.setSorted(sorted.getText());
        jobEntry.setPreload(preload.getText());
        jobEntry.setOpt(opt.getText());
        jobEntry.setCompressed(compressed.getText());
        jobEntry.setDistributed(distributed.getText());
        jobEntry.setIndex(index.getText());
        jobEntry.setNewindexfile(newindexfile.getText());
        
        jobEntry.setKeys(keysCT.getRecordList());
        jobEntry.setPayload(payloadCT.getRecordList());

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
