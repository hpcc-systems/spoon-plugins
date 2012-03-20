/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclglobalvariables;

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
public class ECLGlobalVariablesDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLGlobalVariables jobEntry;
    
    private Text jobEntryName;

    private Text serverIP;
    private Text serverPort;
    private Text landingZone;
    
    private Text cluster;
    private Text jobName;
    private Text eclccInstallDir;
    private Text mlPath;
    private Combo includeML;
 /*
             *  private String mlPath = "ecl-ml";
    private String eclccInstallDir = "C:\\Program Files\\HPCC Systems\\HPCC\\bin\\ver_3_0\\";
    private String jobName = "Spoon-job";
    private String cluster = "hthor";
    private boolean includeML = true;
             */
                 
    
    
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public ECLGlobalVariablesDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLGlobalVariables) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Global Variables");
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
            
            datasets = ap.parseDatasets(this.jobMeta.getJobCopies());
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
        shell.setText("Global Variables");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Variables");

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
        Group varGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(varGroup);
        varGroup.setText("Server Details");
        varGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 220;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        varGroup.setLayoutData(datasetGroupFormat);

        //name = buildText("Distribute Name", null, lsMod, middle, margin, distributeGroup);

        
        serverIP = buildText("Server Host", null, lsMod, middle, margin, varGroup);
        serverPort = buildText("Server Port", serverIP, lsMod, middle, margin, varGroup);
        landingZone = buildText("Landing Zone Dir", serverPort, lsMod, middle, margin, varGroup);
        
        //move thes to Job Information
        cluster = buildText("Cluster", landingZone, lsMod, middle, margin, varGroup);
        jobName = buildText("Job Name", cluster, lsMod, middle, margin, varGroup);
        
        //move these to Library(s)
        
        eclccInstallDir = buildText("eclcc Install Dir", jobName, lsMod, middle, margin, varGroup);
        mlPath = buildText("Path to ML Library", eclccInstallDir, lsMod, middle, margin, varGroup);
        includeML = buildCombo("Include ML Library?", mlPath, lsMod, middle, margin, varGroup, new String[]{"true", "false"});
        
        

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, varGroup);

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
        if (jobEntry.getServerIP() != null) {
            serverIP.setText(jobEntry.getServerIP());
        }
        if (jobEntry.getServerPort() != null) {
            serverPort.setText(jobEntry.getServerPort());
        }
        
        if (jobEntry.getLandingZone() != null) {
            landingZone.setText(jobEntry.getLandingZone());
        }

        
        if (jobEntry.getCluster() != null) {
            cluster.setText(jobEntry.getCluster());
        }
        
        if (jobEntry.getJobName() != null) {
            jobName.setText(jobEntry.getJobName());
        }
        if (jobEntry.getEclccInstallDir() != null) {
            eclccInstallDir.setText(jobEntry.getEclccInstallDir());
        }
        if (jobEntry.getMlPath() != null) {
            mlPath.setText(jobEntry.getMlPath());
        }
        if (jobEntry.getIncludeML() != null) {
            includeML.setText(jobEntry.getIncludeML());
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
        Text text = new Text(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER);
        props.setLook(text);
        text.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        fieldFormat.height = 50;
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
        
        jobEntry.setServerIP(serverIP.getText());
        jobEntry.setServerPort(serverPort.getText());
        jobEntry.setLandingZone(landingZone.getText());
        
        jobEntry.setCluster(cluster.getText());
        jobEntry.setJobName(jobName.getText());
        
        jobEntry.setEclccInstallDir(eclccInstallDir.getText());
        jobEntry.setMlPath(mlPath.getText());
        jobEntry.setIncludeML(includeML.getText());



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
