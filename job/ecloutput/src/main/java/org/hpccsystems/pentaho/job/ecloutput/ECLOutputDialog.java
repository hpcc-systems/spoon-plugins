/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecloutput;

import org.eclipse.swt.layout.GridLayout;
import java.util.ArrayList;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;






import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.job.JobHopMeta;

import org.pentaho.di.job.entry.JobEntryCopy;
import org.hpccsystems.javaecl.HPCCServerInfo;
import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.ecljobentrybase.*;
//JobEntry






import java.util.HashMap;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.hpccsystems.ecljobentrybase.*;


/**
 *
 * @author ChambersJ
 */
public class ECLOutputDialog extends ECLJobEntryDialog implements JobEntryDialogInterface {
	
	
    private ECLOutput jobEntry;
    private Text jobEntryName;

    private Button wOK, wCancel, fileOpenButton;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
   
    private Combo recordset;
    
    

    private Combo isDef; //true set output into attr using job entry name
    private Combo outputType; //recordset,expression
    //private Combo includeFormat; //yes/no
    
    private Combo inputType; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
    private Text outputFormat;
    //private Text recordset;
    
    private Text expression;
    
    
    private Text file;
    private Text typeOptions; // used for CSV, XML, Pipe
    private Text fileOptions; // used for CSV, XML
    
    private Text named; //used for named
    private Combo extend; // availiable for Named
    private Combo returnAll; // availiable for Named
    
    private Combo thor; // used in 
    
    //used in different file types
    private Combo cluster;
    private Text encrypt;
    private Combo compressed;
    private Combo overwrite;
    private Text expire;
    private Combo repeat;// piped

    private Combo pipeType;
    
    
    private Group expressionGroup;
    private Group outputGroup;
    private Group generalGroup;
    private Group recordsetGroup;
    private Group recordsetDetailsGroup;
    private Group fileTypeGroup;
    private Composite c1;
    private FormLayout groupLayout;
    
    private String datasets[] = null;
   
    //private Text serverAddress;
    
    
    private HashMap controls = new HashMap();

    public ECLOutputDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLOutput) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Output");
        }
        
        
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();
        

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        shell.setLayout(new FillLayout(SWT.VERTICAL));
        shell.setText("Define an ECL Output");
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };

        backupChanged = jobEntry.hasChanged();
        
        createContents(shell, lsMod);
        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });
        
                shell.pack();
        shell.open();
        c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        c1.layout();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        
        
        return jobEntry;
    
    }
    
    private void createContents(Composite shell, final ModifyListener lsMod) {
        /*
        final String datasets[] = new String[]{"", "a", "b"};
        Display display = new Display();
        Color red = display.getSystemColor(SWT.COLOR_RED);
        Color blue = display.getSystemColor(SWT.COLOR_BLUE);
        Shell shell = new Shell(display);
        */
        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;
        
        
        AutoPopulate ap = new AutoPopulate();
        try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
            
            datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        
        
        
        shell.setBackground(new Color(shell.getDisplay(),255,255,255));
        GridLayout fl = new GridLayout(2,false);
        
        shell.setLayout(fl);
        GridData gd = new GridData();
        gd.horizontalSpan = 2;



        groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;
        

        final ScrolledComposite sc1 = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.BORDER | SWT.SHADOW_OUT);
        
        sc1.setBackground(new Color(sc1.getDisplay(),255,255,255));
        sc1.setLayoutData(gd);
        sc1.setAlwaysShowScrollBars(true);
        
        c1 = new Composite(sc1, SWT.NONE);
        sc1.setContent(c1);
        //c1.setBackground(red);
        GridLayout layout = new GridLayout();
        layout.marginRight = 15;
        layout.numColumns = 1;
        
        c1.setLayout(layout);
        c1.setBackground(new Color(c1.getDisplay(),255,255,255));

        c1.setSize(595, 400);
        
        this.createGeneralGroup(c1, groupLayout, lsMod);
        
        this.loadGeneralGroup(lsMod);
        //Point dPoint = c1.computeSize(SWT.DEFAULT, SWT.DEFAULT);
        
        //c1.setSize(dPoint);
        c1.setSize(new Point(560,400));

/*
        Button ok = new Button(shell, SWT.PUSH);
        ok.setText("OK");
         Button cancel = new Button(shell, SWT.PUSH);
        cancel.setText("Cancel");
 */       
        
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        
        GridData okGrid = new GridData();
       
        okGrid.widthHint = 55;
        okGrid.heightHint = 24;
        okGrid.horizontalIndent = 250;
        wOK.setLayoutData(okGrid);
        
        
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        
        GridData cancelGrid = new GridData();
        cancelGrid.widthHint = 55;
        cancelGrid.heightHint = 24;
        wCancel.setLayoutData(cancelGrid);

       //BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, shell);

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

        shell.setSize(600, 500);
         
        
        


        
    }
    
    
    
    
    
    private void loadGeneralGroup(final ModifyListener lsMod){
       if(jobEntry.getInputType().equals("Recordset")){
            this.createOutputGroup(c1, groupLayout, lsMod);
       }
        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
         if (jobEntry.getIsDef() != null) {
            
            this.isDef.setText(jobEntry.getIsDef());
            
        }
        if(jobEntry.getInputType() != null){
            String t = jobEntry.getInputType();
            this.inputType.setText(t);
            
            disposeExpressionGroup();
            disposeRecordsetGroup();
            disposeFileTypeGroup();       
            disposeRecordsetDetailsGroup();

            if(inputType.getText().equals("Recordset")) {
                createRecordsetGroup(c1, groupLayout, lsMod);
            } else if(inputType.getText().equals("Expression")){

                createExpressionGroup(c1, groupLayout, lsMod);
            }
            c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
            c1.layout();
        }
        //if(jobEntry.getIncludeFormat() != null){
        //    this.includeFormat.setText(jobEntry.getIncludeFormat());
        //}
        
        
        this.inputType.addSelectionListener(new SelectionAdapter() {
                    public void widgetSelected(SelectionEvent e) {
                            
                            disposeExpressionGroup();
                            disposeRecordsetGroup();
                            disposeFileTypeGroup();       
                            disposeRecordsetDetailsGroup();
                            disposeOutputGroup();
                            if(inputType.getText().equals("Recordset")) {
                                createOutputGroup(c1, groupLayout, lsMod);
                                createRecordsetGroup(c1, groupLayout, lsMod);
                            } else if(inputType.getText().equals("Expression")){
                                
                                createExpressionGroup(c1, groupLayout, lsMod);
                            }
                        c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                        c1.layout();
                    };
            });
    }
    
    private void createGeneralGroup(Composite parent, final FormLayout groupLayout, final ModifyListener lsMod) {
        int middle = 50;
        final int margin = 10;
        generalGroup = new Group(parent, SWT.SHADOW_NONE);
        generalGroup.setBackground(new Color(generalGroup.getDisplay(),255,255,255));
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 150;
        groupFormat.heightHint = 150;
        groupFormat.widthHint = 530;
        generalGroup.setLayoutData(groupFormat);
        generalGroup.setSize(generalGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        generalGroup.layout();

        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);
        this.isDef = buildCombo("Is definition", jobEntryName, lsMod, middle, margin, generalGroup, new String[]{"", "Yes", "No"});
        this.inputType = buildCombo("Input Type", isDef, lsMod, middle, margin, generalGroup, new String[]{"", "Recordset", "Expression"});
       // this.includeFormat = buildCombo("Include Format", inputType, lsMod, middle, margin, generalGroup, new String[]{"", "Yes", "No"});
        
       
        

    }
    
    
    private void disposeOutputGroup(){
         if(this.recordset != null)this.recordset.dispose();
         if(this.outputGroup != null){
            this.outputGroup.dispose();
        }
    }
    private void createOutputGroup(Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 50;
        final int margin = 10;
        outputGroup = new Group(parent, SWT.SHADOW_NONE);
        outputGroup.setBackground(new Color(outputGroup.getDisplay(),255,255,255));
        outputGroup.setText("Input Details");
        outputGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 65;
        groupFormat.heightHint = 55;
        groupFormat.widthHint = 530;
        outputGroup.setLayoutData(groupFormat);
        outputGroup.setSize(outputGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        outputGroup.layout();



        this.recordset = buildCombo("Recordset Name", null, lsMod, middle, margin, outputGroup, datasets);
        
        if (jobEntry.getRecordset() != null) {
            
            this.recordset.setText(jobEntry.getRecordset());
        }
        

    }

    private void disposeExpressionGroup(){
        if(this.expression != null)this.expression.dispose();
        if(this.named != null)this.named.dispose();
        if(this.expressionGroup != null){
            this.expressionGroup.dispose();
        }
    }
    private void createExpressionGroup(Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        this.expressionGroup = new Group(parent, SWT.SHADOW_NONE);
        expressionGroup.setBackground(new Color(expressionGroup.getDisplay(),255,255,255));
        expressionGroup.setText("Expression Details");
        expressionGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 265;
        groupFormat.heightHint = 265;
        groupFormat.widthHint = 530;
        expressionGroup.setLayoutData(groupFormat);
        expressionGroup.setSize(expressionGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        expressionGroup.layout();



        expression = buildMultiText("Expression", null, lsMod, middle, margin, this.expressionGroup);
        named = buildText("Name", expression, lsMod, middle, margin, this.expressionGroup);
        
        if(jobEntry.getExpression() != null && !this.expression.isDisposed()){
            this.expression.setText(jobEntry.getExpression());
        }
        //System.out.println("no if read anmed from file:" + jobEntry.getNamed() + "|");
        if(jobEntry.getNamed() != null && !this.named.isDisposed()){
           // System.out.println("read anmed from file:" + jobEntry.getNamed() + "|");
            this.named.setText(jobEntry.getNamed());
        }

    }
    private void disposeRecordsetGroup(){
        if(this.thor != null)this.thor.dispose();
        if(this.recordsetGroup != null)this.recordsetGroup.dispose();
    }
     private void createRecordsetGroup(Composite parent, final FormLayout groupLayout, final ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        recordsetGroup = new Group(parent, SWT.SHADOW_NONE);
        recordsetGroup.setBackground(new Color(recordsetGroup.getDisplay(),255,255,255));
        recordsetGroup.setText("Recordset Details");
        recordsetGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 65;
        groupFormat.heightHint = 65;
        groupFormat.widthHint = 530;
        recordsetGroup.setLayoutData(groupFormat);
        recordsetGroup.setSize(recordsetGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        recordsetGroup.layout();
        
        this.thor = buildCombo("File on disk, \"owned\" by workunit", null, lsMod, middle, margin, recordsetGroup,new String[]{"", "Yes", "No"});

        if(jobEntry.getThor() != null){
            this.thor.setText(jobEntry.getThor());
            disposeFileTypeGroup();       
            disposeRecordsetDetailsGroup();
            if(thor.getText().equals("No")){
               createRecordsetDetailsGroup(c1, groupLayout, lsMod);
            }
            c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                        c1.layout();
        }
        this.thor.addSelectionListener(new SelectionAdapter(){
                public void widgetSelected(SelectionEvent e) {
                    disposeFileTypeGroup();       
                    disposeRecordsetDetailsGroup();
                    
                    if(thor.getText().equals("Yes")) {

                    } else if(thor.getText().equals("No")){
                       createRecordsetDetailsGroup(c1, groupLayout, lsMod);
                    }
                    c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                    c1.layout();
                };
                
            
            });

       
    }
    
    private void disposeRecordsetDetailsGroup(){
        if(this.outputFormat != null)this.outputFormat.dispose();
        if(this.outputType != null)this.outputType.dispose();
        if(this.recordsetDetailsGroup != null)this.recordsetDetailsGroup.dispose();
    }
    private void createRecordsetDetailsGroup(Composite parent, final FormLayout groupLayout, final ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        recordsetDetailsGroup = new Group(parent, SWT.SHADOW_NONE);
        recordsetDetailsGroup.setBackground(new Color(recordsetDetailsGroup.getDisplay(),255,255,255));
        recordsetDetailsGroup.setText("Recordset Output Details");
        recordsetDetailsGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 105;
        groupFormat.heightHint = 105;
        groupFormat.widthHint = 530;
        recordsetDetailsGroup.setLayoutData(groupFormat);
        recordsetDetailsGroup.setSize(recordsetDetailsGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        recordsetDetailsGroup.layout();


        this.outputFormat = buildText("Limit Columns (CSV)", null, lsMod, middle, margin, recordsetDetailsGroup);
        this.outputType = buildCombo("Type", this.outputFormat, lsMod, middle, margin, recordsetDetailsGroup,new String[]{"", "File", "File - CSV", "File - XML", "Piped", "Named"});
        
        if(jobEntry.getOutputFormat() != null){
            this.outputFormat.setText(jobEntry.getOutputFormat());
        }
        if(jobEntry.getOutputFormat() != null){
            this.outputType.setText(jobEntry.getOutputType());
            //disposeFileTypeGroup();
            if(!outputType.getText().equals("")){
                createFileTypeGroup(outputType.getText(), c1, groupLayout, lsMod);
            }
            c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
            c1.layout();
        }
        
        this.outputType.addSelectionListener(new SelectionAdapter() {
                    public void widgetSelected(SelectionEvent e) {
                           disposeFileTypeGroup();
                            
                            if(!outputType.getText().equals("")){
                                createFileTypeGroup(outputType.getText(), c1, groupLayout, lsMod);
                            }
                        c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                        c1.layout();
                    };
            });
       
    }
    
    private void disposeFileTypeGroup(){
         if(this.file != null){
            this.file.dispose();
        }
        if(this.fileOptions != null ){
            this.fileOptions.dispose();
        }
        if(this.typeOptions != null ){
            this.typeOptions.dispose();
        }
        if(this.cluster != null ){
            this.cluster.dispose();
        }
        if(this.encrypt != null ){
            this.encrypt.dispose();
        }
        if(this.overwrite != null){
            this.overwrite.dispose();
        }
        if(this.expire != null ){
            this.expire.dispose();
        }
        if(this.pipeType != null ){
            this.pipeType.dispose();
        }
        if(this.repeat != null ){
            this.repeat.dispose();
        }
        if(this.extend != null ){
            this.extend.dispose();
        }
        if(this.returnAll != null ){
            this.returnAll.dispose();
        }
        if(this.compressed != null ){
            this.compressed.dispose();
        }
        if(this.fileTypeGroup != null){
            this.fileTypeGroup.dispose();
        }
        
    }
    private void createFileTypeGroup(String type, Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        fileTypeGroup = new Group(parent, SWT.SHADOW_NONE);
        fileTypeGroup.setBackground(new Color(fileTypeGroup.getDisplay(),255,255,255));
        fileTypeGroup.setText(type + " Details");
        fileTypeGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        
        groupFormat.widthHint = 530;
        
        AutoPopulate ap = new AutoPopulate();
        String[] clusters = new String[0];
        
        String serverHost = "";
        int serverPort = 8010;
        String user = "";
        String pass = "";
        try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
                
                serverHost = ap.getGlobalVariable(this.jobMeta.getJobCopies(),"server_ip");
                serverPort = Integer.parseInt(ap.getGlobalVariable(jobMeta.getJobCopies(),"server_port"));
                user = ap.getGlobalVariable(jobMeta.getJobCopies(),"user_name");
                pass = ap.getGlobalVariableEncrypted(jobMeta.getJobCopies(),"password");
                
            }catch (Exception e){
                System.out.println("Error Parsing existing Global Variables ");
                System.out.println(e.toString());
                
            }
        HPCCServerInfo hsi = new HPCCServerInfo(serverHost,serverPort,user,pass);
        
        clusters = hsi.fetchTargetClusters();
        
        int height = 320;
       // {"", "File", "File - CSV", "File - XML", "Piped", "Named"}
        if(type.equals("File")){
            //height = 105;
            this.file = buildText("File", null, lsMod, middle, margin, fileTypeGroup);
            //this.fileOptions = buildText("Thor File Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.cluster = buildCombo("Target Cluster", this.file, lsMod, middle, margin, fileTypeGroup, clusters);
            this.encrypt = buildText("Encryption Key", this.cluster, lsMod, middle, margin, fileTypeGroup);
            this.compressed = buildCombo("Compress", this.encrypt, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
            this.overwrite = buildCombo("Overwrite", this.compressed, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
            this.expire = buildText("Days till expire", this.overwrite, lsMod, middle, margin, fileTypeGroup);
            
        }else if(type.equals("File - CSV")){
            height = 300;
            this.file = buildText("File", null, lsMod, middle, margin, fileTypeGroup);
            //this.fileOptions = buildText("CSV File Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.typeOptions = buildText("CSV Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.cluster = buildCombo("Target Cluster", this.typeOptions, lsMod, middle, margin, fileTypeGroup, clusters);
            this.encrypt = buildText("Encryption Key", this.cluster, lsMod, middle, margin, fileTypeGroup);
            this.overwrite = buildCombo("Overwrite", this.encrypt, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
            this.expire = buildText("Days till expire", this.overwrite, lsMod, middle, margin, fileTypeGroup);
            
        }else if(type.equals("File - XML")){
            height = 300;
            this.file = buildText("File", null, lsMod, middle, margin, fileTypeGroup);
            //this.fileOptions = buildText("XML File Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.typeOptions = buildText("XML Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.cluster = buildCombo("Target Cluster", this.typeOptions, lsMod, middle, margin, fileTypeGroup,clusters);
            this.encrypt = buildText("Encryption Key", this.cluster, lsMod, middle, margin, fileTypeGroup);
            this.overwrite = buildCombo("Overwrite", this.encrypt, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
            this.expire = buildText("Days till expire", this.overwrite, lsMod, middle, margin, fileTypeGroup);
        }else if (type.equals("Piped")){
            height = 165;
            this.typeOptions = buildText("Pipe Options", null, lsMod, middle, margin, fileTypeGroup);
            this.pipeType = buildCombo("Type", this.typeOptions, lsMod, middle, margin, fileTypeGroup,new String[]{"", "CSV", "XML"});
            this.repeat = buildCombo("Repeat", this.pipeType, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
        }else if (type.equals("Named")){
            height = 165;
            this.named = buildText("Name", null, lsMod, middle, margin, fileTypeGroup);
            this.extend = buildCombo("Extend", this.named, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
            this.returnAll = buildCombo("All", this.extend, lsMod, middle, margin, fileTypeGroup,new String[]{"", "Yes", "No"});
        }
        
        groupFormat.minimumHeight = height;
        groupFormat.heightHint = height;
        fileTypeGroup.setLayoutData(groupFormat);
        fileTypeGroup.setSize(fileTypeGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        fileTypeGroup.layout();

        if(this.file != null && !this.file.isDisposed() &&  jobEntry.getFile() != null){
            this.file.setText(jobEntry.getFile());
        }
        if(this.fileOptions != null && !this.fileOptions.isDisposed() && jobEntry.getFileOptions() != null){
            this.fileOptions.setText(jobEntry.getFileOptions());
        }
        if(this.typeOptions != null && !this.typeOptions.isDisposed() && jobEntry.getTypeOptions() != null){
            this.typeOptions.setText(jobEntry.getTypeOptions());
        }
        if(this.cluster != null && !this.cluster.isDisposed() && jobEntry.getCluster() != null){
            this.cluster.setText(jobEntry.getCluster());
        }
        if(this.encrypt != null && !this.encrypt.isDisposed() && jobEntry.getEncrypt() != null){
            this.encrypt.setText(jobEntry.getEncrypt());
        }
        if(this.overwrite != null && !this.overwrite.isDisposed() && jobEntry.getOverwrite() != null){
            this.overwrite.setText(jobEntry.getOverwrite());
        }
        if(this.expire != null && !this.expire.isDisposed() && jobEntry.getExpire() != null){
            this.expire.setText(jobEntry.getExpire());
        }
        if(this.pipeType != null && !this.pipeType.isDisposed() && jobEntry.getPipeType() != null){
            this.pipeType.setText(jobEntry.getPipeType());
        }
        if(this.repeat != null && !this.repeat.isDisposed() && jobEntry.getRepeat() != null){
            this.repeat.setText(jobEntry.getRepeat());
        }
        if(this.extend != null && !this.extend.isDisposed() && jobEntry.getExtend() != null){
            this.extend.setText(jobEntry.getExtend());
        }
        if(this.returnAll != null && !this.returnAll.isDisposed() && jobEntry.getReturnAll() != null){
            this.returnAll.setText(jobEntry.getReturnAll());
        }
        if(this.compressed != null && !this.compressed.isDisposed() && jobEntry.getCompressed() != null){
            this.compressed.setText(jobEntry.getCompressed());
        }
        if(this.named != null && !this.named.isDisposed() && jobEntry.getNamed() != null ){
           // System.out.println("read anmed from file:" + jobEntry.getNamed() + "|");
            this.named.setText(jobEntry.getNamed());
        }

        
        
       
    }
    
    
 
    
    
    
    
    
    
    
    
    
    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	//check to see that the minimum required fields are populated
    	//if errors are recorded depatch promp
    	
    	
    	if(this.jobEntryName.getText().equalsIgnoreCase("")){
    		isValid = false;
    		errors += "\"Job Entry Name\" is a required field!\r\n";
    	}
    	if(isDef.getText().equalsIgnoreCase("")){
    		isValid = false;
    		errors += "\"Is Definition\" is a required field!\r\n";
    	}else if(isDef.getText().equalsIgnoreCase("yes")){
    		isValid = false;
    		errors += "Warning: \"Is Definition\" is set to yes, please keep in mind this output will not be executed until called.\r\n";
    		errors += "\tAlso if this is your only output and the Definition isn't called then this will cause an error.\r\n";
    	}
    	
    	if(this.inputType.getText().equals("Expression")){
    		//require expression
    		if(this.expression.getText().equals("")){
    			isValid = false;
        		errors += "\"Expression\" is a Required field for \"Input Type\" Expression!\r\n";
        	}
    		
    	}else if(this.inputType.getText().equals("Recordset")){
    		//require recordset
    		if(this.recordset.getText().equals("")){
    			isValid = false;
        		errors += "\"Recordset Name\" is a Required field for \"Input Type\" Recordset!\r\n";
    		}
    		
    		try{
    		//if File then require type
    			
    		if(!this.outputType.isDisposed() && this.outputType.getText() != null && this.outputType.getText().equalsIgnoreCase("Piped")){
    			if(!this.typeOptions.isDisposed() && this.typeOptions.getText().equals("")){
    				//make sure you have minimum options
    				isValid = false;
            		errors += "\"Pipe Options\" is Required for \"Type\" Piped!\r\n";
    			}
    		}
    		}catch(Exception e){
    			
    		}
    	}else{
    		//error
    		isValid = false;
    		errors += "\"Input Type\" can not be blank!\r\n";
    	}
    	//System.out.println(this.outputType.getText() + " | " + this.typeOptions.getText());
    	
    	if(!isValid){
    		ErrorNotices en = new ErrorNotices();
    		errors += "If you continue to save with Errors/Warnings you may encounter compile errors.\r\n\r\n";
    		
    		isValid = en.openValidateDialog(getParent(),errors);
    	}
    	return isValid;
    	
    }
    
    private void ok() {
    	if(!validate()){
    		return;
    	}else{
	        jobEntry.setName(jobEntryName.getText());
	        AutoPopulate ap = new AutoPopulate();
	        String serverHost = "";
	        String serverPort = "";
	            try{
	            //Object[] jec = this.jobMeta.getJobCopies().toArray();
	                 
	                serverHost = ap.getGlobalVariable(this.jobMeta.getJobCopies(),"server_ip");
	                serverPort = ap.getGlobalVariable(this.jobMeta.getJobCopies(),"server_port");
	            }catch (Exception e){
	                System.out.println("Error Parsing existing Global Variables ");
	                System.out.println(e.toString());
	                
	            }
	            
	////        jobEntry.setServerAddress(serverHost);
	////        jobEntry.setServerPort(serverPort);
	        
	        if(this.recordset != null && !this.recordset.isDisposed()){
	            jobEntry.setRecordset(this.recordset.getText());
	        }else{
	            jobEntry.setRecordset("");
	        }
	        if(this.isDef != null && !this.isDef.isDisposed()){
	            jobEntry.setIsDef(this.isDef.getText());
	        }else{
	            jobEntry.setIsDef("");
	        }
	        if(this.inputType != null && !this.inputType.isDisposed()){
	            jobEntry.setInputType(this.inputType.getText());
	        }else{
	            jobEntry.setInputType("");
	        }
	        //if(this.includeFormat != null && !this.includeFormat.isDisposed()){
	        //    jobEntry.setIncludeFormat(this.includeFormat.getText());
	        //}else{
	        //    jobEntry.setIncludeFormat("");
	        //}
	        if(this.outputType != null && !this.outputType.isDisposed()){
	            jobEntry.setOutputType(this.outputType.getText());
	        }else{
	            jobEntry.setOutputType("");
	        }
	        if(this.outputFormat != null && !this.outputFormat.isDisposed()){
	            jobEntry.setOutputFormat(this.outputFormat.getText());
	        }else{
	            jobEntry.setOutputFormat("");
	        }
	        if(this.expression != null && !this.expression.isDisposed()){
	            jobEntry.setExpression(this.expression.getText());
	        }else{
	            jobEntry.setExpression("");
	        }
	        if(this.file != null && !this.file.isDisposed()){
	            jobEntry.setFile(this.file.getText());
	        }else{
	            jobEntry.setFile("");
	        }
	        if(this.typeOptions != null && !this.typeOptions.isDisposed()){
	            jobEntry.setTypeOptions(this.typeOptions.getText());
	        }else{
	            jobEntry.setTypeOptions("");
	        }
	        if(this.fileOptions != null && !this.fileOptions.isDisposed()){
	            jobEntry.setFileOptions(this.fileOptions.getText());
	        }else{
	            jobEntry.setFileOptions("");
	        }
	        if(this.named != null && !this.named.isDisposed()){
	            jobEntry.setNamed(this.named.getText());
	        }else{
	            jobEntry.setNamed("");
	        }
	        if(this.extend != null && !this.extend.isDisposed()){
	            jobEntry.setExtend(this.extend.getText());
	        }else{
	            jobEntry.setExtend("");
	        }
	        if(this.returnAll != null && !this.returnAll.isDisposed()){
	            jobEntry.setReturnAll(this.returnAll.getText());
	        }else{
	            jobEntry.setReturnAll("");
	        }
	        if(this.thor != null && !this.thor.isDisposed()){
	            jobEntry.setThor(this.thor.getText());
	        }else{
	            jobEntry.setThor("");
	        }
	        if(this.cluster != null && !this.cluster.isDisposed()){
	            jobEntry.setCluster(this.cluster.getText());
	        }else{
	            jobEntry.setCluster("");
	        }
	        if(this.encrypt != null && !this.encrypt.isDisposed()){
	            jobEntry.setEncrypt(this.encrypt.getText());
	        }else{
	            jobEntry.setEncrypt("");
	        }
	        if(this.compressed != null && !this.compressed.isDisposed()){
	            jobEntry.setCompressed(this.compressed.getText());
	        }else{
	            jobEntry.setCompressed("");
	        }
	        if(this.overwrite != null && !this.overwrite.isDisposed()){
	            jobEntry.setOverwrite(this.overwrite.getText());
	        }else{
	            jobEntry.setOverwrite("");
	        }
	        if(this.expire != null && !this.expire.isDisposed()){
	            jobEntry.setExpire(this.expire.getText());
	        }else{
	            jobEntry.setExpire("");
	        }
	        if(this.repeat != null && !this.repeat.isDisposed()){
	            jobEntry.setRepeat(this.repeat.getText());
	        }else{
	            jobEntry.setRepeat("");
	        }
	        if(this.pipeType != null && !this.pipeType.isDisposed()){
	            jobEntry.setPipeType(this.pipeType.getText());
	        }else{
	            jobEntry.setPipeType("");
	        }
	        
	
	
	        
	        dispose();
    	}
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }


}
