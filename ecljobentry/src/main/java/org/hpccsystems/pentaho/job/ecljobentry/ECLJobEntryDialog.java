/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecljobentry;

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
import org.hpccsystems.eclguifeatures.*;

import java.util.HashMap;
import org.eclipse.swt.widgets.DirectoryDialog;

/**
 *
 * @author ChalaAX
 */
public class ECLJobEntryDialog extends JobEntryDialog implements JobEntryDialogInterface {

   
    
    private HashMap controls = new HashMap();

    public ECLJobEntryDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        
        
        
    }

    public JobEntryInterface open() {
    	return null;
    }

    
    
    protected Button buildButton(String strLabel, Control prevControl, 
             ModifyListener isMod, int middle, int margin, Composite groupBox){
        
            Button nButton = new Button(groupBox, SWT.PUSH | SWT.SINGLE | SWT.CENTER);
            nButton.setText(strLabel);
            props.setLook(nButton);
            //nButton.addModifyListener(lsMod)
            FormData fieldFormat = new FormData();
            
            fieldFormat.left = new FormAttachment(middle, 0);
            fieldFormat.top = new FormAttachment(prevControl, margin);
            fieldFormat.right = new FormAttachment(75, 0);
            fieldFormat.height = 25;

            nButton.setLayoutData(fieldFormat);
        
            return nButton;
            
           
    }
    protected String buildFileDialog() {
        
            //file field
            FileDialog fd = new FileDialog(shell, SWT.SAVE);

            fd.setText("Save");
            fd.setFilterPath("C:/");
            String[] filterExt = { "*.csv", ".xml", "*.txt", "*.*" };
            fd.setFilterExtensions(filterExt);
            String selected = fd.open();
            if(fd.getFileName() != ""){
                return fd.getFilterPath() + System.getProperty("file.separator") + fd.getFileName();
            }else{
                return "";
            }
            
        }

    protected Text buildText(String strLabel, Control prevControl,
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

    protected Text buildMultiText(String strLabel, Control prevControl,
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

    protected Combo buildCombo(String strLabel, Control prevControl,
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

    
}
