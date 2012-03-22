/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.eclguifeatures;
import org.eclipse.swt.*;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.*;
import org.eclipse.swt.layout.*;

/**
 *
 * @author ChambeJX
 */
public class ErrorNotices {
    
    private boolean validateCode;

    public boolean isValidateCode() {
        return validateCode;
    }

    public void setValidateCode(boolean validateCode) {
        this.validateCode = validateCode;
    }
    
    
     public void openValidateCodeDialog(){
        String notice = "Would you like to validate the ECL code at this time?";
        Display display = new Display ();
	//Shell shell = new Shell (display);
	//shell.pack ();
	//shell.open ();
	final Shell dialog = new Shell (display, SWT.DIALOG_TRIM);
	Label label = new Label (dialog, SWT.NONE);
	label.setText (notice);
	Button yesButton = new Button (dialog, SWT.PUSH);
	yesButton.setText ("&YES");
	
        Button noButton = new Button (dialog, SWT.PUSH);
	noButton.setText ("&NO");
        
        
        Listener yesListener = new Listener() {

            public void handleEvent(Event e) {
                validateCode = true;
                dialog.close();
            }
        };
        
         Listener noListener = new Listener() {

            public void handleEvent(Event e) {
                validateCode = false;
                dialog.close();
                
            }
        };
        
        yesButton.addListener(SWT.Selection, yesListener);
        noButton.addListener(SWT.Selection, noListener);
	
	FormLayout form = new FormLayout ();
	form.marginWidth = form.marginHeight = 8;
	dialog.setLayout (form);
	FormData yesData = new FormData ();
	yesData.top = new FormAttachment (label, 8);
	yesButton.setLayoutData (yesData);
	//FormData cancelData = new FormData ();
        
        FormData noData = new FormData ();
        
        noData.left = new FormAttachment (yesButton, 8);
	noData.top = new FormAttachment (yesButton, 0, SWT.TOP);

	noButton.setLayoutData (noData);
	
	
	dialog.setDefaultButton (yesButton);
	dialog.pack ();
	dialog.open ();
	
	while (!dialog.isDisposed ()) {
		if (!display.readAndDispatch ()) display.sleep ();
	}
	display.dispose ();
    }
    
    
    public void openDialog(String notice, String details){
        Display display = new Display ();
	//Shell shell = new Shell (display);
	//shell.pack ();
	//shell.open ();
	final Shell dialog = new Shell (display, SWT.DIALOG_TRIM);
	Label label = new Label (dialog, SWT.NONE);
	label.setText (notice);
	Button okButton = new Button (dialog, SWT.PUSH);
	okButton.setText ("&OK");
	
        
        
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                dialog.close();
            }
        };
        
        okButton.addListener(SWT.Selection, cancelListener);
	
	FormLayout form = new FormLayout ();
	form.marginWidth = form.marginHeight = 8;
	dialog.setLayout (form);
	FormData okData = new FormData ();
	okData.top = new FormAttachment (label, 8);
	okButton.setLayoutData (okData);
	FormData cancelData = new FormData ();
        
        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
               // jobEntry.setChanged();
            }
        };
	
        Text detailsBox = this.buildMultiText(okButton, lsMod, 0, 5, dialog);
	detailsBox.setText(details);
	dialog.setDefaultButton (okButton);
	dialog.pack ();
	dialog.open ();
	
	while (!dialog.isDisposed ()) {
		if (!display.readAndDispatch ()) display.sleep ();
	}
	display.dispose ();
    }
    
    
     private Text buildMultiText(Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox) {


        // text field
        Text text = new Text(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
       // props.setLook(text);
        
        text.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        fieldFormat.height = 200;
        
        text.setLayoutData(fieldFormat);

        return text;
    }
    
}
