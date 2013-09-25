package org.hpccsystems.pentaho.job.eclsort;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.CheckboxCellEditor;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.hpccsystems.sortui.table.SortColumnRecordList;
import org.hpccsystems.sortui.table.SortColumnTable;




import com.hpccsystems.ui.constants.Constants;



public class ECLSortUIComponents {
	
	public static void main(String[] args) {
		Display display = new Display();
	    Shell shell = new Shell(display);
	    
	    /*
	    ConceptEntryBO crb = new ConceptEntryBO();
	    crb.setConceptName("test");
	    crb.setEffectOnSpecificity("test2");
	    crb.setUseBagOfWords(true);
	    
	    List<String> test = new ArrayList<String>();
	    test.add("myfield");
	    test.add("myfield2");
	    test.add("field2");
	    test.add("field3");
	    */
	    SortColumnRecordList slist = new SortColumnRecordList();
	    SortColumnTable stable = new SortColumnTable(shell,slist);
	    shell.setText("sort test");
	    shell.setSize(800, 550);
	    GridLayout layout = new GridLayout();
	    layout.numColumns = 3;
	    layout.marginLeft = 10;
	    layout.marginRight = 10;
	    layout.makeColumnsEqualWidth = true;
	    shell.setLayout(layout);
	    Composite test = new Composite(shell,SWT.NONE);
	    stable.addChildControls(test);
	    shell.open();
	    
		while (!shell.isDisposed()) {
		      if (!display.readAndDispatch())
		        display.sleep();
		    }
		    display.dispose();
	}
	
	
}
