package org.hpccsystems.pentaho.job.eclindex;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.hpccsystems.eclguifeatures.RecordBO;
import org.hpccsystems.eclguifeatures.RecordList;
import org.junit.Test;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.PropsUI;

/**
 * This test does NOT run when unit tests are executed via Maven.
 * It only executes via JUnit when run "manually", from the IDE.
 * 
 * Running this unit test allows the UI component to be exercised
 * and tested in isolation, apart from the Spoon UI.
 * 
 * @author krumlamj
 */
public class ECLIndexDialogTest_Manual {

    @Test
    public void must_display_and_validate() throws KettleException {
        PropsUI.init(new Display(), 0);
        
        final ECLIndex eclIndex = new ECLIndex();
        
        eclIndex.setKeys(newRecordListWith("key1", "key2"));
        eclIndex.setPayload(newRecordListWith("payload1", "payload2", "payload3"));
        
        final ECLIndexDialog testInstance =
                new ECLIndexDialog(new Shell(), eclIndex, (Repository) null,
                        new JobMeta());

        testInstance.open();
        
        // Visually compare trace output to selected values in dialog
        eclIndex.execute(new Result(), 1);
    }

    private RecordList newRecordListWith(String... values) {
        final RecordList result = new RecordList();
        
        for (String value : values) {
            result.addRecordBO(new RecordBO(value));
        }
        return result;
    }
}
