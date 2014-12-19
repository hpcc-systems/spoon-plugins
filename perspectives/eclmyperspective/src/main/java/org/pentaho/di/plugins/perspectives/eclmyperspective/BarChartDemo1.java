package org.pentaho.di.plugins.perspectives.eclmyperspective;



import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.ImageLoader;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.swtchart.Chart;
import org.swtchart.IBarSeries;
import org.swtchart.ISeries.SeriesType;

/**
 * An example for bar chart.
 */
public class BarChartDemo1 {
	static Chart chart;
    private static final double[] ySeries = { 0.2, 1.1, 1.9, 2.3, 1.8, 1.5,
            1.8, 2.6, 2.9, 3.2 };

    /**
     * The main method.
     * 
     * @param args
     *            the arguments
     */
    public static void main(String[] args) {
        Display display = new Display();
        Shell shell = new Shell(display);
        shell.setText("Bar Chart");
        shell.setSize(500, 400);
        shell.setLayout(new FillLayout());
        chart = new Chart(shell, SWT.NONE);
        createChart(shell);
        
        System.out.println("Now Save");
        
        Image image = new Image(display, shell.getBounds().width, shell.getBounds().height);
        GC gc=new GC(image);
        gc.drawImage(image, shell.getBounds().width, shell.getBounds().height);
        
        shell.setImage(image);
        ImageData data = image.getImageData();
        System.out.println(data.getRGBs());
        //System.out.println(image); 
        ImageLoader loader = new ImageLoader();
        loader.data = new ImageData[] { data };
        loader.save("D:/Users/703119704/Desktop/fotu.jpg", SWT.IMAGE_JPEG);
        //image.dispose();

        shell.open();
        //shell.pack();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        display.dispose();
    }

    /**
     * create the chart.
     * 
     * @param parent
     *            The parent composite
     * @return The created chart
     */
    static public Chart createChart(Composite parent) {

        // create a chart
        

        // set titles
        chart.getTitle().setText("Bar Chart");
        chart.getAxisSet().getXAxis(0).getTitle().setText("Data Points");
        chart.getAxisSet().getYAxis(0).getTitle().setText("Amplitude");

        // create bar series
        IBarSeries barSeries = (IBarSeries) chart.getSeriesSet().createSeries(
                SeriesType.BAR, "bar series");
        barSeries.setYSeries(ySeries);

        // adjust the axis range
        chart.getAxisSet().adjustRange();
        
        
        
        
        return chart;
    }
}