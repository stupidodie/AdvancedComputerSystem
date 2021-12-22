package com.acertainbookstore.client.workloads;

import com.acertainbookstore.business.CertainBookStore;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import java.awt.Color;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.ChartUtilities;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.chart.plot.PlotOrientation;
import java.awt.BasicStroke;
import java.io.File;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.axis.NumberAxis;

/**
 * CertainWorkload class runs the workloads by different workers concurrently. It configures the
 * environment for the workers using WorkloadConfiguration objects and reports the metrics
 *
 *
 * Note: In this code file, we use the modification for the main function and runWorkers function
 *      from https://github.com/silvanadrian/ACS2018.
 */

public class CertainWorkload {

    private static int numConcurrentWorkloadThreads;


    /**
     * The main function is from https://github.com/silvanadrian/ACS2018.
     */
    public static void main(String[] args) throws Exception {
        numConcurrentWorkloadThreads = 10;
        String serverAddress = "http://localhost:8081";

        CertainBookStore store = new CertainBookStore();
        List<List<WorkerRunResult>> localResults = runWorkers(store, store);

        // Bookstore HTTP Server needs to run otherwise won't be able to run Certain Workload
        StockManagerHTTPProxy stockManager = new StockManagerHTTPProxy(serverAddress + "/stock");
        BookStoreHTTPProxy bookStore = new BookStoreHTTPProxy(serverAddress);
        List<List<WorkerRunResult>> rpcResults = runWorkers(bookStore, stockManager);

        // Finished initialization, stop the clients if not localTest
        bookStore.stop();
        stockManager.stop();

        reportMetric(localResults, rpcResults);
    }
    /**
     * The runWorkers function is from https://github.com/silvanadrian/ACS2018.
     */
    private static List<List<WorkerRunResult>> runWorkers(BookStore bookstore,
                                                          StockManager stockmanager)
            throws Exception {
        List<List<WorkerRunResult>> totalWorkersRunResults = new ArrayList<>();

        // Generate data in the bookstore before running the workload
        initializeBookStoreData(stockmanager);

        ExecutorService exec = Executors.newFixedThreadPool(numConcurrentWorkloadThreads);

        //Run experiment numConcurrentWorkloadThreads times
        for (int i = 0; i < numConcurrentWorkloadThreads; i++) {
            List<Future<WorkerRunResult>> runResults = new ArrayList<>();
            List<WorkerRunResult> workerRunResults = new ArrayList<>();

            // run experiment with i workers and save result
            for (int j = 0; j <= i; j++) {
                WorkloadConfiguration config = new WorkloadConfiguration(bookstore, stockmanager);
                Worker workerTask = new Worker(config);

                // Keep the futures to wait for the result from the thread
                runResults.add(exec.submit(workerTask));
            }

            // Get the results from the threads using the futures returned
            for (Future<WorkerRunResult> futureRunResult : runResults) {
                WorkerRunResult runResult = futureRunResult.get(); // blocking call
                workerRunResults.add(runResult);
            }

            //Add the experiment data to the results
            totalWorkersRunResults.add(workerRunResults);
            stockmanager.removeAllBooks();
        }

        exec.shutdownNow(); // shutdown the executor
        return totalWorkersRunResults;
    }

    /**
     * Computes the metrics and prints them
     */
    public static void reportMetric(List<List<WorkerRunResult>> totalWorkersRunResults,
                                    List<List<WorkerRunResult>> rpcResults) throws IOException {

        //get local metrics
        List<Double> localLatency = new ArrayList<>();
        List<Double> localThroughput = new ArrayList<>();
        getMetricResults(totalWorkersRunResults, localLatency, localThroughput);

        //get rpc metrics
        List<Double> remoteLatency = new ArrayList<>();
        List<Double> remoteThroughput = new ArrayList<>();
        getMetricResults(rpcResults, remoteLatency, remoteThroughput);


        XYDataset dataset1 = create_Chart("Latency", localLatency, remoteLatency);
        JFreeChart latency_chart = ChartFactory.createScatterPlot(
                "Latency Test","Number of threads","nanoseconds",dataset1,
                PlotOrientation.VERTICAL,true,true, false);         // urls
        XYPlot plot = (XYPlot)latency_chart.getPlot();
        NumberAxis rangeAxis = new LogarithmicAxis("nanoseconds");
        rangeAxis.setAutoRange(true);
        rangeAxis.setTickMarksVisible(false);
        rangeAxis.setAxisLineVisible(false);
        plot.setRangeAxis(0,rangeAxis);
        plot.setBackgroundPaint(new Color(255,228,196));
        var renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesPaint(0, Color.RED);
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer.setSeriesPaint(1, Color.BLUE);
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));
        plot.setRenderer(renderer);
        ChartUtilities.saveChartAsPNG(new File("Latency.png"), latency_chart, 450, 400);

        XYDataset dataset2 = create_Chart("Throughput", localThroughput, remoteThroughput);
        JFreeChart throughput_chart = ChartFactory.createScatterPlot(
                "Throughput Test","Number of threads","Successful interactions per ns",dataset2,
                PlotOrientation.VERTICAL,true,true, false);         // urls
        XYPlot plot2 = (XYPlot)throughput_chart.getPlot();
        NumberAxis rangeAxis2 = new LogarithmicAxis("Successful interactions per ns");
        rangeAxis2.setAutoRange(true);
        rangeAxis2.setTickMarksVisible(false);
        rangeAxis2.setAxisLineVisible(false);
        plot2.setRangeAxis(0,rangeAxis2);
        plot2.setBackgroundPaint(new Color(255,228,196));
        var renderer2 = new XYLineAndShapeRenderer();
        renderer2.setSeriesPaint(0, Color.RED);
        renderer2.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer2.setSeriesPaint(1, Color.BLUE);
        renderer2.setSeriesStroke(0, new BasicStroke(2.0f));
        plot2.setRenderer(renderer2);
        ChartUtilities.saveChartAsPNG(new File("Throughput.png"), throughput_chart, 450, 400);


    }

    private static XYSeriesCollection create_Chart(String title, List<Double> localData,
                                                   List<Double> remoteData){
        double[] xLabels = IntStream.rangeClosed(1, numConcurrentWorkloadThreads).asDoubleStream()
                .toArray();
        double[] local_y = localData.stream().mapToDouble(Double::doubleValue).toArray();
        double[] remote_y = remoteData.stream().mapToDouble(Double::doubleValue).toArray();
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries series1 = new XYSeries("local");
        XYSeries series2 = new XYSeries("remote");

        for (int i = 0; i <xLabels.length ; i++) {
            series1.add(xLabels[i],local_y[i]);
            series2.add(xLabels[i],remote_y[i]);
        }

        dataset.addSeries(series1);
        dataset.addSeries(series2);
        return dataset;
    }

    private static void getMetricResults(List<List<WorkerRunResult>> workerRunResults,
                                         List<Double> latencyList, List<Double> throuputList) {
        long totalRunTime = 0;
        double aggregatedThroughPut = 0;
        double successfulInteractions;
        double elapsedTimeInNanoSecs;
        for (List<WorkerRunResult> workerRunResultList : workerRunResults) {
            for (WorkerRunResult workerRunResult : workerRunResultList) {
                successfulInteractions = workerRunResult.getSuccessfulInteractions();
                elapsedTimeInNanoSecs = workerRunResult.getElapsedTimeInNanoSecs();
                aggregatedThroughPut += successfulInteractions / elapsedTimeInNanoSecs;
                totalRunTime += workerRunResult.getElapsedTimeInNanoSecs();
            }
            double averageLatency = totalRunTime/ (double) workerRunResults.size();

            latencyList.add(averageLatency);
            throuputList.add(aggregatedThroughPut);
        }
    }

    /**
     * Generate the data in bookstore before the workload interactions are run
     *
     * Ignores the serverAddress if its a localTest
     */
    public static void initializeBookStoreData(StockManager stockManager) throws BookStoreException {

        // use the BookSet generator for generating random books (1000)
        BookSetGenerator bookSetGenerator = new BookSetGenerator();
        Set<StockBook> stockBookSet = bookSetGenerator.nextSetOfStockBooks(10);

        //remove all books before, to be sure only the generated books are included
        stockManager.removeAllBooks();
        stockManager.addBooks(stockBookSet);
    }
}
