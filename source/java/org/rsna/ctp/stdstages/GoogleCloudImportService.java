/*---------------------------------------------------------------
 *  Copyright 2015 by the Radiological Society of North America
 *
 *  This source software is released under the terms of the
 *  RSNA Public License (http://mirc.rsna.org/rsnapubliclicense.pdf)
 *----------------------------------------------------------------*/

package org.rsna.ctp.stdstages;

import com.codeminders.demo.DICOMStoreDescriptor;
import com.codeminders.demo.GoogleAPIClient;
import com.codeminders.demo.GoogleAPIClientFactory;
import org.apache.log4j.Logger;
import org.rsna.ctp.pipeline.AbstractImportService;
import org.w3c.dom.Element;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An ImportService that import data from GCP Healthcare DICOM Stores. Import
 * service pulls DICOM objects from selected projectID/DICOMStore and transfers
 * them to next stage of the pipeline.
 */
public class GoogleCloudImportService extends AbstractImportService {

    static final Logger logger = Logger.getLogger(GoogleCloudImportService.class);

    Poller poller;
    long interval = 20000;
    File importDirectory;
    ExecutorService executorService;

    private DICOMStoreDescriptor dicomStoreDecriptor;
    private GoogleAPIClient googleClient;
    private int MAX_THREADS = 5; //num of threads used to pull data from Google in parallel

    /**
     * Class constructor; creates a new instance of the ImportService.
     *
     * @param element the configuration element.
     * @throws Exception on any error
     */
    public GoogleCloudImportService(Element element) throws Exception {
        super(element);

        String directoryName = element.getAttribute("import").trim();
        importDirectory = getDirectory(directoryName);
        if ((importDirectory == null) || !importDirectory.exists()) {
            logger.error(name + ": The import directory was not specified.");
            throw new Exception(name + ": The import directory was not specified.");
        }

        String projectId = element.getAttribute("projectId").trim();
        String locationId = element.getAttribute("locationId").trim();
        String dataSetName = element.getAttribute("dataSetName").trim();
        String dicomStoreName = element.getAttribute("dicomStoreName").trim();

        dicomStoreDecriptor = new DICOMStoreDescriptor(projectId, locationId, dataSetName, dicomStoreName);

        googleClient = GoogleAPIClientFactory.getInstance().getGoogleClient();
    }

    /**
     * Start the service. This method can be overridden by stages which can use it
     * to start subordinate threads created in their constructors. This method is
     * called by the Pipeline after all the stages have been constructed.
     */
    @Override
    public synchronized void start() {
        executorService = Executors.newFixedThreadPool(MAX_THREADS);
        poller = new Poller();
        poller.start();
    }

    /**
     * Stop the service.
     */
    @Override
    public synchronized void shutdown() {

        if (executorService != null) {
            executorService.shutdownNow();
        }
        
        if (poller != null)
            poller.interrupt();
        super.shutdown();
    }

    private static String readLine(InputStream inputStream) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int c;
        for (c = inputStream.read(); c != '\n' && c != -1; c = inputStream.read()) {
            byteArrayOutputStream.write(c);
        }
        if (c == -1 && byteArrayOutputStream.size() == 0) {
            return null;
        }
        String line = byteArrayOutputStream.toString("UTF-8");
        return line;
    }

    public void downloadFile(String fileURL, String saveDir) throws Exception {
        if (!googleClient.isSignedIn()) {
            googleClient.signIn();
        }

        URL url = new URL(fileURL);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setRequestProperty("Authorization", "Bearer " + googleClient.getAccessToken());
        httpConn.setRequestProperty("Accept", "multipart/related; type=application/dicom; transfer-syntax=*");
        int responseCode = httpConn.getResponseCode();

        // always check HTTP response code first
        if (responseCode == HttpURLConnection.HTTP_OK) {
            String fileName = "";
            String disposition = httpConn.getHeaderField("Content-Disposition");

            if (disposition != null) {
                // extracts file name from header field
                int index = disposition.indexOf("filename=");
                if (index > 0) {
                    fileName = disposition.substring(index + 10, disposition.length() - 1);
                }
            } else {
                // extracts file name from URL
                fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1);
            }

            logger.info("Starting download file = " + fileName);

            // opens input stream from the HTTP connection
            InputStream inputStream = httpConn.getInputStream();

            readLine(inputStream);
            readLine(inputStream);
            readLine(inputStream);

            String saveFilePath = saveDir + File.separator + fileName;

            // opens an output stream to save into file
            File file = new File(saveFilePath);
            FileOutputStream outputStream = new FileOutputStream(file);

            int bytesRead = -1;
            byte[] buffer = new byte[1024];
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            outputStream.close();
            inputStream.close();
            fileReceived(file);
            logger.info("File downloaded");

            ReportService.getInstance().addDownloaded(fileURL);

        } else {
            logger.info("No file to download. Server replied HTTP code: " + responseCode);
        }
        httpConn.disconnect();
    }

    public void downloadFile(List<String> fileURLs, String saveDir) throws Exception {
        for (String url : fileURLs) {
            executorService.submit(() -> {
                try {
                    downloadFile(url, saveDir);
                } catch (Exception e) {
                    logger.error("Cannot import DICOM file " + url, e);
                    e.printStackTrace();
                }
            });
        }
    }

    class Poller extends Thread {

        public Poller() {
            super("CloudImport poller ");
        }

        @Override
        public void run() {
            try {
                downloadFile(googleClient.listDCMFileIds(dicomStoreDecriptor), getTempDirectory().getAbsolutePath());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            while (!isInterrupted()) {
                //implement periodic polling of DICOM store and
                try {
                    sleep(interval);
                } catch (InterruptedException ignore) {
                    logger.info("Interrupt received. Exiting GoogleCloudImportService poller...");
                    return;
                }
            }
            logger.info("Interrupt received. Exiting GoogleCloudImportService poller...");
        }
    }
}