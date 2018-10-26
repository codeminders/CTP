package org.rsna.ctp.stdstages;

import com.codeminders.demo.DICOMGoogleClientHttpRequest;
import com.codeminders.demo.DICOMStoreDescriptor;
import com.codeminders.demo.GoogleAPIClient;
import com.codeminders.demo.GoogleAPIClientFactory;
import org.apache.log4j.Logger;
import org.rsna.ctp.objects.FileObject;
import org.rsna.ctp.pipeline.AbstractExportService;
import org.rsna.ctp.pipeline.Status;
import org.rsna.server.HttpResponse;
import org.rsna.util.FileUtil;
import org.rsna.util.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An ExportService that exports files via the DICOM STOW-RS protocol.
 */
public class GoogleCloudExportService extends AbstractExportService {

	static final Logger logger = Logger.getLogger(GoogleCloudExportService.class);

	static final int oneSecond = 1000;
	final int connectionTimeout = 20 * oneSecond;
	final int readTimeout = 120 * oneSecond;

	boolean includeContentDispositionHeader = false;
	boolean logUnauthorizedResponses = true;

	private DICOMStoreDescriptor dicomStoreDecriptor;
	private ExecutorService executorService;
	private int MAX_THREADS = 5;
	private BlockingQueue<File> exportQueue;
	private GoogleCloudExporter googleCloudExporter;

	/**
	 * Class constructor; creates a new instance of the GoogleCloudExportService.
	 *
	 * @param element the configuration element.
	 */
	public GoogleCloudExportService(Element element) {
		super(element);

		// Get the flag for including the Content-Disposition header in requests
		includeContentDispositionHeader = element.getAttribute("includeContentDispositionHeader").trim().toLowerCase().equals("yes");

		String projectId = element.getAttribute("projectId").trim();
		String locationId = element.getAttribute("locationId").trim();
		String dataSetName = element.getAttribute("dataSetName").trim();
		String dicomStoreName = element.getAttribute("dicomStoreName").trim();

		dicomStoreDecriptor = new DICOMStoreDescriptor(projectId, locationId, dataSetName, dicomStoreName);
		FileUtil.deleteAll(getTempDirectory());
	}

	@Override
	public synchronized void start() {
		//Get the AuditLog plugin, if there is one.
		executorService = Executors.newFixedThreadPool(MAX_THREADS);
		exportQueue = new LinkedBlockingQueue<>();
		googleCloudExporter = new GoogleCloudExporter();
		googleCloudExporter.start();
	}


	@Override
	public synchronized void shutdown() {
		if (executorService != null) {
			executorService.shutdownNow();
		}
		if(googleCloudExporter != null) {
			googleCloudExporter.interrupt();
		}
		super.shutdown();
	}

	@Override
	public int getQueueSize() {
		return exportQueue.size();
	}

	@Override
	public synchronized boolean isDown() {
		return stop;
	}

	@Override
	public Status export(File file) {
		return Status.OK;
	}

	/**
	 * Export a FileObject.
	 *
	 * @param fileObject the file to export.
	 */
	@Override
	public void export(FileObject fileObject) {
		File f = fileObject.copyToDirectory(getTempDirectory());
		logger.info("Adding fileObject to export queue: " + f);
		exportQueue.add(f);
	}

	private void exportToGCP(File fileToExport) {
		// Do not export zero-length files
		long fileLength = fileToExport.length();
		if (fileLength == 0) {
			reportStatus(fileToExport, Status.FAIL);
			return;
		}

		try {
			GoogleAPIClient apiClient = GoogleAPIClientFactory.getInstance().getGoogleClient();
			apiClient.signIn();

			// Establish the connection
			URL url = new URL(apiClient.getGHCDicomstoreUrl(dicomStoreDecriptor) + "/dicomWeb/studies");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setReadTimeout(connectionTimeout);
			conn.setConnectTimeout(readTimeout);
			conn.setRequestProperty("Authorization", "Bearer " + apiClient.getAccessToken());

			// Send the file to the server
			DICOMGoogleClientHttpRequest req = new DICOMGoogleClientHttpRequest(conn,
					"multipart/related; type=application/dicom;");
			if (!includeContentDispositionHeader) {
				req.addFilePart(fileToExport, "application/dicom");
			} else {
				String ctHeader = "Content-Type: application/dicom";
				String cdHeader = "Content-Disposition: form-data; name=\"stowrs\"; filename=\""
						+ fileToExport.getName() + "\";";
				String[] headers = { cdHeader, ctHeader };
				req.addFilePart(fileToExport, headers);
			}
			InputStream is = req.post();
			String response = FileUtil.getText(is, "UTF-8");
			conn.disconnect();

			logger.info("POST file("+fileToExport.getAbsolutePath()+") to dicomstore("+url+"). Response:" + response);
			
			// Get the response code and log Unauthorized responses
			int responseCode = conn.getResponseCode();
			if (logger.isDebugEnabled()) {
				try {
					Document doc = XmlUtil.getDocument(response);
					response = XmlUtil.toPrettyString(doc);
				} catch (Exception ex) {
					logger.error("Error during log formatting", ex);
				}
				logger.debug(name + ": Response code: " + responseCode);
				logger.debug(name + ": XML Response Message:\n" + response);
			}
			
			if (responseCode == HttpResponse.unauthorized) {
				if (logUnauthorizedResponses) {
					logger.warn("Unauthorized for " + url);
					logUnauthorizedResponses = false;
				}
				conn.disconnect();
				 reportStatus(fileToExport, Status.FAIL);
				return;
			} else if (responseCode == HttpResponse.forbidden) {
				if (logUnauthorizedResponses) {
					logger.warn("Forbidden for " + url);
					logUnauthorizedResponses = false;
				}
				conn.disconnect();
				 reportStatus(fileToExport, Status.FAIL);
				return;
			} else if (!logUnauthorizedResponses) {
				logUnauthorizedResponses = true;
			}

			if (responseCode == HttpResponse.ok) {
				reportStatus(fileToExport, Status.OK);
			} else {
				reportStatus(fileToExport, Status.FAIL);
			}
			fileToExport.delete();
		} catch (Exception e) {
			if (logger.isDebugEnabled()) {
				logger.debug(name + ": export failed: " + e.getMessage(), e);
			} else {
				logger.warn(name + ": export failed: " + e.getMessage());
			}
			 reportStatus(fileToExport, Status.FAIL);
		}
	}

	private Status reportStatus(File file, Status status) {
		ReportService.getInstance().addExported(file.getAbsolutePath(), status, "");
		return status;
	}

	class GoogleCloudExporter extends Thread {

		GoogleCloudExporter() {
			super("CloudExportService");
		}

		@Override
		public void run() {
			{
				try {
					GoogleAPIClient apiClient = GoogleAPIClientFactory.getInstance().getGoogleClient();
					apiClient.signIn();
					apiClient.checkDicomstore(dicomStoreDecriptor);
				} catch (Exception e) {
					logger.error("Unable to initialize CloudExportService.GoogleCloudExporter. Exiting...", e);
					throw new RuntimeException(e);
				}

				while (!isInterrupted()) {
					try {
						File file = exportQueue.take();
						executorService.submit(() -> exportToGCP(file));
					} catch (InterruptedException interrupt) {
						logger.info("Interrupt received. Exiting GoogleCloudExportService googleCloudExporter...");
						return;
					}
				}
				logger.info("Interrupt received. Exiting GoogleCloudExportService googleCloudExporter...");
			}
		}
	}
}