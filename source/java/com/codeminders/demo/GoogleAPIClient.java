package com.codeminders.demo;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.*;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.ListProjectsResponse;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Tokeninfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;
import org.rsna.util.FileUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GoogleAPIClient {
    private final static Logger logger = Logger.getLogger(GoogleAPIClient.class);
    /**
     * Be sure to specify the name of your application. If the application name is
     * {@code null} or blank, the application will log a warning. Suggested format
     * is "MyCompany-ProductName/1.0".
     */
    private static final String APPLICATION_NAME = "Codeminders-MIRCCTPDemo/1.0";

    private static final String GDRIVE_REPORT_FOLDER_NAME = "De-id";
    
    /**
     * Directory to store user credentials.
     */
    private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".store/google_mirc_auth");
    private static final String GCP_SECRETS_PROPERTY_NAME = "gcpsecrets";
    private static final String DEFAULT_SECRETS_FILE_NAME = "client_secrets.json";
    private static final String GCP_PROPERTIES_FILE_NAME = "gcp.properties";

    /**
     * Global instance of the {@link DataStoreFactory}. The best practice is to make
     * it a single globally shared instance across your application.
     */
    private static DataStoreFactory dataStoreFactory;

    /**
     * Global instance of the HTTP transport.
     */
    private static HttpTransport httpTransport;

    /**
     * Global instance of the JSON factory.
     */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    /**
     * OAuth 2.0 scopes.
     */
    private static final List<String> SCOPES = Arrays.asList(
            "https://www.googleapis.com/auth/cloud-healthcare",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.appdata",
            "https://www.googleapis.com/auth/drive.apps.readonly",
            "https://www.googleapis.com/auth/drive.file");

    private static Oauth2 oauth2;
    private static GoogleClientSecrets clientSecrets;

    /**
     * Instance of Google Cloud Resource Manager
     */
    private static CloudResourceManager cloudResourceManager;
    private static Drive drive;

    private boolean isSignedIn = false;
    private String accessToken;
    
    private List<GoogleAuthListener> listeners = new ArrayList<>();
    
    private SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyyMMdd_HHmm");

    protected GoogleAPIClient() {
    }
    
    public void cleanAuth() {
    	if (!isSignedIn()) {
    		FileUtil.deleteAll(DATA_STORE_DIR);
    	}
    }

    /**
     *  Read command-line properties and save it to gcp.properties file.
     *  So this properties can be later used by other modules of CTP application
     */
    public static void saveProperties() {

        Properties appProps = new Properties();
        //if property specified, save it to gcp.properties file
        String secretsLocation = System.getProperty(GCP_SECRETS_PROPERTY_NAME, DEFAULT_SECRETS_FILE_NAME);
        appProps.setProperty(GCP_SECRETS_PROPERTY_NAME, secretsLocation);

        try (FileOutputStream out = new FileOutputStream(GCP_PROPERTIES_FILE_NAME)) {
            appProps.store(out, "Google Cloud properties");
        } catch (IOException e) {
            logger.error("Cannot write properties file ", e);
            throw new RuntimeException(e);
        }

    }

    private static Credential authorize() throws Exception {
        //read read value of gcpsecrets property property from gcp.properties file
        Properties appProps = new Properties();
        appProps.load(Files.newInputStream(Paths.get(GCP_PROPERTIES_FILE_NAME).toAbsolutePath()));
        String secretsFileName = appProps.getProperty(GCP_SECRETS_PROPERTY_NAME, DEFAULT_SECRETS_FILE_NAME);
        // load client secrets
        Path secrets = Paths.get(secretsFileName).toAbsolutePath();
        clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(Files.newInputStream(secrets)));
        if (clientSecrets.getDetails().getClientId().startsWith("Enter")
                || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
            System.out.println("Generate Client ID and Secret using https://code.google.com/apis/console/ "
                    + "and place client_secrets.json to CTP root folder");
            System.exit(1);
        }
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY,
                clientSecrets, SCOPES).setDataStoreFactory(dataStoreFactory).build();
        // authorize
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    }
    
    public void addListener(GoogleAuthListener listener) {
    	listeners.add(listener);
    }
    
    public void removeListener(GoogleAuthListener listener) {
    	listeners.remove(listener);
    }

    public void signIn() throws Exception {
    	if (!isSignedIn) {
    		try {
    			signInInternal();
    		} catch(Exception e) {
        		DATA_STORE_DIR.delete();
        		signInInternal();
    		}
    	}
    }

    private void signInInternal() throws Exception {
        if (!isSignedIn) {
            int tryCount = 0;
            Exception error;
            do {
                try {
                    tryCount++;
                    httpTransport = GoogleNetHttpTransport.newTrustedTransport();
                    dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR); //new MemoryDataStoreFactory();
                    // authorization
                    Credential credential = authorize();
                    // set up global Oauth2 instance
                    oauth2 = new Oauth2.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
                            .build();

                    cloudResourceManager = new CloudResourceManager.Builder(httpTransport, JSON_FACTORY, credential)
                            .build();
                    drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
                    		.build();
                    accessToken = credential.getAccessToken();
                    System.out.println("Token:" + accessToken);
                    // run commands
                    tokenInfo(accessToken);
                    error = null;
                    isSignedIn = true;
                    for (GoogleAuthListener listener: listeners) {
                    	listener.authorized();
                    }
                } catch (Exception e) {
                    logger.error("Error occurred during authorization", e);
                    error = e;
                    e.printStackTrace();
                    logger.info("Retry authorization");
                    System.out.println("Retry authorization:");
                }
            } while (!isSignedIn && tryCount < 4);
            if (error != null) {
                throw new IllegalStateException(error);
            }
        }
    }
    
    public boolean isSignedIn() {
		return isSignedIn;
	}
    
    public String getAccessToken() {
		return accessToken;
	}

    private static void tokenInfo(String accessToken) throws IOException {
        System.out.println("Validating token");
        Tokeninfo tokeninfo = oauth2.tokeninfo().setAccessToken(accessToken).execute();
        System.out.println(tokeninfo.toString());
        if (!tokeninfo.getAudience().equals(clientSecrets.getDetails().getClientId())) {
            System.err.println("ERROR: audience does not match our client ID!");
        }
    }

    public List<ProjectDescriptor> fetchProjects() throws Exception {
        signIn();
        List<ProjectDescriptor> result = new ArrayList<ProjectDescriptor>();
        CloudResourceManager.Projects.List request = cloudResourceManager.projects().list();
        ListProjectsResponse response;
        do {
            response = request.execute();
            if (response.getProjects() == null) {
                continue;
            }
            for (Project project : response.getProjects()) {
                result.add(new ProjectDescriptor(project.getName(), project.getProjectId()));
            }
            request.setPageToken(response.getNextPageToken());
        } while (response.getNextPageToken() != null);
        return result;
    }

    private String parseName(String name) {
        return name.substring(name.lastIndexOf("/") + 1);
    }

    private HttpResponse googleRequest(String url) throws Exception {
        signIn();
        System.out.println("Google request url:" + url);
        HttpRequest request = httpTransport.createRequestFactory().buildGetRequest(new GenericUrl(url));
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", Arrays.asList(new String[]{"Bearer " + accessToken}));
        request.setHeaders(headers);
        return request.execute();
    }
    
    private HttpResponse googlePostRequest(String url, HttpContent content) throws Exception {
        signIn();
        System.out.println("Google request url:" + url);
        HttpRequest request = httpTransport.createRequestFactory().buildPostRequest(new GenericUrl(url), content);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", Arrays.asList(new String[]{"Bearer " + accessToken}));
        request.setHeaders(headers);
        return request.execute();
    }

    public List<Location> fetchLocations(String projectId) throws Exception {
        signIn();
        String url = "https://healthcare.googleapis.com/v1alpha/projects/" + projectId + "/locations";
        String data = googleRequest(url).parseAsString();
        JsonParser parser = new JsonParser();
        JsonElement jsonTree = parser.parse(data);
        JsonArray jsonObject = jsonTree.getAsJsonObject().get("locations").getAsJsonArray();
        List<Location> list = StreamSupport.stream(jsonObject.spliterator(), false)
                .map(obj -> obj.getAsJsonObject())
                .map(obj -> {
                    return new Location(obj.get("name").getAsString(), obj.get("locationId").getAsString());
                })
                .collect(Collectors.toList());
        return list;
    }

    public List<String> fetchDatasets(String projectId, String locationId) throws Exception {
        signIn();
        String url = "https://healthcare.googleapis.com/v1alpha/projects/" + projectId + "/locations/" + locationId + "/datasets";
        String data = googleRequest(url).parseAsString();
        JsonParser parser = new JsonParser();
        JsonElement jsonTree = parser.parse(data);
        JsonArray jsonObject = jsonTree.getAsJsonObject().get("datasets").getAsJsonArray();
        return StreamSupport.stream(jsonObject.spliterator(), false)
                .map(obj -> obj.getAsJsonObject().get("name").getAsString())
                .map(this::parseName)
                .collect(Collectors.toList());
    }

    public List<String> fetchDicomstores(String projectId, String locationId, String dataset) throws Exception {
        signIn();
        String url = "https://healthcare.googleapis.com/v1alpha/projects/" + projectId + "/locations/" + locationId + "/datasets/" + dataset + "/dicomStores";
        String data = googleRequest(url).parseAsString();
        JsonParser parser = new JsonParser();
        JsonElement jsonTree = parser.parse(data);
        JsonArray jsonObject = jsonTree.getAsJsonObject().get("dicomStores").getAsJsonArray();
        List<String> result = StreamSupport.stream(jsonObject.spliterator(), false)
                .map(obj -> obj.getAsJsonObject().get("name").getAsString())
                .map(this::parseName)
                .collect(Collectors.toList());
        return result;
    }

	public String getGHCDatasetUrl(DICOMStoreDescriptor descriptor) {
		return "https://healthcare.googleapis.com/v1alpha/projects/"+descriptor.getProjectId()+
				"/locations/"+descriptor.getLocationId()+
				"/datasets/"+descriptor.getDataSetName();
	}

	public String getGHCDicomstoreUrl(DICOMStoreDescriptor descriptor) {
		return getGHCDatasetUrl(descriptor) + "/dicomStores/"+descriptor.getDicomStoreName();
	}

	private String getDCMFileUrl(DICOMStoreDescriptor study, String dcmFileId) {
		return getGHCDicomstoreUrl(study)+"/dicomWeb/studies/"+dcmFileId;
	}

	public List<String> listDCMFileIds(DICOMStoreDescriptor descriptor) throws Exception {
		signIn();
		String url = getGHCDicomstoreUrl(descriptor)+"/dicomWeb/studies";
		String data = googleRequest(url).parseAsString();
        JsonElement jsonTree = new JsonParser().parse(data);
        List<String> imageUrls = StreamSupport.stream(jsonTree.getAsJsonArray().spliterator(), false)
        	.map(el -> el.getAsJsonObject().get("0020000D").getAsJsonObject().get("Value").getAsJsonArray().get(0).getAsString())
        	.map(id -> getDCMFileUrl(descriptor, id))
        	.collect(Collectors.toList());
        return imageUrls;
	}

	public String createDicomstore(DICOMStoreDescriptor descriptor) throws Exception {
		signIn();
		String url = getGHCDatasetUrl(descriptor)+"/dicomStores?dicomStoreId=" + descriptor.getDicomStoreName();
		String data = googlePostRequest(url, new EmptyContent()).parseAsString();
        JsonElement jsonTree = new JsonParser().parse(data);
        JsonElement errorEl = jsonTree.getAsJsonObject().get("error");
        if (errorEl != null) {
        	throw new IllegalStateException("Dicomstore save error: " + errorEl.getAsJsonObject().get("message").getAsString());
        }
        return jsonTree.getAsJsonObject().get("name").getAsString();
	}

	public void checkDicomstore(DICOMStoreDescriptor descriptor) throws Exception {
		signIn();
		List<String> dicomstores = fetchDicomstores(descriptor.getProjectId(), descriptor.getLocationId(), descriptor.getDataSetName());
		boolean isNewDicomStore = !dicomstores.contains(descriptor.getDicomStoreName());
		if (isNewDicomStore) {
			String dicomStorePath = createDicomstore(descriptor);
			logger.info("DICOM store created: " + dicomStorePath);
		}
	}

	private String findFolder(String folderName) throws IOException {
		FileList result = drive.files().list()
			.setQ("mimeType = 'application/vnd.google-apps.folder' and name = 'De-id'")
			.setSpaces("drive")
			.execute();
		if (result.getFiles().isEmpty()) {
			return null;
		}
		return result.getFiles().get(0).getId();
	}
	
	private String getParentFolder(String folderName) throws IOException {
		String folderId = findFolder(folderName);
		if (folderId == null) {
			String mimeType = "application/vnd.google-apps.folder";
			File body = new File();
			body.setName(folderName);
			body.setMimeType(mimeType);
			File file = drive.files().create(body)
			  	.setFields("id")
			  	.execute();
			folderId = file.getId();
			logger.info("Folder ID: " + folderId);
		}
		return folderId;
	}
	
	public String exportStringAsGoogleDoc(String title, String description, String localFilename) {
		if (!isSignedIn()) {
			throw new IllegalStateException("User not authorized in Google cloud");
		}
		String result;
	    try {
	    	String reportFilename = title + "_" + timeFormatter.format(new Date());
			String mimeType = "text/html";
			String parentId = getParentFolder(GDRIVE_REPORT_FOLDER_NAME);
			File body = new File();
		    body.setName(reportFilename);
		    body.setDescription(description);
		    body.setMimeType(mimeType);
		    if (parentId != null && parentId.length() > 0) {
		    	body.setParents(Arrays.asList(parentId));
		    }
		    FileContent mediaContent = new FileContent(mimeType, new java.io.File(localFilename));
		    File file = drive.files().create(body, mediaContent).execute();
		    logger.info("File ID: " + file.getId());
		    result = "\"\\" + GDRIVE_REPORT_FOLDER_NAME + "\\" + reportFilename + "\" with id: " + file.getId();
	    } catch (IOException e) {
	    	result = "An error occurred: " + e.getMessage();
	    	logger.info(result, e);
	    }
	    return result;
	}
}
