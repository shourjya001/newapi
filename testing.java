import org.springframework.web.client.HttpClientErrorException;

import org.springframework.web.client.RestTemplate;

import static java.lang.System.out;

import static java.sql.Types.NULL;

@Component("restClientUtility") 4 usages

public class RestClientUtility {

private static final Logger log Logger.getLogger(RestClientUtility.class.getName()); 15

@Autowired

DbeClientDao clientDao;

@Autowired

ObeclientProperties dbeclientProperties;

@Autowired

SendMaestroDataServiceImpl sendMaestroDataService;

@Autowired

private ApplicationConfig applicationConfig;

private static final Logger logger = Logger.getLogger(RestClientUtility.class

private String decompressData(byte[] compressedBytes) { 1 usage

// First, try GZIP decompression

try {

ByteArrayInputStream bis new ByteArrayInputStream(compressedBytes);

GZIPInputStream gis new GZIPInputStream(bis);

ByteArrayOutputStream bos new ByteArrayOutputStream();
ByteArrayOutputStream bos = new ByteArrayOutputStream();

m

}

byte[] buffer = new byte[1098];

int len;

while ((len = gis.read(buffer)) != -1) {

bos.write(buffer, off: 0, len);

}

gis.close();

bos.close();

return new String(bos.toByteArray(), StandardCharsets.UTF_8);

} catch (IOException e) {

log.error("GZIP decompression failed, trying Inflater: ");

System.out.println("GZIP decompression failed, trying Inflater: " + e.getMessage());

// If GZIP fails, try Inflater

try {

Inflater inflater = new Inflater (nowrap: true); // true for ZLIB header

inflater.setInput (compressedBytes);

ByteArrayOutputStream outputStream = new

ByteArrayOutputStream (compressedBytes.length);
byte[] buffer = new byte[9098];

while (!inflater.finished()) {

int count = inflater.inflate(buffer);

outputStream.write(buffer, off: 0, count);

I

}

outputStream.close();

inflater.end();

return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

} catch (DataFormatException | IOException e) {

log.error("Error decompressing content with Inflater: " + e.getMessage());

System.err.println("Error decompressing content with Inflater: " + e.getMessage());

e.printStackTrace();
// If both decompression methods fail, return the original data as a string System.out.println("Both decompression methods failed. Returning original data as string."); log.error("Both decompression methods failed. Returning original data as string."); return new String(compressedBytes, StandardCharsets.UTF_8);

m

}

//sendInternalRatingsEventsApi
public ResponseInternal sendPrimaryroleApi() throws IOException, JsonException {
    String scope = "api.get-third-parties.v1";
    String ClientId = dbeclientProperties.getMaestroClientId();
    String SecretId = dbeclientProperties.getMaestroSecretId();
    String access_token = generateSGconnectToken(scope, ClientId, SecretId);
    ResponseInternal responseObject = null;

    // static date
    String maestrodate = "?snapshotDate=2025-02-15";
    LocalDate today = LocalDate.now();
    String formattedDate = today.toString();
    // String maestrodate = "?snapshotDate="+formattedDate;

    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + access_token);
    headers.set("content-Language", "en-US");
    headers.set("Host", "maestro-search-uat.fr.world.socgen");
    headers.set("Accept", "*/*");
    headers.set("content-type", "application/json");
    headers.set("accept", "application/json");
    headers.set("Accept-Encoding", "gzip, deflate");

    HttpEntity<String> entity = new HttpEntity<>("", headers);

    System.out.println("requestObject---->" + headers);
    log.info("Making API call to Maestro API for Primary Role");

    ResponseEntity<byte[]> result = restTemplate.exchange(
            this.dbeclientProperties.getMaestrorelationshipApiUrl() + maestrodate,
            HttpMethod.GET,
            entity,
            byte[].class
    );

    int status = result.getStatusCode().value();
    if (status == NULL || status == 401 || status == 402 || status == 403
            || status == 404 || status == 500 || status == 201 || status == 501) {
        String errorMessage = "API returned status code: " + status;
        System.err.println(errorMessage);
        log.error(errorMessage);
        sendMaestroDataService.sendErrorNotification("API Error", errorMessage);
        return null;
    }

    if (status == 200) {
        System.out.println("Successfully Data received from Maestro");
        log.info("**Successfully Data received from Maestro API for Primary Role*");

        byte[] responseBody = result.getBody();
        String decompressedJson = decompressData(responseBody);
        
        if (decompressedJson == null) {
            log.error("Failed to decompress or read the response data");
            System.err.println("Failed to decompress or read the response data");
            return null;
        }

        try {
            ObjectMapper mapperObj = new ObjectMapper();
            mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
            mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            // Parse the JSON data into a list of InternalRegistrations
            List<InternalRegistrations> registrationsList = mapperObj.readValue(decompressedJson,
                    new TypeReference<List<InternalRegistrations>>() {});

            // Process the parsed data
            List<InternalRegistrations> processedRegistrations = new ArrayList<>();
            
            for (InternalRegistrations registration : registrationsList) {
                // Pad BDRID if needed
                if (registration.getEntityId() != null) {
                    String bdrid = registration.getEntityId();
                    if (bdrid.length() < 10) {
                        bdrid = String.format("%010d", Long.parseLong(bdrid));
                        registration.setEntityId(bdrid);
                    }
                }
                processedRegistrations.add(registration);
            }

            // Create the response object
            responseObject = new ResponseInternal();
            responseObject.setInternalRegistrations(processedRegistrations);
            
            // Process and print the data in the required format
            processAndPrintEntityData(processedRegistrations);


            // print to check
            if (responseObject != null) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                String jsonResponse = mapper.writeValueAsString(responseObject);
                System.out.println("Full API Response as JSON: " + jsonResponse);
                // Optionally log it as well
                log.info("Full API Response as JSON: " + jsonResponse);
            } catch (JsonProcessingException e) {
                System.err.println("Error converting response to JSON: " + e.getMessage());
                log.error("Error converting response to JSON: " + e.getMessage());
                e.printStackTrace();
            }
        }
            
            return responseObject;
        } catch (JsonProcessingException e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            log.error("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    } else {
        System.err.println("Unexpected status code: " + status);
        log.error("Unexpected status code: " + status);
    }



    return responseObject;
}

