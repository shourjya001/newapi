import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.socgen.riskweb.Model.InternalRegistrations;
import com.socgen.riskweb.Model.SubBookingEntity;
import com.socgen.riskweb.Model.ResponseInternal;
import com.socgen.riskweb.Model.Registration;

import static java.lang.System.out;
import static java.sql.Types.NULL;

@Component("restClientUtility")
public class RestClientUtility {

    private static final Logger log = Logger.getLogger(RestClientUtility.class.getName());

    @Autowired
    DbeClientDao clientDao;

    @Autowired
    ObeclientProperties dbeclientProperties;

    @Autowired
    SendMaestroDataServiceImpl sendMaestroDataService;

    @Autowired
    private ApplicationConfig applicationConfig;

    private String decompressData(byte[] compressedBytes) {
        // First, try GZIP decompression
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(compressedBytes);
            GZIPInputStream gis = new GZIPInputStream(bis);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            byte[] buffer = new byte[1098];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }

            gis.close();
            bos.close();

            return new String(bos.toByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warning("GZIP decompression failed, trying Inflater: " + e.getMessage());
            System.out.println("GZIP decompression failed, trying Inflater: " + e.getMessage());

            // If GZIP fails, try Inflater
            try {
                Inflater inflater = new Inflater(true); // true for ZLIB header
                inflater.setInput(compressedBytes);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedBytes.length);
                byte[] buffer = new byte[9098];

                while (!inflater.finished()) {
                    int count = inflater.inflate(buffer);
                    outputStream.write(buffer, 0, count);
                }

                outputStream.close();
                inflater.end();

                return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
            } catch (DataFormatException | IOException ex) {
                log.severe("Error decompressing content with Inflater: " + ex.getMessage());
                System.err.println("Error decompressing content with Inflater: " + ex.getMessage());
                ex.printStackTrace();
                
                // If both decompression methods fail, return the original data as a string
                System.out.println("Both decompression methods failed. Returning original data as string.");
                log.severe("Both decompression methods failed. Returning original data as string.");
                return new String(compressedBytes, StandardCharsets.UTF_8);
            }
        }
    }

    public ResponseInternal sendPrimaryroleApi() throws IOException {
        System.out.println("Starting sendPrimaryroleApi method");
        
        String scope = "api.get-third-parties.v1";
        String ClientId = dbeclientProperties.getMaestroClientId();
        String SecretId = dbeclientProperties.getMaestroSecretId();

        String access_token = generateSGconnectToken(scope, ClientId, SecretId);
        ResponseInternal responseObject = null;

        // static date
        String maestrodate = "?snapshotDate=2025-02-15";
        // LocalDate today = LocalDate.now();
        // String formattedDate = today.toString();
        // String maestrodate = "?snapshotDate=" + formattedDate;

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

        System.out.println("Sending API request to: " + this.dbeclientProperties.getMaestrorelationshipApiUrl() + maestrodate);
        
        ResponseEntity<byte[]> result = null;
        try {
            result = restTemplate.exchange(
                    this.dbeclientProperties.getMaestrorelationshipApiUrl() + maestrodate,
                    HttpMethod.GET,
                    entity,
                    byte[].class
            );
        } catch (Exception e) {
            log.severe("Exception while calling API: " + e.getMessage());
            System.err.println("Exception while calling API: " + e.getMessage());
            e.printStackTrace();
            return null;
        }

        if (result == null) {
            log.severe("API call result is null");
            System.err.println("API call result is null");
            return null;
        }

        int status = result.getStatusCode().value();
        if (status == NULL || status == 401 || status == 402 || status == 403
                || status == 404 || status == 500 || status == 201 || status == 501) {
            String errorMessage = "API returned status code: " + status;
            System.err.println(errorMessage);
            log.severe(errorMessage);
            sendMaestroDataService.sendErrorNotification("API Error", errorMessage);
            return null;
        }

        if (status == 200) {
            System.out.println("Successfully Data received from Maestro");
            log.info("**Successfully Data received from Maestro API for Primary Role**");

            byte[] responseBody = result.getBody();
            
            if (responseBody == null || responseBody.length == 0) {
                log.severe("Response body is empty");
                System.err.println("Response body is empty");
                return null;
            }
            
            System.out.println("Response body length: " + responseBody.length);
            
            String decompressedJson = decompressData(responseBody);
            
            if (decompressedJson == null || decompressedJson.isEmpty()) {
                log.severe("Failed to decompress or read the response data");
                System.err.println("Failed to decompress or read the response data");
                return null;
            }
            
            System.out.println("Decompressed JSON length: " + decompressedJson.length());
            // Print a sample of the decompressed JSON
            System.out.println("Sample JSON: " + decompressedJson.substring(0, Math.min(500, decompressedJson.length())));

            try {
                ObjectMapper mapperObj = new ObjectMapper();
                mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
                mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                // First try parsing as a list
                List<ResponseInternal> responseObjects = null;
                try {
                    responseObjects = mapperObj.readValue(decompressedJson,
                            new TypeReference<List<ResponseInternal>>() {});
                    System.out.println("Parsed response as List<ResponseInternal>, size: " + 
                                      (responseObjects != null ? responseObjects.size() : "null"));
                } catch (Exception e) {
                    log.warning("Failed to parse as list, trying as single object: " + e.getMessage());
                    System.out.println("Failed to parse as list, trying as single object: " + e.getMessage());
                    
                    // Try parsing as a single object
                    try {
                        ResponseInternal singleResponse = mapperObj.readValue(decompressedJson, ResponseInternal.class);
                        responseObjects = new ArrayList<>();
                        responseObjects.add(singleResponse);
                        System.out.println("Parsed response as single ResponseInternal object");
                    } catch (Exception e2) {
                        log.severe("Failed to parse as single object: " + e2.getMessage());
                        System.err.println("Failed to parse as single object: " + e2.getMessage());
                        
                        // Try to manually extract and parse the JSON structure
                        System.out.println("Attempting manual JSON analysis...");
                        if (decompressedJson.contains("entityId") && decompressedJson.contains("registrations")) {
                            System.out.println("JSON contains entityId and registrations fields");
                            // Analyze the JSON structure to determine the correct parsing approach
                        }
                        
                        throw e2;
                    }
                }

                // Process the response
                List<InternalRegistrations> allInternalRegistrations = new ArrayList<>();
                int totalRecords = 0;
                int recordsWithSubbooking = 0;

                if (responseObjects != null && !responseObjects.isEmpty()) {
                    for (ResponseInternal wrapper : responseObjects) {
                        if (wrapper.getInternalRegistrations() != null) {
                            System.out.println("Processing " + wrapper.getInternalRegistrations().size() + " internal registrations");
                            
                            // Process each internal registration
                            for (InternalRegistrations internalRegistrations : wrapper.getInternalRegistrations()) {
                                totalRecords++;
                                
                                // Check and pad BDRID if needed
                                if (internalRegistrations.getEntityId() != null) {
                                    String bdrid = internalRegistrations.getEntityId();
                                    System.out.println("Processing entityId: " + bdrid);
                                    
                                    if (bdrid.length() < 10) {
                                        // Pad with leading zeros to make it 10 digits
                                        bdrid = String.format("%010d", Long.parseLong(bdrid));
                                        internalRegistrations.setEntityId(bdrid);
                                        System.out.println("Padded entityId: " + bdrid);
                                    }
                                    
                                    // Process registrations for this entityId
                                    if (internalRegistrations.getRegistrations() != null) {
                                        System.out.println("Found " + internalRegistrations.getRegistrations().size() + 
                                                          " registrations for entityId: " + bdrid);
                                        
                                        for (Registration registration : internalRegistrations.getRegistrations()) {
                                            String code = registration.getCode();
                                            System.out.println("Processing registration code: " + code);
                                            
                                            // Check if this registration has subbooking entities
                                            if (registration.getSubBookingEntities() != null && 
                                                !registration.getSubBookingEntities().isEmpty()) {
                                                
                                                System.out.println("Found " + registration.getSubBookingEntities().size() + 
                                                                  " subbookingEntities for code: " + code);
                                                
                                                // Process each subbooking entity
                                                for (SubBookingEntity subBookingEntity : registration.getSubBookingEntities()) {
                                                    String subbookingId = subBookingEntity.getSubbookingId();
                                                    recordsWithSubbooking++;
                                                    
                                                    System.out.println("Found record - EntityId: " + bdrid + 
                                                                      ", Code: " + code + 
                                                                      ", SubbookingId: " + subbookingId);
                                                    
                                                    // Here you would typically add to your data model or prepare for storage
                                                    // Create a record with the format:
                                                    // CODAPP: code (e.g., CIF, LOA)
                                                    // CODTRS: "0" + bdrid (padded entityId)
                                                    // NUMNTTIPL: bdrid (entityId)
                                                    // NUMIPL: subbookingId
                                                    // CODETBGES: "SGCIB"
                                                    
                                                    // For now, just log the values
                                                    System.out.println("Record to prepare: CODAPP=" + code + 
                                                                      ", CODTRS=0" + bdrid + 
                                                                      ", NUMNTTIPL=" + bdrid + 
                                                                      ", NUMIPL=" + subbookingId + 
                                                                      ", CODETBGES=SGCIB");
                                                }
                                            } else {
                                                System.out.println("No subbookingEntities found for code: " + code);
                                            }
                                        }
                                    } else {
                                        System.out.println("No registrations found for entityId: " + bdrid);
                                    }
                                }
                            }
                            
                            allInternalRegistrations.addAll(wrapper.getInternalRegistrations());
                        } else {
                            System.out.println("No internal registrations found in response wrapper");
                        }
                    }
                } else {
                    System.out.println("No response objects found after parsing");
                }

                System.out.println("Total records processed: " + totalRecords);
                System.out.println("Records with subbooking: " + recordsWithSubbooking);

                // Create the response object
                ResponseInternal transformedData = new ResponseInternal();
                transformedData.setInternalRegistrations(allInternalRegistrations);

                String transformedJson = mapperObj.writeValueAsString(transformedData);
                responseObject = mapperObj.readValue(transformedJson, ResponseInternal.class);

            } catch (JsonProcessingException e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
                log.severe("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.err.println("Unexpected status code: " + status);
            log.severe("Unexpected status code: " + status);
        }

        System.out.println("Completed sendPrimaryroleApi method");
        return responseObject;
    }
}
