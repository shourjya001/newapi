import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

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
import com.socgen.riskweb.Model.MaestroTableEntity;
import com.socgen.riskweb.dao.MaestroTableRepository;

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
    
    @Autowired
    private MaestroTableRepository maestroTableRepository;

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

        System.out.println("requestObject---->" + headers);

        ResponseEntity<byte[]> result = restTemplate.exchange(
                this.dbeclientProperties.getMaestrorelationshipApiUrl() + maestrodate,
                HttpMethod.GET,
                entity,
                byte[].class
        );

        int status = result.getStatusCodeValue();
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
            String decompressedJson = decompressData(responseBody);
            
            // Debug output
            System.out.println(decompressedJson);

            if (decompressedJson == null) {
                log.severe("Failed to decompress or read the response data");
                System.err.println("Failed to decompress or read the response data");
                return null;
            }

            try {
                ObjectMapper mapperObj = new ObjectMapper();
                mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
                mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                List<ResponseInternal> responseObjects = mapperObj.readValue(decompressedJson,
                        new TypeReference<List<ResponseInternal>>() {});

                List<InternalRegistrations> allInternalRegistrations = new ArrayList<>();
                List<MaestroTableEntity> maestroTableEntities = new ArrayList<>();

                for (ResponseInternal wrapper : responseObjects) {
                    if (wrapper.getInternalRegistrations() != null) {
                        // Process each internal registration
                        for (InternalRegistrations internalRegistrations : wrapper.getInternalRegistrations()) {
                            // Check and pad BDRID if needed
                            if (internalRegistrations.getEntityId() != null) {
                                String bdrid = internalRegistrations.getEntityId();
                                if (bdrid.length() < 10) {
                                    // Pad with leading zeros to make it 10 digits
                                    bdrid = String.format("%010d", Long.parseLong(bdrid));
                                    internalRegistrations.setEntityId(bdrid);
                                }
                                
                                // Process registrations for this entityId
                                if (internalRegistrations.getRegistrations() != null) {
                                    for (Registration registration : internalRegistrations.getRegistrations()) {
                                        String code = registration.getCode();
                                        
                                        // Check if this registration has subbooking entities
                                        if (registration.getSubBookingEntities() != null && 
                                            !registration.getSubBookingEntities().isEmpty()) {
                                            
                                            // Process each subbooking entity
                                            for (SubBookingEntity subBookingEntity : registration.getSubBookingEntities()) {
                                                // Create a table entity for each subbooking
                                                MaestroTableEntity tableEntity = new MaestroTableEntity();
                                                tableEntity.setCodapp(code);
                                                tableEntity.setCodtrs("0" + bdrid); // Padded entityId
                                                tableEntity.setNumnttipl(bdrid);    // Original entityId
                                                tableEntity.setNumipl(subBookingEntity.getSubbookingId());
                                                tableEntity.setCodetbges("SGCIB"); // Default value from sample
                                                
                                                maestroTableEntities.add(tableEntity);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        allInternalRegistrations.addAll(wrapper.getInternalRegistrations());
                    }
                }
                
                // Save all table entities to the database
                if (!maestroTableEntities.isEmpty()) {
                    saveMaestroTableEntities(maestroTableEntities);
                    log.info("Saved " + maestroTableEntities.size() + " records to the database");
                    System.out.println("Saved " + maestroTableEntities.size() + " records to the database");
                }

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

        System.out.println("Response object: " + responseObject);
        return responseObject;
    }
    
    @Transactional
    private void saveMaestroTableEntities(List<MaestroTableEntity> entities) {
        maestroTableRepository.saveAll(entities);
    }
}
