// InternalRegistrations.java
package com.socgen.riskweb.Model;

import org.springframework.data.annotation.Id;
import java.util.List;

public class InternalRegistrations {
    @Id
    private String entityId;
    private List<Registration> registrations;

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public List<Registration> getRegistrations() {
        return registrations;
    }

    public void setRegistrations(List<Registration> registrations) {
        this.registrations = registrations;
    }
    
    @Override
    public String toString() {
        return "InternalRegistrations{" +
                "entityId='" + entityId + '\'' +
                ", registrations=" + registrations +
                '}';
    }
}

// Registration.java
package com.socgen.riskweb.Model;

import java.util.List;

public class Registration {
    private String code;
    private String label;
    private String value;
    private List<SubBookingEntity> subBookingEntities;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<SubBookingEntity> getSubBookingEntities() {
        return subBookingEntities;
    }

    public void setSubBookingEntities(List<SubBookingEntity> subBookingEntities) {
        this.subBookingEntities = subBookingEntities;
    }
    
    @Override
    public String toString() {
        return "Registration{" +
                "code='" + code + '\'' +
                ", label='" + label + '\'' +
                ", value='" + value + '\'' +
                ", subBookingEntities=" + subBookingEntities +
                '}';
    }
}

// SubBookingEntity.java
package com.socgen.riskweb.Model;

public class SubBookingEntity {
    private String subbookingId;
    private String subbookingName;
    private String value;

    public String getSubbookingId() {
        return subbookingId;
    }

    public void setSubbookingId(String subbookingId) {
        this.subbookingId = subbookingId;
    }

    public String getSubbookingName() {
        return subbookingName;
    }

    public void setSubbookingName(String subbookingName) {
        this.subbookingName = subbookingName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return "SubBookingEntity{" +
                "subbookingId='" + subbookingId + '\'' +
                ", subbookingName='" + subbookingName + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}

// ResponseInternal.java
package com.socgen.riskweb.Model;

import java.util.List;

public class ResponseInternal {
    private List<InternalRegistrations> internalRegistrations;

    public List<InternalRegistrations> getInternalRegistrations() {
        return internalRegistrations;
    }

    public void setInternalRegistrations(List<InternalRegistrations> internalRegistrations) {
        this.internalRegistrations = internalRegistrations;
    }
}
