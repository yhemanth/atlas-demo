package org.apache.atlas.importer.common;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;

import java.util.Map;

public class AtlasReferenceableBuilder {

    private Referenceable referenceable;

    public static AtlasReferenceableBuilder newAtlasReferenceableBuilder() {
        return new AtlasReferenceableBuilder();
    }

    public AtlasReferenceableBuilder ofType(String referenceableType) {
        referenceable = new Referenceable(referenceableType);
        return this;
    }

    public AtlasReferenceableBuilder withReferenceableName(String referenceableName) {
        referenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, referenceableName);
        return this;
    }

    public AtlasReferenceableBuilder withAssetProperties(String name, String description, String owner) {
        referenceable.set(AtlasClient.NAME, name);
        referenceable.set(AtlasClient.DESCRIPTION, description);
        referenceable.set(AtlasClient.OWNER, owner);
        return this;
    }

    public AtlasReferenceableBuilder withAttributeProperties(Map<String, Object> attributeProperties) {
        for (Map.Entry<String, Object> entries : attributeProperties.entrySet()) {
            referenceable.set(entries.getKey(), entries.getValue());
        }
        return this;
    }

    public Referenceable build() {
        return referenceable;
    }
}
