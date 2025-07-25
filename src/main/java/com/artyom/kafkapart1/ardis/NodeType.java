package com.artyom.kafkapart1.ardis;


public enum NodeType {
    PERSON,
    ORGANIZATION,
    UNDETERMINED;

    public static final String PERSON_NODE_IDENTIFIER = "__p_";
    public static final String ORGANIZATION_NODE_IDENTIFIER = "__o_";
    public static final String PERSON_TYPE_KEY = "p";
    public static final String ORGANIZATION_TYPE_KEY = "o";
    public static final String PERSON_COLLECTION ="node_person";
    public static final String UNDETERMINED_COLLECTION ="node_undetermined";
    public static final String ORGANIZATION_COLLECTION ="node_organization";

    public static NodeType getType(String type) {
        return switch (type) {
            case String s when PERSON_TYPE_KEY.equalsIgnoreCase(s) -> PERSON;
            case String s when ORGANIZATION_TYPE_KEY.equalsIgnoreCase(s) -> ORGANIZATION;
            default -> UNDETERMINED;
        };
    }

    public static NodeType getNodeTypeFromId(String id) {
        return switch (id) {
            case String s when s.contains(PERSON_NODE_IDENTIFIER) -> PERSON;
            case String s when s.contains(ORGANIZATION_NODE_IDENTIFIER) -> ORGANIZATION;
            default -> UNDETERMINED;
        };
    }
}
