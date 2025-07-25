package com.artyom.kafkapart1.ardis;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CommonSection extends Section {
    private String footer;
    private Map<String, Set<String>> valueMap = new LinkedHashMap<>();

    private List<List<String>> valueList = new ArrayList<>();
    private JsonObject data;

    public CommonSection(View view, String id, String title, String sectionId) {
        super(view, id, title, sectionId);
    }

    public CommonSection(View view, String id, String title, String sectionId, String header) {
        super(view, id, title, sectionId, header);
    }

    public CommonSection(String id, String sectionId) {
        super(id, sectionId);
    }
}
