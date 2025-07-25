package com.artyom.kafkapart1.ardis;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExploreResponse implements Serializable {
    private String projectId = null;
    private Map<String, Object> node = new LinkedHashMap<>();
    private ProcessStatus status = ProcessStatus.RUNNING;
    private Map<String, Object> analytics = new LinkedHashMap<>();

    public ExploreResponse(NodeType nodeType, String nodeTitle, String nodeAvatar) {
        node.put("type", nodeType);
        node.put("title", nodeTitle);

		if (nodeAvatar != null) {
			node.put("avatar", nodeAvatar);
		}
    }
}
