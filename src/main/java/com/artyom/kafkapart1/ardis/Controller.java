package com.artyom.kafkapart1.ardis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Controller {


    private static Map<String, List<String>> extractRelationShortcut(ExploreResponse exploreResponse) {
        Map<String, Object> analytics = exploreResponse.getAnalytics();
        CommonSection relationship = (CommonSection) analytics.get("relationship");
        JsonObject data = relationship.getData();

        Map<String, String> idToName = new LinkedHashMap<>();
        data.getAsJsonArray("nodes").forEach(n -> {
            JsonObject node = n.getAsJsonObject();
            String id = node.get("id").getAsString();
            String title = node.get("title").getAsString();
            String nodeType = node.get("nodeType").getAsString();

            idToName.put(id, nodeType + ": " + title);
        });

        Map<String, List<Edge>> graph = new HashMap<>();
        data.getAsJsonArray("edges").forEach(e -> {
            JsonObject edge = e.getAsJsonObject();
            String source = edge.get("source").getAsString();
            String target = edge.get("target").getAsString();
            String title = edge.get("title").getAsString();

            graph.computeIfAbsent(target, k -> new ArrayList<>()).add(new Edge(source, title));
        });

        Map<String, List<String>> result = new LinkedHashMap<>();

        for (String targetId : idToName.keySet()) {
            List<Edge> edges = graph.get(targetId);
            if (edges == null) continue;

            String targetName = idToName.get(targetId);
            List<String> relations = new ArrayList<>();

            for (Edge edge : edges) {
                String relation = edge.relation;
                String sourceName = idToName.get(edge.source);
                relations.add(relation + ": " + sourceName);
            }

            if (!relations.isEmpty()) {
                result.put(targetName, relations);
            }
        }

        JsonObject jsonObject = new JsonObject();
        result.entrySet().stream().findFirst().ifPresent(entry -> {
            jsonObject.addProperty("root", entry.getKey());

            JsonArray directArray = new JsonArray();
            for (var direct : entry.getValue()) {
                directArray.add(direct);
            }
            jsonObject.add("direct", directArray);
        });

        JsonArray childrenArray = new JsonArray();
        result.entrySet().stream().skip(1)
            .forEach(entry -> {
                List<String> direct = entry.getValue();
                if (direct.isEmpty()) {
                    return;
                }
                direct = direct.stream().map(name -> "'" + name + "'").toList();
                childrenArray.add(entry.getKey() + " -> [" + String.join(", ", direct) + "]");
            });
        jsonObject.add("related", childrenArray);

        return result;
    }


    private static List<String> extractRelationShortcutDFS(ExploreResponse exploreResponse) {
        Map<String, Object> analytics = exploreResponse.getAnalytics();
        CommonSection relationship = (CommonSection) analytics.get("relationship");
        JsonObject data = relationship.getData();

        Map<String, String> idToName = new LinkedHashMap<>();
        data.getAsJsonArray("nodes").forEach(n -> {
            JsonObject node = n.getAsJsonObject();
            String id = node.get("id").getAsString();
            String title = node.get("title").getAsString();
            String nodeType = node.get("nodeType").getAsString();

            idToName.put(id, nodeType + ": " + title);
        });

        Map<String, List<Edge>> graph = new HashMap<>();
        data.getAsJsonArray("edges").forEach(e -> {
            JsonObject edge = e.getAsJsonObject();
            String source = edge.get("source").getAsString();
            String target = edge.get("target").getAsString();
            String title = edge.get("title").getAsString();

            graph.computeIfAbsent(target, k -> new ArrayList<>()).add(new Edge(source, title));
        });

        Set<String> visited = new HashSet<>();
        List<List<String>> allPaths = new ArrayList<>();

        for (String nodeId : idToName.keySet()) {
            dfs(nodeId, idToName.get(nodeId), idToName, graph, visited, new ArrayList<>(), allPaths);
        }

        Node root = new Node("ROOT");
        for (List<String> path : allPaths) {
            insertPath(root, path);
        }

        List<String> result = new ArrayList<>();
        for (Node child : root.children.values()) {
            result.add(serializeNode(child));
        }
        Map<String, Object> relationMap = convertTreeToMap(root);

        result =  result.stream().filter(r -> r.contains("->")).toList();

        return result;
    }


    private static void insertPath(Node root, List<String> path) {
        Node current = root;
        for (String part : path) {
            current = current.children.computeIfAbsent(part, Node::new);
        }
    }


    private static String serializeNode(Node node) {
        if (node.children.isEmpty()) {
            return node.value;
        }
        List<String> parts = new ArrayList<>();
        for (Node child : node.children.values()) {
            parts.add(serializeNode(child));
        }
        return node.value + " -> [" + String.join(", ", parts) + "]";
    }

    private static Map<String, Object> convertTreeToMap(Node node) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, Node> entry : node.children.entrySet()) {
            Node child = entry.getValue();
            if (child.children.isEmpty()) {
                map.put(child.value, null);
            } else {
                map.put(child.value, convertTreeToMap(child));
            }
        }
        return map;
    }

    private static void dfs(String nodeId, String label, Map<String, String> idToName,
                            Map<String, List<Edge>> graph, Set<String> visited,
                            List<String> path, List<List<String>> allPaths) {
        if (visited.contains(nodeId)) return;
        visited.add(nodeId);

        path.add(label);
        if (!graph.containsKey(nodeId)) {
            allPaths.add(new ArrayList<>(path));
        } else {
            for (Edge edge : graph.get(nodeId)) {
                String relation = edge.relation;
                String nextLabel = idToName.get(edge.source);
                path.add(relation);
                dfs(edge.source, nextLabel, idToName, graph, visited, path, allPaths);
                path.removeLast();
            }
        }

        path.removeLast();
        visited.remove(nodeId);
    }

    private static class Edge {
        String source;
        String relation;

        Edge(String source, String relation) {
            this.source = source;
            this.relation = relation;
        }
    }

    private static class Node {
        String value;
        Map<String, Node> children = new LinkedHashMap<>();

        Node(String value) {
            this.value = value;
        }
    }
}
