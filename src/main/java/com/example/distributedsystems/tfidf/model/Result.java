package com.example.distributedsystems.tfidf.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Result {
    private Map<String, DocumentData> documentToDocumentData = new HashMap<>();
    public void addDocumentData(String document, DocumentData documentData) {
        documentToDocumentData.put(document, documentData);
    }
    public Map<String, DocumentData> getDocumentToDocumentData() {
        return Collections.unmodifiableMap(documentToDocumentData);
    }
}
