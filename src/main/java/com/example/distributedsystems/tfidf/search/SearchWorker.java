package com.example.distributedsystems.tfidf.search;

import com.example.distributedsystems.httpserver.OnRequestCallback;
import com.example.distributedsystems.tfidf.model.DocumentData;
import com.example.distributedsystems.tfidf.model.Result;
import com.example.distributedsystems.tfidf.model.SerializationUtils;
import com.example.distributedsystems.tfidf.model.Task;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;

public class SearchWorker implements OnRequestCallback {
    private final String ENDPOINT = "/task";

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        Task task = (Task) SerializationUtils.deserialize(requestPayload);
        Result result = createResult(task);
        return SerializationUtils.serialize(result);
    }

    private Result createResult(Task task) {
        List<String> documents = task.getDocuments();
        System.out.println("Processing task with documents: " + documents);
        Result result = new Result();
        for (String document : documents) {
            List<String> words = parseWordsFromDocument(document);
            DocumentData documentData = TFIDF.createDocumentData(words, task.getSearchTerms());
            result.addDocumentData(document, documentData);
        }
        return result;
    }

    private List<String> parseWordsFromDocument(String document) {
        FileReader fileReader = null;
        try {
            fileReader = new FileReader(document);
        } catch (FileNotFoundException e) {
            return Collections.emptyList();
        }
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<String> lines = bufferedReader.lines()
                .toList();

        return TFIDF.getWordsFromLines(lines);
    }

    @Override
    public String getEndpoint() {
        return null;
    }
}
