package com.example.distributedsystems.tfidf;

import com.example.distributedsystems.tfidf.model.DocumentData;
import com.example.distributedsystems.tfidf.search.TFIDF;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequentialSearch {
    public static final String BOOKS_DIRECTORY = "./resources/books";
    public static final String SEARCH_QUERY_1 = "The best detective that catches many criminals using his deductive methods";
    public static final String SEARCH_QUERY_2 = "A war between Russia and France in the cold winter";

//    public static void main(String[] args) throws FileNotFoundException {
//        File documentsDirectory = new File(BOOKS_DIRECTORY);
//        List<String> documents = Arrays.asList(documentsDirectory.list())
//                .stream()
//                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
//                .toList();
//        List<String> terms = TFIDF.getWordsFormLine(SEARCH_QUERY_1);
//        findMostRelevantDocuments(documents, terms);
//    }

    private static void findMostRelevantDocuments(List<String> documents, List<String> terms) throws FileNotFoundException {
        Map<String, DocumentData> documentDataMap = new HashMap<>();
        for (String document : documents) {
            BufferedReader reader = new BufferedReader(new FileReader(document));
            List<String> lines = reader.lines().toList();
            List<String> words = TFIDF.getWordsFromLines(lines);
            DocumentData documentData = TFIDF.createDocumentData(words, terms);
            documentDataMap.put(document, documentData);
        }

        Map<Double, List<String>> documentsSortedByScore = TFIDF.getDocumentsSortedByScore(terms, documentDataMap);
        printResults(documentsSortedByScore);
    }

    private static void printResults(Map<Double, List<String>> documentsSortedByScore) {
        for (Map.Entry<Double, List<String>> entry : documentsSortedByScore.entrySet()) {
            double score = entry.getKey();
            List<String> documentNames = entry.getValue();
            System.out.println("Score: " + score);
            for (String documentName : documentNames) {
                System.out.println("Document: " + documentName);
            }
        }
    }
}
