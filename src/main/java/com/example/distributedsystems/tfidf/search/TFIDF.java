package com.example.distributedsystems.tfidf.search;

import com.example.distributedsystems.tfidf.model.DocumentData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TFIDF {
    public static double calculateFrequency(List<String> words, String term) {
        long count = 0;
        for (String word : words) {
            if (word.equalsIgnoreCase(term)) {
                count++;
            }
        }
        return (double) count / words.size();
    }

    public static List<String> getWordsFormLine(String line){
        return Arrays.asList(line.split("(\\.)+|(,)+|( )+|(-)+|\\?+|(;)+|(d/)+|(/n)"));
    }
    public static List<String> getWordsFromLines(List<String> lines) {
        List<String> words = new ArrayList<>();
        for (String line : lines) {
            words.addAll(getWordsFormLine(line));
        }
        return words;
    }

    public static DocumentData createDocumentData(List<String> words, List<String> terms) {
        DocumentData documentData = new DocumentData();
        for (String term : terms) {
            double frequency = calculateFrequency(words, term);
            documentData.putTermFrequency(term, frequency);
        }
        return documentData;
    }

    public static Map<Double, List<String>> getDocumentsSortedByScore(List<String> terms, Map<String, DocumentData> documentResults) {
        TreeMap<Double, List<String>> sortedDocuments = new TreeMap<>();
        Map<String, Double> termToIdfMap = getTermToInverseDocumentFrequencyMap(terms, documentResults);

        for (String docId : documentResults.keySet()) {
            DocumentData documentData = documentResults.get(docId);
            double score = calculateDocumentScore(terms, documentData, termToIdfMap);
            addDocumentScoreToTreeMap(sortedDocuments, score, docId);
        }

        return sortedDocuments.descendingMap();

    }

    private static double calculateDocumentScore(List<String> terms, DocumentData documentData, Map<String, Double> termTOInverseDocumentFrequency) {
        double score = 0.0;
        for (String term : terms) {
            double tf = documentData.getFrequency(term);
            double idf = termTOInverseDocumentFrequency.getOrDefault(term, 0.0);
            score += tf * idf;
        }
        return score;
    }

    private static void addDocumentScoreToTreeMap(TreeMap<Double, List<String>> sortedDocuments, double score, String docId) {
        List<String> documentsWithCurrentScore = sortedDocuments.get(score);
        if (documentsWithCurrentScore == null) {
            sortedDocuments.put(score, new ArrayList<>());
        }
        sortedDocuments.get(score).add(docId);
        sortedDocuments.put(score, documentsWithCurrentScore);
    }

    public static Map<String, Double> getTermToInverseDocumentFrequencyMap(List<String> terms, Map<String, DocumentData> documentResults) {
        Map<String, Double> termToIdfMap = new HashMap<>();
        for (String term : terms) {
            double idf = getInverseDocumentFrequencies(term, documentResults);
            termToIdfMap.put(term, idf);
        }
        return termToIdfMap;
    }

    public static double getInverseDocumentFrequencies(String term, Map<String, DocumentData> documentResults) {
        double nt = 0;
        for (String docId : documentResults.keySet()) {
            DocumentData documentData = documentResults.get(docId);
            if (documentData.getFrequency(term) > 0.0) {
                nt++;
            }
        }
        return nt == 0 ? 0 : Math.log10(documentResults.size() / nt);
    }
}
