package com.example.distributedsystems.httpserver;

public interface OnRequestCallback {
    byte[] handleRequest(byte[] requestPayload);

    String getEndpoint();
}