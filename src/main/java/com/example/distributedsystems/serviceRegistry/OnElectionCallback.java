package com.example.distributedsystems.serviceRegistry;

public interface OnElectionCallback {

    void onElectedToBeLeader();

    void onWorker();
}