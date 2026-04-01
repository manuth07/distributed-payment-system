package com.example.ds_project.timesync;

/**
 * NTP-style time synchronization response.
 * Server responds with its timestamps for round-trip delay estimation.
 */
public record TimeSyncResponse(
    long serverReceiveTime,    // milliseconds since epoch - server received the request
    long serverSendTime,       // milliseconds since epoch - server sending this response
    long estimatedClockOffset  // milliseconds - calculated offset (positive = this node is ahead)
) {}
