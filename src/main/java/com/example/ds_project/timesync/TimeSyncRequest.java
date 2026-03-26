package com.example.ds_project.timesync;

/**
 * NTP-style time synchronization request.
 * Client sends its local time to the server for offset calculation.
 */
public record TimeSyncRequest(
    String nodeId,
    long clientSendTime  // milliseconds since epoch - client's local time when sending
) {}
