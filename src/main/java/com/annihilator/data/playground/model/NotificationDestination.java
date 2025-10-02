package com.annihilator.data.playground.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationDestination {

    private UUID id;
    private UUID playgroundId;
    private String destinationType;
    private String destination;
    private long createdAt;

    // Convenience constructor for creating new destinations
    public NotificationDestination(UUID playgroundId, String destinationType, String destination) {
        this.id = UUID.randomUUID();
        this.playgroundId = playgroundId;
        this.destinationType = destinationType;
        this.destination = destination;
        this.createdAt = System.currentTimeMillis();
    }

    // Convenience constructor for EMAIL destinations
    public NotificationDestination(UUID playgroundId, String emailAddress) {
        this(playgroundId, "EMAIL", emailAddress);
    }
}
