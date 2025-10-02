package com.annihilator.data.playground.notification;

import com.annihilator.data.playground.utility.NotificationException;

/**
 * Interface for notification services.
 * Provides a contract for sending notifications through various channels.
 */
public interface NotificationService {

    /**
     * Sends a notification with the specified subject and message.
     *
     * @param subject the subject/title of the notification
     * @param message the content/body of the notification
     * @throws NotificationException if the notification fails to send
     */
    void notify(String subject, String message) throws NotificationException;

    /**
     * Sends a notification with the specified subject, message, and recipient.
     *
     * @param subject the subject/title of the notification
     * @param message the content/body of the notification
     * @param recipient the recipient of the notification (email address, phone number, etc.)
     * @throws NotificationException if the notification fails to send
     */
    void notify(String subject, String message, String recipient) throws NotificationException;
}

