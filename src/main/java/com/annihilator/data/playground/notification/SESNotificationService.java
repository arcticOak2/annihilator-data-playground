package com.annihilator.data.playground.notification;

import com.annihilator.data.playground.config.AWSSESConfig;
import com.annihilator.data.playground.utility.NotificationException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.Body;
import software.amazon.awssdk.services.ses.model.Content;
import software.amazon.awssdk.services.ses.model.Destination;
import software.amazon.awssdk.services.ses.model.Message;
import software.amazon.awssdk.services.ses.model.SendEmailRequest;
import software.amazon.awssdk.services.ses.model.SendEmailResponse;
import software.amazon.awssdk.services.ses.model.SesException;

import java.util.List;

/**
 * AWS SES implementation of NotificationService.
 */
public class SESNotificationService implements NotificationService {

    private final AWSSESConfig sesConfig;
    private final SesClient sesClient;

    public SESNotificationService(AWSSESConfig sesConfig) {
        this.sesConfig = sesConfig;
        this.sesClient = SesClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(sesConfig.getAccessKey(), sesConfig.getSecretKey())))
                .build();
    }

    @Override
    public void notify(String subject, String message) throws NotificationException {
        notify(subject, message, sesConfig.getTo());
    }

    @Override
    public void notify(String subject, String message, String recipient) throws NotificationException {
        try {
            Destination destination = Destination.builder()
                    .toAddresses(recipient)
                    .build();

            Content subjectContent = Content.builder()
                    .data(subject)
                    .build();

            Content bodyContent = Content.builder()
                    .data(message)
                    .build();

            Body body = Body.builder()
                    .html(bodyContent)
                    .build();

            Message msg = Message.builder()
                    .subject(subjectContent)
                    .body(body)
                    .build();

            SendEmailRequest emailRequest = SendEmailRequest.builder()
                    .destination(destination)
                    .message(msg)
                    .source(sesConfig.getFrom())
                    .build();

            sesClient.sendEmail(emailRequest);

        } catch (SesException e) {
            throw new NotificationException("Failed to send email: " + e.awsErrorDetails().errorMessage(), e);
        }
    }

    public void close() {
        sesClient.close();
    }
}