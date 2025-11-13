package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

// Stanford Parser imports
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class Worker {

    // --- Configuration Constants ---
    private static final Region REGION = Region.EU_WEST_1;
    private static final String S3_BUCKET_NAME = "your-unique-bucket-name-here";
    private static final int SQS_VISIBILITY_TIMEOUT = 300;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar Worker.jar <taskQueueName> <resultQueueName>");
            return;
        }

        String taskQueueName = args[0];
        String resultQueueName = args[1];

        try (SqsClient sqs = SqsClient.builder().region(REGION).build();
             S3Client s3 = S3Client.builder().region(REGION).build()) {

            StanfordCoreNLP pipeline = initializeStanfordPipeline();

            String taskQueueUrl = getQueueUrl(sqs, taskQueueName);
            String resultQueueUrl = getQueueUrl(sqs, resultQueueName);

            System.out.println("Worker started, processing tasks from: " + taskQueueUrl);

            // --- Worker Life Cycle: Repeatedly ---
            while (true) {
                // 1. Gets a message from an SQS queue.
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(taskQueueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .visibilityTimeout(SQS_VISIBILITY_TIMEOUT)
                        .build();

                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

                if (messages.isEmpty()) {
                    System.out.print(".");
                    continue;
                }

                Message message = messages.get(0);
                boolean requiresDeletion = processTask(s3, sqs, pipeline, message.body(), resultQueueName);

                // 6. Removes the processed message from the SQS queue.
                if (requiresDeletion) {
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(taskQueueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build());
                }
            }

        } catch (Exception e) {
            System.err.println("Worker crashed, relying on SQS Visibility Timeout for task recovery.");
        }
    }

    // =================================================================================
    // CORE TASK PROCESSING (Simplified Error Handling)
    // =================================================================================

    private static boolean processTask(S3Client s3, SqsClient sqs, StanfordCoreNLP pipeline, String messageBody, String resultQueueName) {
        String doneQueueName;
        String analysisType;
        String fileUrl;

        // --- 1. Basic Parsing ---
        try {
            String[] parts = messageBody.split("\\|", 2);
            doneQueueName = parts[1];
            String[] taskParts = parts[0].split("\t", 2);
            analysisType = taskParts[0].trim();
            fileUrl = taskParts[1].trim();

        } catch (Exception e) {
            return true; // Malformed message, delete it to prevent infinite loop.
        }

        String resultLine;

        try {
            // 2. Downloads the text file.
            String fileContent = downloadTextFile(fileUrl);

            // 3. Performs the requested analysis.
            String analysisOutput = performAnalysis(pipeline, analysisType, fileContent);

            // 4. Uploads the resulting analysis file to S3.
            String outputS3Key = "analyzed/" + doneQueueName + "/" + UUID.randomUUID().toString() + ".txt";
            String s3Url = uploadAnalysisToS3(s3, analysisOutput, outputS3Key);

            // Success message line construction:
            resultLine = String.format("%s: <a href='%s'>%s</a> <a href='%s'>OUTPUT</a>",
                    analysisType, fileUrl, fileUrl, s3Url);

            // 5. Puts a message in an SQS queue indicating the result.
            sendResultToManager(sqs, doneQueueName, resultLine, resultQueueName);
            return true; // Success! Delete the task message.

        } catch (Exception e) {
            // If an exception occurs, send a message to the manager...
            String exceptionDescription = e.getMessage() != null ? e.getMessage().substring(0, Math.min(e.getMessage().length(), 250)) : "Unknown Error";

            // Failure message line construction:
            resultLine = String.format("%s: <a href='%s'>%s</a> %s",
                    analysisType, fileUrl, fileUrl, "EXCEPTION: " + exceptionDescription);

            // Send failure message to Manager.
            sendResultToManager(sqs, doneQueueName, resultLine, resultQueueName);
            return true; // Report sent successfully! Delete the task message.
        }
    }

    // =================================================================================
    // UTILITY METHODS
    // =================================================================================

    private static StanfordCoreNLP initializeStanfordPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, parse");
        return new StanfordCoreNLP(props);
    }

    private static String downloadTextFile(String fileUrl) throws IOException {
        URL url = new URL(fileUrl);
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(15000);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    private static String performAnalysis(StanfordCoreNLP pipeline, String analysisType, String content) {
        Document document = new Document(content);
        StringBuilder output = new StringBuilder();

        for (Sentence sent : document.sentences()) {
            String analyzed;

            switch (analysisType) {
                case "POS":
                    analyzed = "POS: " + sent.posTags().toString();
                    break;
                case "CONSTITUENCY":
                    analyzed = "CONSTITUENCY: " + sent.parse().toString();
                    break;
                case "DEPENDENCY":
                    // FINAL FIX: Using dependencyGraph().toString()
                    // This is the most likely correct method for simple API dependency output in v3.6.0
                    analyzed = "DEPENDENCY: " + sent.dependencyGraph().toString();
                    break;
                default:
                    analyzed = "Unsupported Analysis Type: " + analysisType;
            }
            output.append(analyzed).append("\n");
        }
        return output.toString();
    }

    private static String uploadAnalysisToS3(S3Client s3, String analysisResult, String key) {
        String s3Url = String.format("s3://%s/%s", S3_BUCKET_NAME, key);

        s3.putObject(PutObjectRequest.builder()
                        .bucket(S3_BUCKET_NAME)
                        .key(key)
                        .contentType("text/plain")
                        .acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromString(analysisResult));
        return s3Url;
    }

    private static void sendResultToManager(SqsClient sqs, String doneQueueName, String resultLine, String resultQueueName) {
        try {
            String queueUrl = getQueueUrl(sqs, resultQueueName);

            // Done Message Format for Manager: DONE_QUEUE_NAME|HTML_RESULT_LINE
            String messageBody = doneQueueName + "|" + resultLine;

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build());
        } catch (SqsException e) {
            System.err.println("Error sending result to Manager, relying on task retry: " + e.getMessage());
        }
    }

    private static String getQueueUrl(SqsClient sqs, String queueName) throws SqsException {
        return sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
    }
}