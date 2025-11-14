package com.example;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Worker {

    // --- Configuration Constants ---
    private static final Region REGION = Region.EU_WEST_1;
    private static final String S3_BUCKET_NAME = "your-unique-bucket-name-here";
    private static final int MAX_TEXT_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB limit for safety
    private static final int DOWNLOAD_TIMEOUT_MS = 60000; // 1 minute timeout for downloads

    // Stanford NLP Pipeline (initialized once for efficiency)
    private static StanfordCoreNLP pipeline;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar Worker.jar <taskQueueName> <resultQueueName>");
            return;
        }

        String taskQueueName = args[0];
        String resultQueueName = args[1];

        System.out.println("Worker started.");
        System.out.println("Task Queue: " + taskQueueName);
        System.out.println("Result Queue: " + resultQueueName);

        // Initialize Stanford CoreNLP pipeline (expensive operation - do once)
        initializeNLPPipeline();

        try (SqsClient sqs = SqsClient.builder().region(REGION).build();
             S3Client s3 = S3Client.builder().region(REGION).build()) {

            String taskQueueUrl = getQueueUrl(sqs, taskQueueName);
            String resultQueueUrl = getQueueUrl(sqs, resultQueueName);

            System.out.println("Worker ready. Starting message processing loop...");

            // Main worker loop - continuously process messages
            while (true) {
                try {
                    // Receive messages from task queue
                    List<Message> messages = receiveMessages(sqs, taskQueueUrl);

                    if (messages.isEmpty()) {
                        // No messages - brief sleep before next poll
                        TimeUnit.SECONDS.sleep(2);
                        continue;
                    }

                    for (Message message : messages) {
                        processTaskMessage(sqs, s3, message, taskQueueUrl, resultQueueUrl);
                    }

                } catch (InterruptedException e) {
                    System.out.println("Worker interrupted. Shutting down gracefully...");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error in worker main loop: " + e.getMessage());
                    e.printStackTrace();
                    // Continue processing other messages
                }
            }

        } catch (Exception e) {
            System.err.println("Fatal error in Worker: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Worker terminated.");
    }

    // =================================================================================
    // NLP PIPELINE INITIALIZATION
    // =================================================================================

    private static void initializeNLPPipeline() {
        System.out.println("Initializing Stanford CoreNLP pipeline...");
        Properties props = new Properties();
        // Configure annotators for all three analysis types
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse");
        props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
        props.setProperty("tokenize.language", "en");

        pipeline = new StanfordCoreNLP(props);
        System.out.println("Pipeline initialized successfully.");
    }

    // =================================================================================
    // SQS HELPERS
    // =================================================================================

    private static String getQueueUrl(SqsClient sqs, String queueName) {
        try {
            return sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build()).queueUrl();
        } catch (QueueDoesNotExistException e) {
            System.err.println("Queue does not exist: " + queueName);
            throw e;
        }
    }

    private static List<Message> receiveMessages(SqsClient sqs, String queueUrl) {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1) // Process one at a time for reliability
                    .waitTimeSeconds(20) // Long polling
                    .build();

            return sqs.receiveMessage(receiveRequest).messages();
        } catch (SqsException e) {
            System.err.println("Error receiving messages: " + e.getMessage());
            return List.of();
        }
    }

    private static void deleteMessage(SqsClient sqs, String queueUrl, String receiptHandle) {
        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build());
        } catch (SqsException e) {
            System.err.println("Error deleting message: " + e.getMessage());
        }
    }

    private static void sendResultMessage(SqsClient sqs, String queueUrl, String messageBody) {
        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build());
        } catch (SqsException e) {
            System.err.println("Error sending result message: " + e.getMessage());
            throw e;
        }
    }

    // =================================================================================
    // TASK PROCESSING
    // =================================================================================

    /**
     * Processes a single task message.
     * Message format: ANALYSIS_TYPE\tURL|DONE_QUEUE_NAME
     */
    private static void processTaskMessage(SqsClient sqs, S3Client s3, Message message,
                                           String taskQueueUrl, String resultQueueUrl) {
        String messageBody = message.body();
        String receiptHandle = message.receiptHandle();

        System.out.println("\n--- Processing new task ---");
        System.out.println("Message: " + messageBody);

        try {
            // Parse message: "ANALYSIS_TYPE\tURL|DONE_QUEUE_NAME"
            String[] mainParts = messageBody.split("\\|", 2);
            if (mainParts.length != 2) {
                throw new IllegalArgumentException("Invalid message format (missing |): " + messageBody);
            }

            String taskPart = mainParts[0]; // "ANALYSIS_TYPE\tURL"
            String doneQueueName = mainParts[1];

            String[] taskDetails = taskPart.split("\t", 2);
            if (taskDetails.length != 2) {
                throw new IllegalArgumentException("Invalid task format (missing tab): " + taskPart);
            }

            String analysisType = taskDetails[0].trim().toUpperCase();
            String inputUrl = taskDetails[1].trim();

            System.out.println("Analysis Type: " + analysisType);
            System.out.println("Input URL: " + inputUrl);
            System.out.println("Done Queue: " + doneQueueName);

            // Validate analysis type
            if (!analysisType.equals("POS") &&
                    !analysisType.equals("CONSTITUENCY") &&
                    !analysisType.equals("DEPENDENCY")) {
                throw new IllegalArgumentException("Unknown analysis type: " + analysisType);
            }

            // 1. Download text file from URL
            String textContent = downloadTextFromUrl(inputUrl);

            // 2. Perform NLP analysis
            String analysisResult = performAnalysis(textContent, analysisType);

            // 3. Upload result to S3
            String outputS3Url = uploadResultToS3(s3, analysisResult, doneQueueName, analysisType);

            // 4. Send result message to manager
            // Format: DONE_QUEUE_NAME|<analysis type>: <input file> <output file>
            String resultMessage = String.format("%s|%s: <a href=\"%s\">%s</a> <a href=\"%s\">output</a>",
                    doneQueueName, analysisType, inputUrl, inputUrl, outputS3Url);

            sendResultMessage(sqs, resultQueueUrl, resultMessage);

            System.out.println("✅ Task completed successfully!");

        } catch (Exception e) {
            // Handle exceptions gracefully - send error message to manager
            System.err.println("❌ Error processing task: " + e.getMessage());
            e.printStackTrace();

            try {
                // Extract what we can from the message
                String[] mainParts = messageBody.split("\\|", 2);
                String doneQueueName = mainParts.length > 1 ? mainParts[1] : "unknown";

                String[] taskDetails = mainParts[0].split("\t", 2);
                String analysisType = taskDetails.length > 0 ? taskDetails[0].trim() : "UNKNOWN";
                String inputUrl = taskDetails.length > 1 ? taskDetails[1].trim() : "unknown";

                // Send error result
                String errorMessage = String.format("%s|%s: <a href=\"%s\">%s</a> ERROR: %s",
                        doneQueueName, analysisType, inputUrl, inputUrl,
                        escapeHtml(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));

                sendResultMessage(sqs, resultQueueUrl, errorMessage);

            } catch (Exception e2) {
                System.err.println("Failed to send error message: " + e2.getMessage());
            }
        } finally {
            // IMPORTANT: Always delete message from queue after processing (success or failure)
            // This ensures the message doesn't get reprocessed
            deleteMessage(sqs, taskQueueUrl, receiptHandle);
        }
    }

    // =================================================================================
    // TEXT DOWNLOAD
    // =================================================================================

    private static String downloadTextFromUrl(String urlString) throws IOException {
        System.out.println("Downloading text from: " + urlString);

        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(DOWNLOAD_TIMEOUT_MS);
        connection.setReadTimeout(DOWNLOAD_TIMEOUT_MS);
        connection.setRequestProperty("User-Agent", "Mozilla/5.0"); // Some servers require this

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new IOException("HTTP error code: " + responseCode + " for URL: " + urlString);
        }

        // Check content length to avoid downloading huge files
        long contentLength = connection.getContentLengthLong();
        if (contentLength > MAX_TEXT_SIZE_BYTES) {
            throw new IOException("File too large: " + contentLength + " bytes (max: " + MAX_TEXT_SIZE_BYTES + ")");
        }

        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), "UTF-8"))) {

            String line;
            int totalBytes = 0;
            while ((line = reader.readLine()) != null) {
                totalBytes += line.getBytes("UTF-8").length;
                if (totalBytes > MAX_TEXT_SIZE_BYTES) {
                    throw new IOException("File exceeded size limit during download");
                }
                content.append(line).append("\n");
            }
        }

        System.out.println("Download complete. Size: " + content.length() + " characters");
        return content.toString();
    }

    // =================================================================================
    // NLP ANALYSIS
    // =================================================================================

    private static String performAnalysis(String text, String analysisType) throws Exception {
        System.out.println("Performing " + analysisType + " analysis...");

        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("Text content is empty");
        }

        StringBuilder result = new StringBuilder();

        // For very large texts, process line by line to avoid memory issues
        String[] lines = text.split("\n");
        int lineCount = 0;
        int processedLines = 0;

        for (String line : lines) {
            lineCount++;

            // Skip empty lines
            if (line.trim().isEmpty()) {
                continue;
            }

            // Limit processing to avoid extremely long execution times
            if (processedLines >= 100) {
                result.append("[... truncated after 100 non-empty lines ...]\n");
                break;
            }

            try {
                String lineResult = analyzeLine(line, analysisType);
                result.append("Line ").append(lineCount).append(":\n");
                result.append(lineResult).append("\n\n");
                processedLines++;
            } catch (Exception e) {
                result.append("Line ").append(lineCount).append(": ERROR - ")
                        .append(e.getMessage()).append("\n\n");
            }
        }

        System.out.println("Analysis complete. Processed " + processedLines + " lines.");
        return result.toString();
    }

    private static String analyzeLine(String line, String analysisType) {
        // Create an annotation object with the text
        Annotation document = new Annotation(line);

        // Run all annotators on the text
        pipeline.annotate(document);

        // Get sentences from the document
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        StringBuilder lineResult = new StringBuilder();

        for (CoreMap sentence : sentences) {
            switch (analysisType) {
                case "POS":
                    lineResult.append(performPOSTagging(sentence));
                    break;
                case "CONSTITUENCY":
                    lineResult.append(performConstituencyParsing(sentence));
                    break;
                case "DEPENDENCY":
                    lineResult.append(performDependencyParsing(sentence));
                    break;
            }
            lineResult.append("\n");
        }

        return lineResult.toString();
    }

    /**
     * Part-of-Speech Tagging
     */
    private static String performPOSTagging(CoreMap sentence) {
        StringBuilder result = new StringBuilder();
        List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);

        for (CoreLabel token : tokens) {
            String word = token.get(CoreAnnotations.TextAnnotation.class);
            String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
            result.append(word).append("/").append(pos).append(" ");
        }

        return result.toString().trim();
    }

    /**
     * Constituency Parsing (Tree representation)
     */
    private static String performConstituencyParsing(CoreMap sentence) {
        Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
        return tree.toString();
    }

    /**
     * Dependency Parsing
     */
    private static String performDependencyParsing(CoreMap sentence) {
        SemanticGraph dependencies = sentence.get(
                SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class);
        return dependencies.toString();
    }

    // =================================================================================
    // S3 UPLOAD
    // =================================================================================

    private static String uploadResultToS3(S3Client s3, String content, String doneQueueName,
                                           String analysisType) throws Exception {
        System.out.println("Uploading result to S3...");

        // Generate unique file name
        String fileName = String.format("%s-%s-%s.txt",
                analysisType.toLowerCase(),
                UUID.randomUUID().toString(),
                System.currentTimeMillis());

        String s3Key = "results/" + doneQueueName + "/" + fileName;

        try {
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(S3_BUCKET_NAME)
                            .key(s3Key)
                            .contentType("text/plain")
                            .build(),
                    RequestBody.fromString(content));

            String s3Url = String.format("s3://%s/%s", S3_BUCKET_NAME, s3Key);
            System.out.println("Upload complete: " + s3Url);
            return s3Url;

        } catch (Exception e) {
            System.err.println("Error uploading to S3: " + e.getMessage());
            throw e;
        }
    }

    // =================================================================================
    // UTILITY METHODS
    // =================================================================================

    /**
     * Escape HTML special characters to prevent XSS in error messages
     */
    private static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}