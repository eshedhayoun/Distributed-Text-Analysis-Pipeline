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

import software.amazon.awssdk.services.s3.model.PutObjectRequest;
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

    private static final int MAX_TEXT_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB
    private static final int DOWNLOAD_TIMEOUT_MS = 60000; // 1 minute
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

        // Initialize NLP pipeline
        initializeNLPPipeline();

        AWS aws = AWS.getInstance();

        try {
            String taskQueueUrl = aws.getQueueUrl(taskQueueName);
            String resultQueueUrl = aws.getQueueUrl(resultQueueName);

            System.out.println("Worker ready. Processing tasks...");

            // Main processing loop
            while (true) {
                try {
                    ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                            ReceiveMessageRequest.builder()
                                    .queueUrl(taskQueueUrl)
                                    .maxNumberOfMessages(1)
                                    .waitTimeSeconds(20)
                                    .build());

                    List<Message> messages = response.messages();
                    if (messages.isEmpty()) {
                        TimeUnit.SECONDS.sleep(2);
                        continue;
                    }

                    for (Message message : messages) {
                        processTask(aws, message, taskQueueUrl, resultQueueUrl);
                    }

                } catch (InterruptedException e) {
                    System.out.println("Worker interrupted. Shutting down...");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Worker error: " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Worker terminated.");
    }

    private static void initializeNLPPipeline() {
        System.out.println("Initializing Stanford CoreNLP...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse");
        props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
        props.setProperty("tokenize.language", "en");
        pipeline = new StanfordCoreNLP(props);
        System.out.println("Pipeline ready.");
    }

    private static void processTask(AWS aws, Message message, String taskQueueUrl, String resultQueueUrl) {
        String body = message.body();
        String receiptHandle = message.receiptHandle();

        System.out.println("\n--- Processing task ---");

        try {
            // Parse: ANALYSIS_TYPE\tURL|DONE_QUEUE_NAME
            String[] mainParts = body.split("\\|", 2);
            if (mainParts.length != 2) {
                throw new IllegalArgumentException("Invalid message format");
            }

            String taskPart = mainParts[0];
            String doneQueueName = mainParts[1];

            String[] taskDetails = taskPart.split("\t", 2);
            if (taskDetails.length != 2) {
                throw new IllegalArgumentException("Invalid task format");
            }

            String analysisType = taskDetails[0].trim().toUpperCase();
            String inputUrl = taskDetails[1].trim();

            System.out.println("Type: " + analysisType);
            System.out.println("URL: " + inputUrl);

            // Validate analysis type
            if (!analysisType.equals("POS") && !analysisType.equals("CONSTITUENCY") &&
                    !analysisType.equals("DEPENDENCY")) {
                throw new IllegalArgumentException("Unknown analysis type: " + analysisType);
            }

            // 1. Download text
            String text = downloadText(inputUrl);

            // 2. Perform analysis
            String analysisResult = performAnalysis(text, analysisType);

            // 3. Upload result to S3
            String outputS3Url = uploadResultToS3(aws, analysisResult, doneQueueName, analysisType);

            // 4. Send result to manager
            String resultMessage = String.format("%s|%s: <a href=\"%s\">%s</a> <a href=\"%s\">output</a>",
                    doneQueueName, analysisType, inputUrl, inputUrl, outputS3Url);

            aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                    .queueUrl(resultQueueUrl)
                    .messageBody(resultMessage)
                    .build());

            System.out.println("Task complete.");

        } catch (Exception e) {
            System.err.println("Task failed: " + e.getMessage());

            // Send error result
            try {
                String[] mainParts = body.split("\\|", 2);
                String doneQueueName = mainParts.length > 1 ? mainParts[1] : "unknown";

                String[] taskDetails = mainParts[0].split("\t", 2);
                String analysisType = taskDetails.length > 0 ? taskDetails[0].trim() : "UNKNOWN";
                String inputUrl = taskDetails.length > 1 ? taskDetails[1].trim() : "unknown";

                String errorMessage = String.format("%s|%s: <a href=\"%s\">%s</a> ERROR: %s",
                        doneQueueName, analysisType, inputUrl, inputUrl, escapeHtml(e.getMessage()));

                aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                        .queueUrl(resultQueueUrl)
                        .messageBody(errorMessage)
                        .build());

            } catch (Exception e2) {
                System.err.println("Failed to send error message: " + e2.getMessage());
            }
        } finally {
            // Always delete message
            aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(taskQueueUrl)
                    .receiptHandle(receiptHandle)
                    .build());
        }
    }

    private static String downloadText(String urlString) throws IOException {
        System.out.println("Downloading from: " + urlString);

        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(DOWNLOAD_TIMEOUT_MS);
        conn.setReadTimeout(DOWNLOAD_TIMEOUT_MS);
        conn.setRequestProperty("User-Agent", "Mozilla/5.0");

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new IOException("HTTP error: " + responseCode);
        }

        long contentLength = conn.getContentLengthLong();
        if (contentLength > MAX_TEXT_SIZE_BYTES) {
            throw new IOException("File too large: " + contentLength + " bytes");
        }

        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), "UTF-8"))) {

            String line;
            int totalBytes = 0;
            while ((line = reader.readLine()) != null) {
                totalBytes += line.getBytes("UTF-8").length;
                if (totalBytes > MAX_TEXT_SIZE_BYTES) {
                    throw new IOException("File exceeded size limit");
                }
                content.append(line).append("\n");
            }
        }

        System.out.println("Downloaded " + content.length() + " chars.");
        return content.toString();
    }

    private static String performAnalysis(String text, String analysisType) {
        System.out.println("Performing " + analysisType + " analysis...");

        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("Text is empty");
        }

        StringBuilder result = new StringBuilder();
        String[] lines = text.split("\n");
        int processed = 0;

        for (int i = 0; i < lines.length && processed < 100; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;

            try {
                String lineResult = analyzeLine(line, analysisType);
                result.append("Line ").append(i + 1).append(":\n");
                result.append(lineResult).append("\n\n");
                processed++;
            } catch (Exception e) {
                result.append("Line ").append(i + 1).append(": ERROR - ")
                        .append(e.getMessage()).append("\n\n");
            }
        }

        if (processed >= 100) {
            result.append("[Truncated after 100 lines]\n");
        }

        System.out.println("Analysis complete. Processed " + processed + " lines.");
        return result.toString();
    }

    private static String analyzeLine(String line, String analysisType) {
        Annotation document = new Annotation(line);
        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        StringBuilder result = new StringBuilder();

        for (CoreMap sentence : sentences) {
            switch (analysisType) {
                case "POS":
                    result.append(performPOS(sentence));
                    break;
                case "CONSTITUENCY":
                    result.append(performConstituency(sentence));
                    break;
                case "DEPENDENCY":
                    result.append(performDependency(sentence));
                    break;
            }
            result.append("\n");
        }

        return result.toString();
    }

    private static String performPOS(CoreMap sentence) {
        StringBuilder result = new StringBuilder();
        List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);

        for (CoreLabel token : tokens) {
            String word = token.get(CoreAnnotations.TextAnnotation.class);
            String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
            result.append(word).append("/").append(pos).append(" ");
        }

        return result.toString().trim();
    }

    private static String performConstituency(CoreMap sentence) {
        Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
        return tree.toString();
    }

    private static String performDependency(CoreMap sentence) {
        SemanticGraph dependencies = sentence.get(
                SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class);
        return dependencies.toString();
    }

    private static String uploadResultToS3(AWS aws, String content, String doneQueueName, String analysisType) {
        String fileName = String.format("%s-%s-%s.txt",
                analysisType.toLowerCase(),
                UUID.randomUUID().toString(),
                System.currentTimeMillis());

        String s3Key = "results/" + doneQueueName + "/" + fileName;

        aws.getS3Client().putObject(
                PutObjectRequest.builder()
                        .bucket(AWS.S3_BUCKET_NAME)
                        .key(s3Key)
                        .contentType("text/plain")
                        .build(),
                RequestBody.fromString(content));

        String s3Url = "s3://" + AWS.S3_BUCKET_NAME + "/" + s3Key;
        System.out.println("Uploaded: " + s3Url);
        return s3Url;
    }

    private static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}