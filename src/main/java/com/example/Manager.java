package com.example;

import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Manager {

    private static volatile boolean terminationReceived = false;
    private static volatile boolean resultMonitorStarted = false;
    private static final ConcurrentHashMap<String, Integer> submittedTaskCounts = new ConcurrentHashMap<>();
    private static final AtomicInteger totalPendingTasks = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, String> activeWorkerIds = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newFixedThreadPool(20);

    public static void main(String[] args) {
        System.out.println("Manager started.");

        AWS aws = AWS.getInstance();

        try {
            // Create all necessary queues
            String inputQueueUrl = aws.createQueue(AWS.INPUT_QUEUE_NAME);
            String taskQueueUrl = aws.createQueue(AWS.TASK_QUEUE_NAME);
            String resultQueueUrl = aws.createQueue(AWS.RESULT_QUEUE_NAME);

            // Set visibility timeout for task queue
            aws.getSqsClient().setQueueAttributes(SetQueueAttributesRequest.builder()
                    .queueUrl(taskQueueUrl)
                    .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "600"))
                    .build());
            System.out.println("Task queue visibility timeout set.");

            // Discover existing workers
            discoverExistingWorkers(aws);
            System.out.println("Found " + activeWorkerIds.size() + " existing workers.");

            // Start result monitoring thread (ONLY ONCE)
            if (!resultMonitorStarted) {
                executor.submit(new ResultMonitor(aws, resultQueueUrl));
                resultMonitorStarted = true;
            }

            System.out.println("Manager ready. Polling for tasks...");

            // Main loop
            while (!terminationReceived || totalPendingTasks.get() > 0) {
                if (terminationReceived && totalPendingTasks.get() == 0) {
                    System.out.println("All tasks complete. Shutting down...");
                    break;
                }

                // Poll for new tasks if not terminating
                if (!terminationReceived) {
                    ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                            ReceiveMessageRequest.builder()
                                    .queueUrl(inputQueueUrl)
                                    .maxNumberOfMessages(10)
                                    .waitTimeSeconds(20)
                                    .build());

                    for (Message message : response.messages()) {
                        if (processClientMessage(aws, message, taskQueueUrl)) {
                            // Termination received
                            aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                    .queueUrl(inputQueueUrl)
                                    .receiptHandle(message.receiptHandle())
                                    .build());
                            break;
                        }

                        // Delete processed message
                        aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(inputQueueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());
                    }
                }

                TimeUnit.SECONDS.sleep(5);
            }

            // Cleanup and terminate
            cleanupAndTerminate(aws);

        } catch (Exception e) {
            System.err.println("Manager error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }
    }

    private static boolean processClientMessage(AWS aws, Message message, String taskQueueUrl) {
        String body = message.body();

        if (body.equals("TERMINATE")) {
            terminationReceived = true;
            System.out.println("\n*** TERMINATE command received. ***");
            return true;
        }

        if (terminationReceived) {
            System.out.println("Manager is terminating. Rejecting new job: " + body);
            return false;
        }

        // Parse: inputS3Url|doneQueueName|n
        String[] parts = body.split("\\|", 3);
        if (parts.length == 3) {
            executor.submit(() -> handleJob(aws, parts[0], parts[1], Integer.parseInt(parts[2]), taskQueueUrl));
        } else {
            System.err.println("Malformed message: " + body);
        }
        return false;
    }

    private static void handleJob(AWS aws, String inputS3Url, String doneQueueName, int n, String taskQueueUrl) {
        try {
            System.out.printf("\n[Job %s] Processing...\n", doneQueueName);

            // 1. Download and parse input file
            List<String> tasks = parseInputFile(aws, inputS3Url);
            System.out.printf("[Job %s] Found %d tasks.\n", doneQueueName, tasks.size());

            if (tasks.isEmpty()) return;

            // Track this job
            submittedTaskCounts.put(doneQueueName, tasks.size());
            totalPendingTasks.addAndGet(tasks.size());

            // 2. Calculate workers needed and launch
            int requiredWorkers = (int) Math.ceil((double) tasks.size() / n);
            int currentWorkers = activeWorkerIds.size();
            int workersToLaunch = requiredWorkers - currentWorkers;

            System.out.printf("[Job %s] Need %d workers, have %d, launching %d\n",
                    doneQueueName, requiredWorkers, currentWorkers, Math.max(0, workersToLaunch));

            if (workersToLaunch > 0) {
                launchWorkers(aws, workersToLaunch);
            }

            // 3. Send tasks to queue
            for (String task : tasks) {
                String taskMessage = task + "|" + doneQueueName;
                aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                        .queueUrl(taskQueueUrl)
                        .messageBody(taskMessage)
                        .build());
            }

            System.out.printf("[Job %s] Tasks sent to workers.\n", doneQueueName);

        } catch (Exception e) {
            System.err.println("Error handling job " + doneQueueName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void discoverExistingWorkers(AWS aws) {
        List<Instance> workers = aws.findInstancesByTag(AWS.WORKER_TAG_VALUE,
                InstanceStateName.RUNNING, InstanceStateName.PENDING);

        for (Instance worker : workers) {
            activeWorkerIds.put(worker.instanceId(), worker.instanceId());
        }
    }

    private static void launchWorkers(AWS aws, int count) {
        int activeInstances = activeWorkerIds.size() + 1;
        int available = AWS.MAX_INSTANCES - activeInstances;
        int toLaunch = Math.min(count, available);

        if (toLaunch <= 0) {
            System.out.println("Cannot launch workers - at capacity.");
            return;
        }

        String startupScript = String.join("\n",
                "#!/bin/bash",
                "exec > >(tee /var/log/user-data.log) 2>&1",
                "apt-get update -y",
                "apt-get install -y default-jdk unzip curl",
                "curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'",
                "unzip awscliv2.zip",
                "./aws/install",
                "cd /home/ubuntu",
                "/usr/local/bin/aws s3 cp s3://" + AWS.S3_BUCKET_NAME + "/" + AWS.WORKER_JAR_KEY + " Worker.jar",
                "/usr/local/bin/aws s3 cp s3://" + AWS.S3_BUCKET_NAME + "/jars/lib/ lib/ --recursive",  // ← ADD THIS LINE
                "java -cp Worker.jar:lib/* com.example.Worker " + AWS.TASK_QUEUE_NAME + " " + AWS.RESULT_QUEUE_NAME + " > worker.log 2>&1 &",                "echo 'Worker started'"
        );

        String userData = Base64.getEncoder().encodeToString(startupScript.getBytes());

        List<String> newWorkerIds = aws.launchInstances(toLaunch, userData, AWS.WORKER_TAG_VALUE);
        for (String id : newWorkerIds) {
            activeWorkerIds.put(id, id);
        }

        System.out.println("Launched " + toLaunch + " workers.");
    }

    private static List<String> parseInputFile(AWS aws, String s3Url) throws Exception {
        String s3UrlStripped = s3Url.substring(5);
        int firstSlash = s3UrlStripped.indexOf('/');
        String bucket = s3UrlStripped.substring(0, firstSlash);
        String key = s3UrlStripped.substring(firstSlash + 1);

        try (InputStreamReader isr = new InputStreamReader(
                aws.getS3Client().getObject(
                        GetObjectRequest.builder().bucket(bucket).key(key).build(),
                        ResponseTransformer.toInputStream()));
             BufferedReader br = new BufferedReader(isr)) {

            return br.lines()
                    .filter(line -> !line.trim().isEmpty())
                    .collect(Collectors.toList());
        }
    }

    private static class ResultMonitor implements Runnable {
        private final AWS aws;
        private final String resultQueueUrl;
        private final ConcurrentHashMap<String, List<String>> results = new ConcurrentHashMap<>();

        ResultMonitor(AWS aws, String resultQueueUrl) {
            this.aws = aws;
            this.resultQueueUrl = resultQueueUrl;
        }

        @Override
        public void run() {
            System.out.println("ResultMonitor started.");

            while (!terminationReceived || totalPendingTasks.get() > 0) {
                try {
                    ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                            ReceiveMessageRequest.builder()
                                    .queueUrl(resultQueueUrl)
                                    .maxNumberOfMessages(10)
                                    .waitTimeSeconds(20)
                                    .build());

                    for (Message message : response.messages()) {
                        String[] parts = message.body().split("\\|", 2);
                        if (parts.length != 2) {
                            aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                    .queueUrl(resultQueueUrl)
                                    .receiptHandle(message.receiptHandle())
                                    .build());
                            continue;
                        }

                        String doneQueueName = parts[0];
                        String resultLine = parts[1];

                        results.computeIfAbsent(doneQueueName,
                                        k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(resultLine);

                        totalPendingTasks.decrementAndGet();

                        // Check if job complete
                        Integer expected = submittedTaskCounts.get(doneQueueName);
                        List<String> jobResults = results.get(doneQueueName);

                        if (expected != null && jobResults != null && jobResults.size() == expected) {
                            System.out.printf("\n*** Job %s COMPLETE! ***\n", doneQueueName);

                            List<String> finalResults = results.remove(doneQueueName);
                            submittedTaskCounts.remove(doneQueueName);

                            createAndSendSummary(aws, doneQueueName, finalResults);
                        }

                        aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(resultQueueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());
                    }

                    TimeUnit.MILLISECONDS.sleep(500);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("ResultMonitor error: " + e.getMessage());
                }
            }

            System.out.println("ResultMonitor stopped.");
        }

        private void createAndSendSummary(AWS aws, String doneQueueName, List<String> results) {
            // Generate HTML
            StringBuilder html = new StringBuilder();
            html.append("<!DOCTYPE html>\n<html>\n<head>\n<title>Summary</title>\n</head>\n<body>\n");
            html.append("<h1>Text Analysis Summary</h1>\n<ul>\n");
            for (String line : results) {
                html.append("<li>").append(line).append("</li>\n");
            }
            html.append("</ul>\n</body>\n</html>");

            // Upload to S3
            String s3Key = "output/" + doneQueueName + "/summary.html";
            aws.getS3Client().putObject(
                    PutObjectRequest.builder()
                            .bucket(AWS.S3_BUCKET_NAME)
                            .key(s3Key)
                            .contentType("text/html")
                            .build(),
                    RequestBody.fromString(html.toString()));

            String summaryUrl = "s3://" + AWS.S3_BUCKET_NAME + "/" + s3Key;
            System.out.println("Summary uploaded: " + summaryUrl);

            // Send notification to client
            try {
                String doneQueueUrl = aws.getQueueUrl(doneQueueName);
                aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                        .queueUrl(doneQueueUrl)
                        .messageBody(summaryUrl)
                        .build());
            } catch (Exception e) {
                System.err.println("Error notifying client: " + e.getMessage());
            }
        }
    }

    private static void cleanupAndTerminate(AWS aws) {
        System.out.println("\n=== Manager Cleanup & Shutdown ===");

        // 1. Terminate all workers
        if (!activeWorkerIds.isEmpty()) {
            List<String> workerIds = new ArrayList<>(activeWorkerIds.keySet());
            aws.terminateInstances(workerIds);
            System.out.println("Workers terminated.");
        }

        // 2. Delete shared queues (assignment: clean up resources)
        System.out.println("Cleaning up Manager queues...");
        try {
            String inputQueueUrl = aws.getQueueUrl(AWS.INPUT_QUEUE_NAME);
            aws.deleteQueue(inputQueueUrl);
            System.out.println("Input queue deleted.");
        } catch (Exception e) {
            System.err.println("Note: Input queue cleanup: " + e.getMessage());
        }

        try {
            String taskQueueUrl = aws.getQueueUrl(AWS.TASK_QUEUE_NAME);
            aws.deleteQueue(taskQueueUrl);
            System.out.println("Task queue deleted.");
        } catch (Exception e) {
            System.err.println("Note: Task queue cleanup: " + e.getMessage());
        }

        try {
            String resultQueueUrl = aws.getQueueUrl(AWS.RESULT_QUEUE_NAME);
            aws.deleteQueue(resultQueueUrl);
            System.out.println("Result queue deleted.");
        } catch (Exception e) {
            System.err.println("Note: Result queue cleanup: " + e.getMessage());
        }

        // 3. Wait briefly for cleanup
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 4. Self-terminate
        String managerId = aws.getCurrentInstanceId();
        if (managerId != null) {
            aws.terminateInstances(List.of(managerId));
            System.out.println("Manager self-terminated.");
        } else {
            System.out.println("Could not get instance ID. Manual cleanup may be needed.");
        }
    }
}