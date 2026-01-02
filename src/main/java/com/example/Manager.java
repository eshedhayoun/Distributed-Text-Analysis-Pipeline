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
            String inputQueueUrl = aws.createQueue(AWS.INPUT_QUEUE_NAME);
            String taskQueueUrl = aws.createQueue(AWS.TASK_QUEUE_NAME);
            String resultQueueUrl = aws.createQueue(AWS.RESULT_QUEUE_NAME);

            aws.getSqsClient().setQueueAttributes(SetQueueAttributesRequest.builder()
                    .queueUrl(taskQueueUrl)
                    .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "600"))
                    .build());
            System.out.println("Task queue visibility timeout set.");

            discoverExistingWorkers(aws);
            System.out.println("Found " + activeWorkerIds.size() + " existing workers.");

            restoreStateFromS3(aws);

            if (!resultMonitorStarted) {
                executor.submit(new ResultMonitor(aws, resultQueueUrl));
                resultMonitorStarted = true;
            }

            signalWatchdogReady(aws);

            System.out.println("Manager ready. Polling for tasks...");

            long terminationStartTime = 0;

            while (!terminationReceived || totalPendingTasks.get() > 0) {
                if (terminationReceived && totalPendingTasks.get() == 0) {
                    System.out.println("All tasks complete. Shutting down...");
                    break;
                }

                if (terminationReceived) {
                    if (terminationStartTime == 0) {
                        terminationStartTime = System.currentTimeMillis();
                        System.out.println("Termination received. Waiting for " + totalPendingTasks.get() + " pending tasks...");
                    } else {
                        long waitTime = System.currentTimeMillis() - terminationStartTime;
                        if (waitTime > 120000) {
                            System.out.println("⚠️ Termination timeout after 2 minutes!");
                            System.out.println("Forcing shutdown with " + totalPendingTasks.get() + " pending tasks remaining.");
                            break;
                        }

                        if (waitTime % 10000 < 5000) {
                            System.out.println("Waiting for " + totalPendingTasks.get() + " pending tasks... (" + (waitTime / 1000) + "s)");
                        }
                    }
                }

                if (!terminationReceived) {
                    ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                            ReceiveMessageRequest.builder()
                                    .queueUrl(inputQueueUrl)
                                    .maxNumberOfMessages(10)
                                    .waitTimeSeconds(20)
                                    .build());

                    for (Message message : response.messages()) {
                        if (processClientMessage(aws, message, taskQueueUrl)) {
                            aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                    .queueUrl(inputQueueUrl)
                                    .receiptHandle(message.receiptHandle())
                                    .build());
                            break;
                        }

                        aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(inputQueueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());
                    }
                } else {
                    TimeUnit.SECONDS.sleep(5);
                }

                TimeUnit.SECONDS.sleep(2);
            }

            System.out.println("=== Exiting main loop ===");
            System.out.println("terminationReceived: " + terminationReceived);
            System.out.println("totalPendingTasks: " + totalPendingTasks.get());

            cleanupAndTerminate(aws);

            System.out.println("=== Cleanup complete ===");

        } catch (Exception e) {
            System.err.println("Manager error: " + e.getMessage());
            e.printStackTrace();

            try {
                cleanupAndTerminate(aws);
            } catch (Exception cleanupError) {
                System.err.println("Cleanup failed: " + cleanupError.getMessage());
            }
        } finally {
            executor.shutdownNow();
            System.out.println("Manager terminated.");
        }
    }

    private static void signalWatchdogReady(AWS aws) {
        try {
            String watchdogQueueUrl = aws.createQueue(AWS.WATCHDOG_QUEUE_NAME);
            aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                    .queueUrl(watchdogQueueUrl)
                    .messageBody("MANAGER_READY")
                    .build());
            System.out.println("✅ Sent ready signal to watchdog");
        } catch (Exception e) {
            System.err.println("Failed to signal watchdog: " + e.getMessage());
        }
    }

    private static void saveJobMetadataToS3(AWS aws, String jobId, int totalTasks) {
        String s3Key = "manager-state/" + jobId + "/metadata.json";

        String json = String.format(
                "{\"jobId\":\"%s\",\"totalTasks\":%d,\"status\":\"PROCESSING\",\"timestamp\":%d}",
                jobId, totalTasks, System.currentTimeMillis()
        );

        try {
            aws.getS3Client().putObject(
                    PutObjectRequest.builder()
                            .bucket(AWS.S3_BUCKET_NAME)
                            .key(s3Key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(json)
            );
            System.out.println("Saved job metadata to S3: " + jobId);
        } catch (Exception e) {
            System.err.println("Error saving job metadata: " + e.getMessage());
        }
    }

    private static void saveResultToS3(AWS aws, String jobId, String resultLine) {
        String resultId = UUID.randomUUID().toString();
        String s3Key = "manager-state/" + jobId + "/results/" + resultId + ".txt";

        try {
            aws.getS3Client().putObject(
                    PutObjectRequest.builder()
                            .bucket(AWS.S3_BUCKET_NAME)
                            .key(s3Key)
                            .contentType("text/plain")
                            .build(),
                    RequestBody.fromString(resultLine)
            );
        } catch (Exception e) {
            System.err.println("Error saving result to S3: " + e.getMessage());
        }
    }

    private static void markJobCompleteInS3(AWS aws, String jobId) {
        String s3Key = "manager-state/" + jobId + "/metadata.json";

        String json = String.format(
                "{\"jobId\":\"%s\",\"status\":\"COMPLETE\",\"completedAt\":%d}",
                jobId, System.currentTimeMillis()
        );

        try {
            aws.getS3Client().putObject(
                    PutObjectRequest.builder()
                            .bucket(AWS.S3_BUCKET_NAME)
                            .key(s3Key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(json)
            );
            System.out.println("Marked job complete in S3: " + jobId);
        } catch (Exception e) {
            System.err.println("Error marking job complete: " + e.getMessage());
        }
    }

    private static void restoreStateFromS3(AWS aws) {
        System.out.println("Restoring manager state from S3...");

        try {
            ListObjectsV2Response response = aws.getS3Client().listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(AWS.S3_BUCKET_NAME)
                            .prefix("manager-state/")
                            .build()
            );

            Map<String, Integer> jobsToRestore = new HashMap<>();

            for (S3Object s3Object : response.contents()) {
                String key = s3Object.key();

                if (key.endsWith("/metadata.json")) {
                    String[] parts = key.split("/");
                    if (parts.length >= 2) {
                        String jobId = parts[1];

                        try {
                            String json = aws.getS3Client().getObjectAsBytes(
                                    GetObjectRequest.builder()
                                            .bucket(AWS.S3_BUCKET_NAME)
                                            .key(key)
                                            .build()
                            ).asUtf8String();

                            if (json.contains("\"status\":\"PROCESSING\"")) {
                                String[] jsonParts = json.split("\"totalTasks\":");
                                if (jsonParts.length > 1) {
                                    String numStr = jsonParts[1].split(",")[0].trim();
                                    int totalTasks = Integer.parseInt(numStr);
                                    jobsToRestore.put(jobId, totalTasks);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing metadata for " + jobId + ": " + e.getMessage());
                        }
                    }
                }
            }

            if (jobsToRestore.isEmpty()) {
                System.out.println("No in-progress jobs to restore.");
                return;
            }

            System.out.println("Found " + jobsToRestore.size() + " in-progress jobs to restore.");

            for (Map.Entry<String, Integer> entry : jobsToRestore.entrySet()) {
                String jobId = entry.getKey();
                int totalTasks = entry.getValue();

                ListObjectsV2Response resultsResponse = aws.getS3Client().listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(AWS.S3_BUCKET_NAME)
                                .prefix("manager-state/" + jobId + "/results/")
                                .build()
                );

                List<String> existingResults = new ArrayList<>();

                for (S3Object resultObj : resultsResponse.contents()) {
                    try {
                        String resultLine = aws.getS3Client().getObjectAsBytes(
                                GetObjectRequest.builder()
                                        .bucket(AWS.S3_BUCKET_NAME)
                                        .key(resultObj.key())
                                        .build()
                        ).asUtf8String();

                        existingResults.add(resultLine);
                    } catch (Exception e) {
                        System.err.println("Error reading result: " + e.getMessage());
                    }
                }

                submittedTaskCounts.put(jobId, totalTasks);
                ResultMonitor.results.put(jobId, Collections.synchronizedList(new ArrayList<>(existingResults)));

                int pendingTasks = totalTasks - existingResults.size();
                totalPendingTasks.addAndGet(pendingTasks);

                System.out.printf("✅ Restored job %s: %d/%d tasks complete (%d pending)\n",
                        jobId, existingResults.size(), totalTasks, pendingTasks);

                if (existingResults.size() == totalTasks) {
                    System.out.println("Job " + jobId + " was complete! Creating summary...");
                    final List<String> finalResults = existingResults;
                    executor.submit(() -> {
                        try {
                            ResultMonitor.createAndSendSummaryStatic(aws, jobId, finalResults);
                            markJobCompleteInS3(aws, jobId);
                            submittedTaskCounts.remove(jobId);
                            ResultMonitor.results.remove(jobId);
                        } catch (Exception e) {
                            System.err.println("Error creating summary for restored job: " + e.getMessage());
                        }
                    });
                }
            }

            System.out.println("State restoration complete!");

        } catch (Exception e) {
            System.err.println("Error restoring state from S3: " + e.getMessage());
            e.printStackTrace();
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

            List<String> tasks = parseInputFile(aws, inputS3Url);
            System.out.printf("[Job %s] Found %d tasks.\n", doneQueueName, tasks.size());

            if (tasks.isEmpty()) return;

            saveJobMetadataToS3(aws, doneQueueName, tasks.size());

            submittedTaskCounts.put(doneQueueName, tasks.size());
            totalPendingTasks.addAndGet(tasks.size());

            int requiredWorkers = (int) Math.ceil((double) tasks.size() / n);
            int currentWorkers = activeWorkerIds.size();
            int workersToLaunch = requiredWorkers - currentWorkers;

            System.out.printf("[Job %s] Need %d workers, have %d, launching %d\n",
                    doneQueueName, requiredWorkers, currentWorkers, Math.max(0, workersToLaunch));

            if (workersToLaunch > 0) {
                launchWorkers(aws, workersToLaunch);
            }

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
                "/usr/local/bin/aws s3 cp s3://" + AWS.S3_BUCKET_NAME + "/jars/lib/ lib/ --recursive",
                "java -cp Worker.jar:lib/* com.example.Worker " + AWS.TASK_QUEUE_NAME + " " + AWS.RESULT_QUEUE_NAME + " > worker.log 2>&1 &",
                "echo 'Worker started'"
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
        static final ConcurrentHashMap<String, List<String>> results = new ConcurrentHashMap<>();

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

                        saveResultToS3(aws, doneQueueName, resultLine);

                        results.computeIfAbsent(doneQueueName,
                                        k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(resultLine);

                        totalPendingTasks.decrementAndGet();

                        Integer expected = submittedTaskCounts.get(doneQueueName);
                        List<String> jobResults = results.get(doneQueueName);

                        if (expected != null && jobResults != null && jobResults.size() == expected) {
                            System.out.printf("\n*** Job %s COMPLETE! ***\n", doneQueueName);

                            List<String> finalResults = results.remove(doneQueueName);
                            submittedTaskCounts.remove(doneQueueName);

                            createAndSendSummary(aws, doneQueueName, finalResults);
                            markJobCompleteInS3(aws, doneQueueName);
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
            createAndSendSummaryStatic(aws, doneQueueName, results);
        }

        static void createAndSendSummaryStatic(AWS aws, String doneQueueName, List<String> results) {
            StringBuilder html = new StringBuilder();
            html.append("<!DOCTYPE html>\n<html>\n<head>\n<title>Summary</title>\n</head>\n<body>\n");
            html.append("<h1>Text Analysis Summary</h1>\n<ul>\n");
            for (String line : results) {
                html.append("<li>").append(line).append("</li>\n");
            }
            html.append("</ul>\n</body>\n</html>");

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

        if (!activeWorkerIds.isEmpty()) {
            System.out.println("Terminating " + activeWorkerIds.size() + " workers...");
            List<String> workerIds = new ArrayList<>(activeWorkerIds.keySet());

            try {
                aws.terminateInstances(workerIds);
                System.out.println("✅ Workers terminated: " + workerIds);
            } catch (Exception e) {
                System.err.println("Error terminating workers: " + e.getMessage());
            }
        } else {
            System.out.println("No workers to terminate.");
        }

        System.out.println("Cleaning up Manager queues...");

        String[] queuesToDelete = {
                AWS.INPUT_QUEUE_NAME,
                AWS.TASK_QUEUE_NAME,
                AWS.RESULT_QUEUE_NAME,
                AWS.WATCHDOG_QUEUE_NAME
        };

        for (String queueName : queuesToDelete) {
            try {
                String queueUrl = aws.getQueueUrl(queueName);
                aws.deleteQueue(queueUrl);
                System.out.println("✅ Deleted queue: " + queueName);
            } catch (QueueDoesNotExistException e) {
                System.out.println("Queue doesn't exist: " + queueName);
            } catch (Exception e) {
                System.err.println("Error deleting queue " + queueName + ": " + e.getMessage());
            }
        }

        try {
            System.out.println("Waiting 5 seconds before self-termination...");
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        String managerId = aws.getCurrentInstanceId();
        if (managerId != null) {
            System.out.println("Self-terminating manager: " + managerId);
            try {
                aws.terminateInstances(List.of(managerId));
                System.out.println("✅ Manager self-terminated.");
            } catch (Exception e) {
                System.err.println("Error self-terminating: " + e.getMessage());
            }
        } else {
            System.out.println("⚠️ Could not get instance ID. Manual cleanup may be needed.");
        }
    }
}