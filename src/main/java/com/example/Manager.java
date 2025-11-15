package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {

    // --- Configuration Constants ---
    private static final Region REGION = Region.EU_WEST_2;
    private static final String S3_BUCKET_NAME = "your-unique-bucket-name-here";
    private static final String INPUT_QUEUE_NAME = "Client_Manager_Queue";
    private static final String TASK_QUEUE_NAME = "Manager_Worker_TaskQueue";
    private static final String RESULT_QUEUE_NAME = "Manager_Results_Queue";

    private static final int MAX_TOTAL_INSTANCES = 19;
    private static final String WORKER_AMI_ID = "ami-xxxxxxxxxxxxxxxxx";
    private static final String WORKER_INSTANCE_TYPE = InstanceType.T2_MICRO.toString();
    private static final String WORKER_TAG_KEY = "Role";
    private static final String WORKER_TAG_VALUE = "Worker";
    private static final String WORKER_JAR_KEY = "jars/Worker.jar";
    private static final String STANFORD_JAR_KEY = "jars/stanford-corenlp-4.5.1.jar";
    private static final String METADATA_URL = "http://169.254.169.254/latest/meta-data/instance-id";

    // SQS Visibility Timeout - gives workers time to process before message becomes visible again
    private static final int VISIBILITY_TIMEOUT_SECONDS = 120; // 2 minutes per task. more then the required 90 for completion

    // --- State Tracking ---
    private static volatile boolean terminationReceived = false;
    // Map tracks active jobs by their DoneQueueName and the TOTAL tasks submitted
    private static final ConcurrentHashMap<String, Integer> submittedTaskCounts = new ConcurrentHashMap<>();
    // Counts how many tasks across ALL jobs are currently processing/pending
    private static final AtomicInteger totalPendingTasks = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, String> activeWorkerInstanceIds = new ConcurrentHashMap<>();

    // ExecutorService for concurrent handling of client requests
    private static final ExecutorService concurrentExecutor = Executors.newFixedThreadPool(20);

    public static void main(String[] args) {
        System.out.println("Manager node started. Initializing...");

        try (SqsClient sqs = SqsClient.builder().region(REGION).build();
             S3Client s3 = S3Client.builder().region(REGION).build();
             Ec2Client ec2 = Ec2Client.builder().region(REGION).build()) {

            // 0. Ensure all necessary shared SQS Queues exist
            String inputQueueUrl = getOrCreateQueueUrl(sqs, INPUT_QUEUE_NAME);
            String taskQueueUrl = getOrCreateQueueUrl(sqs, TASK_QUEUE_NAME);
            String resultQueueUrl = getOrCreateQueueUrl(sqs, RESULT_QUEUE_NAME);

            // Configure visibility timeout for task queue (critical for fault tolerance)
            configureQueueVisibilityTimeout(sqs, taskQueueUrl, VISIBILITY_TIMEOUT_SECONDS);

            // Discover already running workers (from previous jobs or crashes)
            discoverExistingWorkers(ec2);
            System.out.println("Found " + activeWorkerInstanceIds.size() + " existing worker instances.");

            // Start result monitoring thread
            concurrentExecutor.submit(new WorkerResultMonitor(sqs, s3, resultQueueUrl));

            System.out.println("Manager ready. Polling loop started.");

            // Main polling loop
            while (!terminationReceived || totalPendingTasks.get() > 0) {

                if (terminationReceived && totalPendingTasks.get() == 0) {
                    System.out.println("All tasks completed. Preparing to terminate.");
                    break;
                }

                // Poll input queue if not terminating
                if (!terminationReceived) {
                    List<Message> messages = receiveMessages(sqs, inputQueueUrl, 10);

                    for (Message message : messages) {
                        if (processIncomingMessage(sqs, s3, ec2, message, taskQueueUrl)) {
                            // Termination message received
                            sqs.deleteMessage(DeleteMessageRequest.builder()
                                    .queueUrl(inputQueueUrl)
                                    .receiptHandle(message.receiptHandle())
                                    .build());
                            break;
                        }
                        // Delete the message after handing it off
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(inputQueueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());
                    }
                }

                TimeUnit.SECONDS.sleep(5);
            }

            // Cleanup and Self-Terminate
            cleanupAndTerminate(ec2);

        } catch (Exception e) {
            System.err.println("Manager encountered a fatal error: " + e.getMessage());
            e.printStackTrace();
            // Attempt to terminate workers even on crash
            try (Ec2Client ec2 = Ec2Client.builder().region(REGION).build()) {
                cleanupWorkers(ec2);
            } catch (Exception ignored) {}
        } finally {
            concurrentExecutor.shutdownNow();
        }
    }

    // =================================================================================
    // SQS HELPERS
    // =================================================================================

    private static String getQueueUrl(SqsClient sqs, String queueName) throws SqsException {
        return sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
    }

    private static String getOrCreateQueueUrl(SqsClient sqs, String queueName) throws SqsException {
        try {
            return getQueueUrl(sqs, queueName);
        } catch (QueueDoesNotExistException e) {
            System.out.printf("Queue %s not found. Creating it.\n", queueName);
            return sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl();
        }
    }

    private static void configureQueueVisibilityTimeout(SqsClient sqs, String queueUrl, int timeoutSeconds) {
        try {
            sqs.setQueueAttributes(SetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributes(Collections.singletonMap(
                            QueueAttributeName.VISIBILITY_TIMEOUT,
                            String.valueOf(timeoutSeconds)
                    ))
                    .build());
            System.out.println("Configured visibility timeout for task queue: " + timeoutSeconds + " seconds");
        } catch (SqsException e) {
            System.err.println("Error configuring queue visibility timeout: " + e.getMessage());
        }
    }

    private static List<Message> receiveMessages(SqsClient sqs, String queueUrl, int maxMessages) {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(maxMessages)
                    .waitTimeSeconds(20) // Long polling
                    .build();
            return sqs.receiveMessage(receiveRequest).messages();
        } catch (SqsException e) {
            System.err.println("Error receiving messages: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Processes the incoming message from the Local Application.
     * @return true if the message is a TERMINATE command, false otherwise.
     */
    private static boolean processIncomingMessage(SqsClient sqs, S3Client s3, Ec2Client ec2,
                                                  Message message, String taskQueueUrl) {
        String body = message.body();

        if (body.equals("TERMINATE")) {
            terminationReceived = true;
            System.out.println("\n*** Received TERMINATE command. Stopping new task acceptance. ***");
            return true;
        }

        // Parse message: inputS3Url|doneQueueName|n
        String[] parts = body.split("\\|", 3);
        if (parts.length == 3) {
            String inputS3Url = parts[0];
            String doneQueueName = parts[1];
            int n = Integer.parseInt(parts[2]);

            // Delegate the orchestration to a separate thread for concurrency
            concurrentExecutor.submit(() ->
                    orchestrateWorkload(sqs, s3, ec2, inputS3Url, doneQueueName, n, taskQueueUrl));

        } else {
            System.err.println("Unknown/Malformed message received: " + body);
        }
        return false;
    }

    // =================================================================================
    // ORCHESTRATION & SCALING
    // =================================================================================

    private static void orchestrateWorkload(SqsClient sqs, S3Client s3, Ec2Client ec2,
                                            String inputS3Url, String doneQueueName, int n,
                                            String taskQueueUrl) {
        try {
            System.out.printf("\n[Job %s] Starting orchestration...\n", doneQueueName);

            // 1. Download and parse the input file from S3
            List<String> rawTasks = parseInputFile(s3, inputS3Url);
            int totalTasks = rawTasks.size();
            System.out.printf("[Job %s] Parsed %d total tasks.\n", doneQueueName, totalTasks);

            if (totalTasks == 0) {
                System.err.println("[Job " + doneQueueName + "] No tasks found in input file.");
                return;
            }

            // Update global task count and track submitted tasks for this job
            submittedTaskCounts.put(doneQueueName, totalTasks);
            totalPendingTasks.addAndGet(totalTasks);

            // 2. Calculate required workers and launch if needed
            // m = ceiling(totalTasks / n) - required workers for this job
            // k = current active workers
            // Launch m - k workers (if m > k)
            int requiredWorkers = (int) Math.ceil((double) totalTasks / n);
            int currentActiveWorkers = activeWorkerInstanceIds.size();
            int workersToLaunch = requiredWorkers - currentActiveWorkers;

            System.out.printf("[Job %s] Required workers: %d, Active workers: %d, Will launch: %d\n",
                    doneQueueName, requiredWorkers, currentActiveWorkers, Math.max(0, workersToLaunch));

            if (workersToLaunch > 0) {
                launchWorkers(ec2, workersToLaunch);
            }

            // 3. Create SQS messages for each task
            // All workers pull from the same queue (shared task queue)
            delegateTasksToSharedQueue(sqs, taskQueueUrl, rawTasks, doneQueueName);

            System.out.printf("[Job %s] All tasks delegated to queue.\n", doneQueueName);

        } catch (Exception e) {
            System.err.println("Orchestration failed for job " + doneQueueName + ": " + e.getMessage());
            e.printStackTrace();
            // Clean up tracking for this failed job
            submittedTaskCounts.remove(doneQueueName);
            totalPendingTasks.addAndGet(-submittedTaskCounts.getOrDefault(doneQueueName, 0));
        }
    }

    private static void discoverExistingWorkers(Ec2Client ec2) {
        try {
            Filter runningFilter = Filter.builder()
                    .name("instance-state-name")
                    .values(InstanceStateName.RUNNING.toString(), InstanceStateName.PENDING.toString())
                    .build();

            Filter workerTagFilter = Filter.builder()
                    .name("tag:" + WORKER_TAG_KEY)
                    .values(WORKER_TAG_VALUE)
                    .build();

            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(runningFilter, workerTagFilter)
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);

            response.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .forEach(instance -> activeWorkerInstanceIds.put(instance.instanceId(), instance.instanceId()));

        } catch (Ec2Exception e) {
            System.err.println("Error discovering existing workers: " + e.getMessage());
        }
    }

    private static void launchWorkers(Ec2Client ec2, int workersToLaunch) {
        // Calculate available capacity
        int activeManagersAndWorkers = activeWorkerInstanceIds.size() + 1; // +1 for Manager itself
        int availableCapacity = MAX_TOTAL_INSTANCES - activeManagersAndWorkers;
        int finalLaunchCount = Math.min(workersToLaunch, availableCapacity);

        if (finalLaunchCount <= 0) {
            System.out.println("Cannot launch workers - at capacity limit or insufficient space.");
            return;
        }

        System.out.println("Launching " + finalLaunchCount + " worker instance(s)...");

        // User data script - corrected for Ubuntu
        String startupScript = String.join("\n",
                "#!/bin/bash",
                "exec > >(tee /var/log/user-data.log) 2>&1",
                "echo 'Starting worker initialization...'",
                "apt-get update -y",
                "apt-get install -y default-jdk awscli",
                "echo 'Java and AWS CLI installed'",
                "aws s3 cp s3://" + S3_BUCKET_NAME + "/" + STANFORD_JAR_KEY + " /home/ubuntu/stanford-corenlp.jar",
                "aws s3 cp s3://" + S3_BUCKET_NAME + "/" + WORKER_JAR_KEY + " /home/ubuntu/Worker.jar",
                "echo 'JARs downloaded'",
                "cd /home/ubuntu",
                "nohup java -Xmx4g -cp stanford-corenlp.jar:Worker.jar com.example.Worker " +
                        TASK_QUEUE_NAME + " " + RESULT_QUEUE_NAME + " > worker.log 2>&1 &",
                "echo 'Worker started'"
        );

        String userData = java.util.Base64.getEncoder().encodeToString(startupScript.getBytes());

        try {
            RunInstancesResponse response = ec2.runInstances(RunInstancesRequest.builder()
                    .imageId(WORKER_AMI_ID)
                    .instanceType(InstanceType.fromValue(WORKER_INSTANCE_TYPE))
                    .minCount(finalLaunchCount)
                    .maxCount(finalLaunchCount)
                    .userData(userData)
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.INSTANCE)
                            .tags(Tag.builder()
                                    .key(WORKER_TAG_KEY)
                                    .value(WORKER_TAG_VALUE)
                                    .build())
                            .build())
                    .build());

            for (Instance instance : response.instances()) {
                activeWorkerInstanceIds.put(instance.instanceId(), instance.instanceId());
                System.out.println("Launched worker: " + instance.instanceId());
            }

        } catch (Ec2Exception e) {
            System.err.println("Error launching workers: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void delegateTasksToSharedQueue(SqsClient sqs, String taskQueueUrl,
                                                   List<String> rawTasks, String doneQueueName) {
        // Task message format: ANALYSIS_TYPE\tURL|DONE_QUEUE_NAME
        // This allows worker to know where to send results
        for (String taskLine : rawTasks) {
            String workerMessage = taskLine + "|" + doneQueueName;
            try {
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(taskQueueUrl)
                        .messageBody(workerMessage)
                        .build());
            } catch (SqsException e) {
                System.err.println("Error sending task to queue: " + e.getMessage());
            }
        }
    }

    private static List<String> parseInputFile(S3Client s3, String s3Url) throws Exception {
        // Parse s3://bucket/key format
        if (!s3Url.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 URL format: " + s3Url);
        }

        String s3UrlStripped = s3Url.substring(5); // Remove "s3://"
        int firstSlash = s3UrlStripped.indexOf('/');

        if (firstSlash == -1) {
            throw new IllegalArgumentException("Invalid S3 URL format: " + s3Url);
        }

        String bucket = s3UrlStripped.substring(0, firstSlash);
        String key = s3UrlStripped.substring(firstSlash + 1);

        try (InputStreamReader isr = new InputStreamReader(
                s3.getObject(
                        GetObjectRequest.builder().bucket(bucket).key(key).build(),
                        ResponseTransformer.toInputStream()));
             BufferedReader br = new BufferedReader(isr)) {

            return br.lines()
                    .filter(line -> !line.trim().isEmpty()) // Filter empty lines
                    .collect(Collectors.toList());
        }
    }

    // =================================================================================
    // RESULT AGGREGATION & MONITORING
    // =================================================================================

    /**
     * Runnable dedicated to continuously polling the worker result queue and finalizing jobs.
     */
    private static class WorkerResultMonitor implements Runnable {
        private final SqsClient sqs;
        private final S3Client s3;
        private final String resultQueueUrl;
        private final ConcurrentHashMap<String, List<String>> resultAggregators = new ConcurrentHashMap<>();

        public WorkerResultMonitor(SqsClient sqs, S3Client s3, String resultQueueUrl) {
            this.sqs = sqs;
            this.s3 = s3;
            this.resultQueueUrl = resultQueueUrl;
        }

        @Override
        public void run() {
            System.out.println("WorkerResultMonitor started.");

            while (!terminationReceived || totalPendingTasks.get() > 0) {
                try {
                    List<Message> results = receiveMessages(sqs, resultQueueUrl, 10);

                    for (Message result : results) {
                        // Worker result format: DONE_QUEUE_NAME|HTML_RESULT_LINE
                        String[] parts = result.body().split("\\|", 2);
                        if (parts.length != 2) {
                            System.err.println("Malformed result message: " + result.body());
                            sqs.deleteMessage(DeleteMessageRequest.builder()
                                    .queueUrl(resultQueueUrl)
                                    .receiptHandle(result.receiptHandle())
                                    .build());
                            continue;
                        }

                        String doneQueueName = parts[0];
                        String resultLine = parts[1];

                        // Aggregate result line for this job
                        resultAggregators.computeIfAbsent(doneQueueName,
                                        k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(resultLine);

                        int remaining = totalPendingTasks.decrementAndGet();

                        // Check if job is complete
                        Integer expectedCount = submittedTaskCounts.get(doneQueueName);
                        List<String> currentResults = resultAggregators.get(doneQueueName);

                        if (expectedCount != null && currentResults != null &&
                                currentResults.size() == expectedCount) {

                            // Job is complete!
                            System.out.printf("\n*** Job %s complete! (%d/%d tasks) ***\n",
                                    doneQueueName, currentResults.size(), expectedCount);

                            List<String> finalResults = resultAggregators.remove(doneQueueName);
                            submittedTaskCounts.remove(doneQueueName);

                            // Generate and upload summary
                            String summaryS3Url = generateAndUploadSummary(s3, doneQueueName, finalResults);
                            sendDoneNotification(sqs, doneQueueName, summaryS3Url);
                        }

                        // Delete processed message
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(resultQueueUrl)
                                .receiptHandle(result.receiptHandle())
                                .build());
                    }

                    TimeUnit.MILLISECONDS.sleep(500);

                } catch (InterruptedException e) {
                    System.out.println("WorkerResultMonitor interrupted.");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error in WorkerResultMonitor: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println("WorkerResultMonitor stopped.");
        }

        private String generateAndUploadSummary(S3Client s3, String doneQueueName, List<String> results) {
            StringBuilder htmlContent = new StringBuilder();
            htmlContent.append("<!DOCTYPE html>\n");
            htmlContent.append("<html>\n<head>\n");
            htmlContent.append("<title>Text Analysis Summary</title>\n");
            htmlContent.append("<style>body{font-family:Arial,sans-serif;margin:20px;}");
            htmlContent.append("ul{list-style-type:none;}li{margin:10px 0;}</style>\n");
            htmlContent.append("</head>\n<body>\n");
            htmlContent.append("<h1>Text Analysis Summary</h1>\n");
            htmlContent.append("<ul>\n");

            for (String line : results) {
                htmlContent.append("<li>").append(line).append("</li>\n");
            }

            htmlContent.append("</ul>\n</body>\n</html>");

            String summaryS3Key = "output/" + doneQueueName + "/summary.html";
            String summaryS3Url = String.format("s3://%s/%s", S3_BUCKET_NAME, summaryS3Key);

            try {
                s3.putObject(
                        PutObjectRequest.builder()
                                .bucket(S3_BUCKET_NAME)
                                .key(summaryS3Key)
                                .contentType("text/html")
                                .build(),
                        RequestBody.fromString(htmlContent.toString()));

                System.out.println("Summary uploaded to: " + summaryS3Url);
            } catch (S3Exception e) {
                System.err.println("Error uploading summary: " + e.getMessage());
            }

            return summaryS3Url;
        }

        private void sendDoneNotification(SqsClient sqs, String doneQueueName, String summaryS3Url) {
            try {
                String queueUrl = getQueueUrl(sqs, doneQueueName);
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(summaryS3Url)
                        .build());
                System.out.println("Done notification sent to: " + doneQueueName);
            } catch (QueueDoesNotExistException e) {
                System.err.println("Done queue does not exist: " + doneQueueName);
            } catch (SqsException e) {
                System.err.println("Error sending done notification: " + e.getMessage());
            }
        }
    }

    // =================================================================================
    // TERMINATION LOGIC
    // =================================================================================

    private static String getManagerInstanceId() {
        try {
            URL url = new URL(METADATA_URL);
            URLConnection connection = url.openConnection();
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()))) {
                return in.readLine();
            }
        } catch (Exception e) {
            System.err.println("Error getting manager instance ID: " + e.getMessage());
            return null;
        }
    }

    private static void cleanupWorkers(Ec2Client ec2) {
        if (!activeWorkerInstanceIds.isEmpty()) {
            try {
                List<String> workerIds = new ArrayList<>(activeWorkerInstanceIds.keySet());
                ec2.terminateInstances(TerminateInstancesRequest.builder()
                        .instanceIds(workerIds)
                        .build());
                System.out.printf("Terminated %d active Worker instances.\n", workerIds.size());
            } catch (Ec2Exception e) {
                System.err.println("Error terminating Workers: " + e.getMessage());
            }
        } else {
            System.out.println("No active workers to terminate.");
        }
    }

    private static void cleanupAndTerminate(Ec2Client ec2) {
        System.out.println("\n=== Manager Shutting Down ===");

        // First terminate all workers
        cleanupWorkers(ec2);

        // Wait a bit to ensure workers get termination signal
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Then terminate self
        String managerId = getManagerInstanceId();
        if (managerId != null) {
            try {
                ec2.terminateInstances(TerminateInstancesRequest.builder()
                        .instanceIds(managerId)
                        .build());
                System.out.println("Manager instance self-terminated: " + managerId);
            } catch (Ec2Exception e) {
                System.err.println("Error terminating Manager: " + e.getMessage());
            }
        } else {
            System.out.println("Could not determine Manager instance ID. Manual cleanup may be needed.");
        }
    }
}