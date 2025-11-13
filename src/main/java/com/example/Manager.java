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
    private static final Region REGION = Region.EU_WEST_1;
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
    private static final String METADATA_URL = "http://169.254.169.254/latest/meta-data/instance-id";

    // --- State Tracking (Used for Synchronization, NOT Aggregation) ---
    private static volatile boolean terminationReceived = false;
    // Map tracks active jobs by their DoneQueueName and the TOTAL tasks submitted.
    private static final ConcurrentHashMap<String, Integer> submittedTaskCounts = new ConcurrentHashMap<>();
    // Counts how many tasks across ALL jobs are currently processing/pending (used for wait logic)
    private static final AtomicInteger totalPendingTasks = new AtomicInteger(0);
    private static final List<String> activeWorkerInstanceIds = Collections.synchronizedList(new ArrayList<>());

    // ExecutorService for concurrent handling of client requests and result processing
    private static final ExecutorService concurrentExecutor = Executors.newFixedThreadPool(20);

    public static void main(String[] args) {
        System.out.println("Manager node started. Initializing...");

        try (SqsClient sqs = SqsClient.builder().region(REGION).build();
             S3Client s3 = S3Client.builder().region(REGION).build();
             Ec2Client ec2 = Ec2Client.builder().region(REGION).build()) {

            // 0. Ensure all necessary shared SQS Queues exist
            getOrCreateQueueUrl(sqs, INPUT_QUEUE_NAME);
            String resultQueueUrl = getOrCreateQueueUrl(sqs, RESULT_QUEUE_NAME);

            // Start a thread to continuously monitor the worker result queue and check for job completion
            concurrentExecutor.submit(new WorkerResultMonitor(sqs, s3, resultQueueUrl));

            System.out.println("Manager ready. Polling loop started.");

            // Main polling loop (manages all incoming requests concurrently)
            while (!terminationReceived || totalPendingTasks.get() > 0) {

                if (terminationReceived && totalPendingTasks.get() == 0) break;

                // 1. Poll input queue if not terminating
                if (!terminationReceived) {
                    List<Message> messages = receiveMessages(sqs, INPUT_QUEUE_NAME);

                    for (Message message : messages) {
                        if (processIncomingMessage(sqs, s3, ec2, message)) {
                            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(getQueueUrl(sqs, INPUT_QUEUE_NAME)).receiptHandle(message.receiptHandle()).build());
                            break;
                        }
                        // Delete the message after handing it off to the ExecutorService
                        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(getQueueUrl(sqs, INPUT_QUEUE_NAME)).receiptHandle(message.receiptHandle()).build());
                    }
                }

                TimeUnit.SECONDS.sleep(5);
            }

            // Cleanup and Self-Terminate
            cleanupAndTerminate(ec2);

        } catch (Exception e) {
            System.err.println("Manager encountered a fatal error: " + e.getMessage());
            // Attempt to terminate workers even on crash
            try (Ec2Client ec2 = Ec2Client.builder().region(REGION).build()) { cleanupWorkers(ec2); } catch (Exception ignored) {}
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
        try { return getQueueUrl(sqs, queueName); } catch (QueueDoesNotExistException e) {
            System.out.printf("Queue %s not found. Creating it.\n", queueName);
            return sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl();
        }
    }

    private static List<Message> receiveMessages(SqsClient sqs, String queueName) {
        String queueUrl;
        try { queueUrl = getQueueUrl(sqs, queueName); } catch (SqsException e) { return Collections.emptyList(); }

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(20).build();
        return sqs.receiveMessage(receiveRequest).messages();
    }

    /**
     * Processes the incoming message from the Local Application.
     * @return true if the message is a TERMINATE command, false otherwise.
     */
    private static boolean processIncomingMessage(SqsClient sqs, S3Client s3, Ec2Client ec2, Message message) {
        String body = message.body();

        if (body.equals("TERMINATE")) {
            terminationReceived = true;
            System.out.println("\n*** Received TERMINATE command. Stopping new task acceptance. ***");
            return true;
        }

        // IMPORTANT: Must not handle each request at a time, but work on all requests in parallel.
        String[] parts = body.split("\\|", 3);
        if (parts.length == 3) {
            String doneQueueName = parts[1];
            int n = Integer.parseInt(parts[2]);

            // Delegate the orchestration to a separate thread for concurrency
            concurrentExecutor.submit(() -> orchestrateWorkload(sqs, s3, ec2, parts[0], doneQueueName, n));

        } else {
            System.err.println("Unknown/Malformed message received: " + body);
        }
        return false;
    }

    // =================================================================================
    // ORCHESTRATION & SCALING
    // =================================================================================

    private static void orchestrateWorkload(SqsClient sqs, S3Client s3, Ec2Client ec2, String inputS3Url, String doneQueueName, int n) {
        try {
            // 1. Downloads the input file from S3.
            List<String> rawTasks = parseInputFile(s3, inputS3Url);
            int totalTasks = rawTasks.size();
            System.out.printf("[Job %s] Parsed %d total tasks.\n", doneQueueName, totalTasks);

            if (totalTasks == 0) return;

            // Update global task count and track submitted tasks for this job
            submittedTaskCounts.put(doneQueueName, totalTasks);
            totalPendingTasks.addAndGet(totalTasks);

            // 2. Checks the SQS message count and starts Worker processes (nodes) accordingly.
            int requiredWorkers = (int) Math.ceil((double) totalTasks / n); // m (workers needed)
            int currentActiveWorkers = activeWorkerInstanceIds.size(); // k (active workers)
            int workersToLaunch = requiredWorkers - currentActiveWorkers; // m - k

            System.out.printf("[Job %s] Required: %d. Active: %d. Launching: %d.\n",
                    doneQueueName, requiredWorkers, currentActiveWorkers, workersToLaunch);

            if (workersToLaunch > 0) {
                launchWorkers(ec2, workersToLaunch);
            }

            // 3. Creates an SQS message for each URL... All worker nodes take their messages from the same SQS queue.
            String taskQueueUrl = getOrCreateQueueUrl(sqs, TASK_QUEUE_NAME);
            delegateTasksToSharedQueue(sqs, taskQueueUrl, rawTasks, doneQueueName);

        } catch (Exception e) {
            System.err.println("Orchestration failed for job " + doneQueueName + ": " + e.getMessage());
        }
    }

    private static void launchWorkers(Ec2Client ec2, int workersToLaunch) {
        int activeManagersAndWorkers = activeWorkerInstanceIds.size() + 1;
        int availableCapacity = MAX_TOTAL_INSTANCES - activeManagersAndWorkers;
        int finalLaunchCount = Math.min(workersToLaunch, availableCapacity);

        if (finalLaunchCount <= 0) return;

        // User data script includes installing Java/AWS CLI and downloading the Worker JAR
        String startupScript = String.join("\n",
                "#!/bin/bash",
                "apt update",
                "apt install -y default-jdk awscli",
                "aws s3 cp s3://" + S3_BUCKET_NAME + "/jars/stanford-corenlp-4.5.1.jar /home/ec2-user/",
                "aws s3 cp s3://" + S3_BUCKET_NAME + "/" + WORKER_JAR_KEY + " /home/ec2-user/Worker.jar",
                "java -Xmx4g -jar /home/ec2-user/Worker.jar " + TASK_QUEUE_NAME + " " + RESULT_QUEUE_NAME + " &"
        );
        String userData = java.util.Base64.getEncoder().encodeToString(startupScript.getBytes());

        RunInstancesResponse response = ec2.runInstances(RunInstancesRequest.builder()
                .imageId(WORKER_AMI_ID).instanceType(InstanceType.fromValue(WORKER_INSTANCE_TYPE))
                .minCount(finalLaunchCount).maxCount(finalLaunchCount).userData(userData)
                .tagSpecifications(TagSpecification.builder().resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key(WORKER_TAG_KEY).value(WORKER_TAG_VALUE).build()).build())
                .build());

        for (Instance instance : response.instances()) { activeWorkerInstanceIds.add(instance.instanceId()); }
    }

    private static void delegateTasksToSharedQueue(SqsClient sqs, String taskQueueUrl, List<String> rawTasks, String doneQueueName) {
        // Task message includes the DONE_QUEUE_NAME so the worker can send the result to the correct place
        for (String taskLine : rawTasks) {
            String workerMessage = taskLine + "|" + doneQueueName;
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(taskQueueUrl).messageBody(workerMessage).build());
        }
    }

    private static List<String> parseInputFile(S3Client s3, String s3Url) throws Exception {
        String s3UrlStripped = s3Url.substring(5);
        String bucket = s3UrlStripped.substring(0, s3UrlStripped.indexOf('/'));
        String key = s3UrlStripped.substring(s3UrlStripped.indexOf('/') + 1);

        try (InputStreamReader isr = new InputStreamReader(s3.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toInputStream()));
             BufferedReader br = new BufferedReader(isr)) {
            return br.lines().collect(Collectors.toList());
        }
    }

    // =================================================================================
    // RESULT AGGREGATION & MONITORING (SQS-driven)
    // =================================================================================

    /**
     * Runnable dedicated to continuously polling the worker result queue and finalizing jobs.
     * State is tracked via S3 result files and the submittedTaskCounts map.
     */
    private static class WorkerResultMonitor implements Runnable {
        private final SqsClient sqs; private final S3Client s3; private final String resultQueueUrl;
        private final ConcurrentHashMap<String, List<String>> resultAggregators = new ConcurrentHashMap<>();

        public WorkerResultMonitor(SqsClient sqs, S3Client s3, String resultQueueUrl) {
            this.sqs = sqs; this.s3 = s3; this.resultQueueUrl = resultQueueUrl;
        }

        @Override public void run() {
            while (!terminationReceived || totalPendingTasks.get() > 0) {
                try {
                    List<Message> results = receiveMessages(sqs, RESULT_QUEUE_NAME);

                    for (Message result : results) {
                        // Worker result format: DONE_QUEUE_NAME|HTML_RESULT_LINE
                        String[] parts = result.body().split("\\|", 2);
                        if (parts.length != 2) {
                            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(resultQueueUrl).receiptHandle(result.receiptHandle()).build());
                            continue;
                        }

                        String doneQueueName = parts[0];
                        String resultLine = parts[1];

                        // Aggregate result line for this job
                        resultAggregators.computeIfAbsent(doneQueueName, k -> Collections.synchronizedList(new ArrayList<>())).add(resultLine);
                        totalPendingTasks.decrementAndGet();

                        // Check if job is complete
                        if (submittedTaskCounts.containsKey(doneQueueName) &&
                                resultAggregators.get(doneQueueName).size() == submittedTaskCounts.get(doneQueueName)) {

                            // Job is complete! Creates response messages for the jobs.
                            System.out.printf("\n*** Job for client %s finished! Generating summary. ***\n", doneQueueName);

                            List<String> finalResults = resultAggregators.remove(doneQueueName);
                            submittedTaskCounts.remove(doneQueueName);

                            String summaryS3Url = generateAndUploadSummary(s3, doneQueueName, finalResults);
                            sendDoneNotification(sqs, doneQueueName, summaryS3Url);
                        }
                        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(resultQueueUrl).receiptHandle(result.receiptHandle()).build());
                    }
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (Exception e) { System.err.println("Error in WorkerResultMonitor: " + e.getMessage()); }
            }
        }

        private String generateAndUploadSummary(S3Client s3, String doneQueueName, List<String> results) {
            StringBuilder htmlContent = new StringBuilder("<html><head><title>Analysis Summary</title></head><body><h1>Text Analysis Summary</h1><ul>");
            for (String line : results) { htmlContent.append("<li>").append(line).append("</li>"); }
            htmlContent.append("</ul></body></html>");

            String summaryS3Key = "output/" + doneQueueName + "/summary.html";
            String summaryS3Url = String.format("s3://%s/%s", S3_BUCKET_NAME, summaryS3Key);

            s3.putObject(PutObjectRequest.builder().bucket(S3_BUCKET_NAME).key(summaryS3Key).contentType("text/html").build(),
                    RequestBody.fromString(htmlContent.toString()));
            return summaryS3Url;
        }

        private void sendDoneNotification(SqsClient sqs, String doneQueueName, String summaryS3Url) {
            try {
                String queueUrl = getQueueUrl(sqs, doneQueueName);
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(summaryS3Url).build());
            } catch (SqsException e) { System.err.println("Error sending done notification to client: " + e.getMessage()); }
        }
    }

    // =================================================================================
    // TERMINATION LOGIC
    // =================================================================================

    private static String getManagerInstanceId() {
        try {
            URL url = new URL(METADATA_URL);
            URLConnection connection = url.openConnection();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                return in.readLine();
            }
        } catch (Exception e) { return null; }
    }

    private static void cleanupWorkers(Ec2Client ec2) {
        if (!activeWorkerInstanceIds.isEmpty()) {
            try {
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(activeWorkerInstanceIds).build());
                System.out.printf("Terminated %d active Worker instances.\n", activeWorkerInstanceIds.size());
            } catch (Ec2Exception e) { System.err.println("Error terminating Workers: " + e.getMessage()); }
        }
    }

    private static void cleanupAndTerminate(Ec2Client ec2) {
        System.out.println("\n--- Manager Shutting Down ---");
        cleanupWorkers(ec2);

        String managerId = getManagerInstanceId();
        if (managerId != null) {
            try {
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(managerId).build());
                System.out.println("Manager instance self-terminated.");
            } catch (Ec2Exception e) { System.err.println("Error terminating Manager: " + e.getMessage()); }
        }
    }
}