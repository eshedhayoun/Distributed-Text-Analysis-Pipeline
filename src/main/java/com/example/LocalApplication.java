package com.example;

import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;

import java.io.File;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalApplication {

    private static final int MAX_MANAGER_WAIT_SECONDS = 120;
    private static final int QUEUE_CHECK_INTERVAL_SECONDS = 5;

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: java -jar yourjar.jar <inputFileName> <outputFileName> <n> [terminate]");
            System.err.println("  n: workers' files ratio (max files per worker)");
            System.err.println("  terminate: indicates termination request");
            return;
        }

        String inputFilePath = args[0];
        String outputFileName = args[1];
        int n;
        try {
            n = Integer.parseInt(args[2]);
            if (n <= 0) {
                System.err.println("Argument 'n' must be a positive integer.");
                return;
            }
        } catch (NumberFormatException e) {
            System.err.println("Argument 'n' must be an integer.");
            return;
        }

        boolean terminateMode = args.length == 4 && args[3].equalsIgnoreCase("terminate");

        String jobId = UUID.randomUUID().toString();
        String doneQueueName = "DoneQueue-" + jobId;
        String doneQueueUrl = null;
        AtomicBoolean jobComplete = new AtomicBoolean(false);

        AWS aws = AWS.getInstance();

        try {
            System.out.println("=== Starting Local Application ===");
            System.out.println("Job ID: " + jobId);
            System.out.println("Terminate mode: " + terminateMode);

            aws.createBucketIfNotExists(AWS.S3_BUCKET_NAME);
            System.out.println("S3 Bucket ready: " + AWS.S3_BUCKET_NAME);

            ensureManagerIsRunning(aws);

            startManagerWatchdog(aws, jobComplete, terminateMode);

            doneQueueUrl = aws.createQueue(doneQueueName);
            System.out.println("Created done queue for this job: " + doneQueueName);

            String s3Key = "input/" + jobId + "/" + new File(inputFilePath).getName();
            String inputS3Url = uploadFileToS3(aws, inputFilePath, s3Key);

            sendTaskToManager(aws, inputS3Url, doneQueueName, n);

            String summaryS3Url = waitForCompletion(aws, doneQueueUrl, jobId);

            if (summaryS3Url != null) {
                downloadSummaryFile(aws, summaryS3Url, outputFileName);
                System.out.println("✅ Job completed successfully!");
            } else {
                System.err.println("Failed to receive completion message.");
            }

            jobComplete.set(true);
            System.out.println("Job marked as complete.");

            TimeUnit.SECONDS.sleep(2);

            if (terminateMode) {
                sendTerminationMessage(aws);
                System.out.println("Termination message sent to Manager.");

                System.out.println("Waiting for manager to terminate...");
                waitForManagerFullTermination(aws);
            } else {
                System.out.println("No termination requested - Manager will continue running for other jobs.");
            }

            System.out.println("=== Local Application finished ===");

        } catch (Exception e) {
            jobComplete.set(true);
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (doneQueueUrl != null) {
                System.out.println("Cleaning up job resources...");
                try {
                    aws.deleteQueue(doneQueueUrl);
                    System.out.println("Job queue deleted: " + doneQueueName);
                } catch (Exception e) {
                    System.err.println("Error deleting job queue: " + e.getMessage());
                }
            }
        }
    }

    private static void startManagerWatchdog(AWS aws, AtomicBoolean jobComplete, boolean terminateMode) {
        Thread watchdog = new Thread(() -> {
            System.out.println("Manager watchdog started - monitoring initialization...");

            int consecutiveFailures = 0;
            boolean managerFullyReady = false;

            while (!jobComplete.get()) {
                try {
                    TimeUnit.SECONDS.sleep(60);

                    if (jobComplete.get()) {
                        System.out.println("Job is complete - watchdog will stop checking");
                        break;
                    }

                    List<Instance> managers = aws.findInstancesByTag(
                            AWS.MANAGER_TAG_VALUE,
                            InstanceStateName.PENDING,
                            InstanceStateName.RUNNING
                    );

                    boolean managerHealthy = isManagerHealthy(managers);

                    if (managerHealthy) {
                        consecutiveFailures = 0;

                        if (!managerFullyReady) {
                            boolean hasReadySignal = checkForReadySignal(aws);
                            if (hasReadySignal) {
                                managerFullyReady = true;
                                System.out.println("✅ Manager fully initialized and ready");
                            } else {
                                System.out.println("Manager is initializing...");
                            }
                        }
                    } else {
                        consecutiveFailures++;

                        List<Instance> allManagers = aws.findInstancesByTag(
                                AWS.MANAGER_TAG_VALUE,
                                InstanceStateName.RUNNING,
                                InstanceStateName.PENDING,
                                InstanceStateName.STOPPING,
                                InstanceStateName.SHUTTING_DOWN,
                                InstanceStateName.STOPPED,
                                InstanceStateName.TERMINATED
                        );

                        String state = "not found";
                        if (!allManagers.isEmpty()) {
                            state = allManagers.get(0).state().nameAsString();
                        }

                        System.out.println("⚠️ Manager not healthy - state: " + state + " (" + consecutiveFailures + "/3)");

                        if (consecutiveFailures >= 3) {
                            System.out.println("⚠️ Manager failed 3 consecutive checks!");

                            if (jobComplete.get()) {
                                System.out.println("Job is complete - not restarting manager");
                                break;
                            }

                            boolean hasPendingWork = checkForPendingJobs(aws);

                            if (terminateMode && !hasPendingWork) {
                                System.out.println("✅ Terminate mode and no pending work. Manager terminated gracefully.");
                                break;
                            }

                            if (hasPendingWork || !managerFullyReady) {
                                if (hasPendingWork) {
                                    System.out.println("⚠️ Pending work detected! Restarting manager...");
                                } else {
                                    System.out.println("⚠️ Manager died during initialization! Restarting...");
                                }

                                if (!allManagers.isEmpty()) {
                                    List<String> managerIds = allManagers.stream()
                                            .map(Instance::instanceId)
                                            .collect(java.util.stream.Collectors.toList());
                                    aws.terminateInstances(managerIds);
                                    System.out.println("Terminated old manager instances");
                                    TimeUnit.SECONDS.sleep(30);
                                }

                                launchManager(aws);
                                System.out.println("New manager launched");

                                consecutiveFailures = 0;
                                managerFullyReady = false;

                                System.out.println("Waiting for new manager to initialize...");
                                if (waitForManagerReadySignal(aws)) {
                                    System.out.println("✅ New manager operational");
                                    managerFullyReady = true;
                                } else {
                                    System.err.println("⚠️ New manager failed to initialize");
                                }
                            } else {
                                System.out.println("✅ No pending work. Manager terminated gracefully.");
                                break;
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Watchdog error: " + e.getMessage());
                }
            }

            System.out.println("Manager watchdog stopped.");
        });

        watchdog.setDaemon(true);
        watchdog.start();
    }

    private static boolean isManagerHealthy(List<Instance> managers) {
        if (managers.isEmpty()) {
            return false;
        }

        Instance manager = managers.get(0);
        String state = manager.state().nameAsString();

        return state.equals("pending") || state.equals("running");
    }

    private static boolean checkForReadySignal(AWS aws) {
        try {
            String watchdogQueueUrl = aws.getQueueUrl(AWS.WATCHDOG_QUEUE_NAME);

            ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                    ReceiveMessageRequest.builder()
                            .queueUrl(watchdogQueueUrl)
                            .maxNumberOfMessages(1)
                            .waitTimeSeconds(0)
                            .build());

            if (!response.messages().isEmpty()) {
                Message message = response.messages().get(0);

                if (message.body().equals("MANAGER_READY")) {
                    aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(watchdogQueueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build());

                    return true;
                }
            }

            return false;

        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean waitForManagerReadySignal(AWS aws) throws InterruptedException {
        System.out.println("Waiting for manager ready signal...");

        String watchdogQueueUrl;
        try {
            watchdogQueueUrl = aws.createQueue(AWS.WATCHDOG_QUEUE_NAME);
        } catch (Exception e) {
            System.err.println("Failed to create watchdog queue: " + e.getMessage());
            return false;
        }

        int maxWaitSeconds = 300;
        int waited = 0;

        while (waited < maxWaitSeconds) {
            try {
                ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(watchdogQueueUrl)
                                .maxNumberOfMessages(1)
                                .waitTimeSeconds(20)
                                .build());

                if (!response.messages().isEmpty()) {
                    Message message = response.messages().get(0);

                    if (message.body().equals("MANAGER_READY")) {
                        aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(watchdogQueueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build());

                        System.out.println("✅ Received manager ready signal!");
                        return true;
                    }
                }

                waited += 20;

            } catch (Exception e) {
                System.err.println("Error waiting for ready signal: " + e.getMessage());
                TimeUnit.SECONDS.sleep(5);
                waited += 5;
            }
        }

        System.err.println("⚠️ Manager did not send ready signal within 5 minutes");
        return false;
    }

    private static boolean checkForPendingJobs(AWS aws) {
        try {
            String inputQueueUrl = aws.getQueueUrl(AWS.INPUT_QUEUE_NAME);
            GetQueueAttributesResponse inputAttrs = aws.getSqsClient().getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                            .queueUrl(inputQueueUrl)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                            .build()
            );

            int inputMessages = Integer.parseInt(
                    inputAttrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0")
            );

            String taskQueueUrl = aws.getQueueUrl(AWS.TASK_QUEUE_NAME);
            GetQueueAttributesResponse taskAttrs = aws.getSqsClient().getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                            .queueUrl(taskQueueUrl)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                            .build()
            );

            int taskMessages = Integer.parseInt(
                    taskAttrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0")
            );
            int taskMessagesInFlight = Integer.parseInt(
                    taskAttrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0")
            );

            String resultQueueUrl = aws.getQueueUrl(AWS.RESULT_QUEUE_NAME);
            GetQueueAttributesResponse resultAttrs = aws.getSqsClient().getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                            .queueUrl(resultQueueUrl)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                            .build()
            );

            int resultMessages = Integer.parseInt(
                    resultAttrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0")
            );

            boolean hasPending = (inputMessages > 0) || (taskMessages > 0) ||
                    (taskMessagesInFlight > 0) || (resultMessages > 0);

            if (hasPending) {
                System.out.println("Pending work detected: " +
                        "input=" + inputMessages + ", " +
                        "tasks=" + taskMessages + ", " +
                        "tasksInFlight=" + taskMessagesInFlight + ", " +
                        "results=" + resultMessages);
            }

            return hasPending;

        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (Exception e) {
            System.err.println("Error checking pending jobs: " + e.getMessage());
            return false;
        }
    }

    private static void waitForManagerFullTermination(AWS aws) {
        int maxWait = 180;
        int waited = 0;

        try {
            while (waited < maxWait) {
                List<Instance> managers = aws.findInstancesByTag(
                        AWS.MANAGER_TAG_VALUE,
                        InstanceStateName.RUNNING,
                        InstanceStateName.SHUTTING_DOWN,
                        InstanceStateName.STOPPING
                );

                if (managers.isEmpty()) {
                    System.out.println("✅ Manager and workers terminated.");
                    return;
                }

                System.out.print(".");
                TimeUnit.SECONDS.sleep(5);
                waited += 5;
            }

            System.out.println("\n⚠️ Manager still running after " + maxWait + " seconds.");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void ensureManagerIsRunning(AWS aws) throws InterruptedException {
        List<Instance> managers = aws.findInstancesByTag(AWS.MANAGER_TAG_VALUE,
                InstanceStateName.RUNNING);

        if (!managers.isEmpty()) {
            System.out.println("Manager instance found.");
            if (waitForManagerQueue(aws)) {
                System.out.println("Manager is ready.");
                return;
            }
        }

        List<Instance> terminatingManagers = aws.findInstancesByTag(AWS.MANAGER_TAG_VALUE,
                InstanceStateName.SHUTTING_DOWN, InstanceStateName.STOPPING);

        if (!terminatingManagers.isEmpty()) {
            System.out.println("Manager is terminating. Waiting for it to shut down...");
            waitForManagerToTerminate(aws);
        }

        System.out.println("Manager not found. Launching...");
        launchManager(aws);

        System.out.println("Waiting for Manager to initialize...");
        if (!waitForManagerQueue(aws)) {
            System.err.println("Manager queue not detected. Proceeding anyway...");
        }
    }

    private static void waitForManagerToTerminate(AWS aws) throws InterruptedException {
        int maxWait = 120;
        int waited = 0;

        while (waited < maxWait) {
            List<Instance> terminatingManagers = aws.findInstancesByTag(AWS.MANAGER_TAG_VALUE,
                    InstanceStateName.SHUTTING_DOWN, InstanceStateName.STOPPING,
                    InstanceStateName.RUNNING);

            if (terminatingManagers.isEmpty()) {
                System.out.println("Old manager terminated.");
                TimeUnit.SECONDS.sleep(10);
                return;
            }

            System.out.print(".");
            TimeUnit.SECONDS.sleep(5);
            waited += 5;
        }

        System.out.println("\nManager still terminating after " + maxWait + " seconds. Proceeding...");
    }

    private static void launchManager(AWS aws) {
        String startupScript = String.join("\n",
                "#!/bin/bash",
                "exec > >(tee /var/log/user-data.log) 2>&1",
                "echo 'Starting Manager setup...'",
                "apt-get update -y",
                "apt-get install -y default-jdk unzip curl",
                "echo 'Installing AWS CLI...'",
                "cd /tmp",
                "curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'",
                "unzip -q awscliv2.zip",
                "./aws/install",
                "echo 'AWS CLI installed'",
                "cd /home/ubuntu",
                "echo 'Downloading Manager JAR...'",
                "/usr/local/bin/aws s3 cp s3://" + AWS.S3_BUCKET_NAME + "/" + AWS.MANAGER_JAR_KEY + " Manager.jar",
                "echo 'Downloading lib folder...'",
                "/usr/local/bin/aws s3 cp s3://" + AWS.S3_BUCKET_NAME + "/jars/lib/ lib/ --recursive",
                "ls -lh Manager.jar",
                "ls -lh lib/ | head -5",
                "echo 'Starting Manager Java process...'",
                "java -version",
                "java -cp Manager.jar:lib/* com.example.Manager > manager.log 2>&1 &",
                "sleep 5",
                "ps aux | grep java",
                "echo '=== Manager log ==='",
                "cat manager.log",
                "echo 'Manager startup complete'"
        );

        String userData = Base64.getEncoder().encodeToString(startupScript.getBytes());
        List<String> instanceIds = aws.launchInstances(1, userData, AWS.MANAGER_TAG_VALUE);
        System.out.println("Manager launched: " + instanceIds.get(0));
    }

    private static boolean waitForManagerQueue(AWS aws) throws InterruptedException {
        int totalWait = 0;
        while (totalWait < MAX_MANAGER_WAIT_SECONDS) {
            try {
                aws.getQueueUrl(AWS.INPUT_QUEUE_NAME);
                System.out.println("Manager queue detected!");
                return true;
            } catch (QueueDoesNotExistException e) {
                System.out.print(".");
                TimeUnit.SECONDS.sleep(QUEUE_CHECK_INTERVAL_SECONDS);
                totalWait += QUEUE_CHECK_INTERVAL_SECONDS;
            }
        }
        return false;
    }

    private static String uploadFileToS3(AWS aws, String localFilePath, String s3Key) {
        File file = new File(localFilePath);
        if (!file.exists()) {
            throw new IllegalArgumentException("Input file not found: " + localFilePath);
        }

        aws.getS3Client().putObject(
                PutObjectRequest.builder()
                        .bucket(AWS.S3_BUCKET_NAME)
                        .key(s3Key)
                        .build(),
                RequestBody.fromFile(file));

        String s3Url = "s3://" + AWS.S3_BUCKET_NAME + "/" + s3Key;
        System.out.println("Input uploaded: " + s3Url);
        return s3Url;
    }

    private static void sendTaskToManager(AWS aws, String inputS3Url, String doneQueueName, int n) {
        String queueUrl;
        try {
            queueUrl = aws.getQueueUrl(AWS.INPUT_QUEUE_NAME);
        } catch (QueueDoesNotExistException e) {
            queueUrl = aws.createQueue(AWS.INPUT_QUEUE_NAME);
        }
        String messageBody = String.format("%s|%s|%d", inputS3Url, doneQueueName, n);

        aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build());

        System.out.println("Task sent to Manager.");
    }

    private static String waitForCompletion(AWS aws, String doneQueueUrl, String jobId)
            throws InterruptedException {
        System.out.println("Waiting for job " + jobId + " completion...");

        while (true) {
            ReceiveMessageResponse response = aws.getSqsClient().receiveMessage(
                    ReceiveMessageRequest.builder()
                            .queueUrl(doneQueueUrl)
                            .maxNumberOfMessages(1)
                            .waitTimeSeconds(20)
                            .build());

            List<Message> messages = response.messages();
            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                String summaryS3Url = message.body();

                aws.getSqsClient().deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(doneQueueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());

                System.out.println("Job " + jobId + " completion message received!");
                return summaryS3Url;
            } else {
                System.out.print(".");
            }
        }
    }

    private static void downloadSummaryFile(AWS aws, String s3Url, String outputFileName) {
        String s3UrlStripped = s3Url.substring(5);
        int firstSlash = s3UrlStripped.indexOf('/');
        String bucket = s3UrlStripped.substring(0, firstSlash);
        String key = s3UrlStripped.substring(firstSlash + 1);

        File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            outputFile.delete();
        }

        aws.getS3Client().getObject(
                GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build(),
                ResponseTransformer.toFile(Paths.get(outputFileName)));

        System.out.println("Summary downloaded: " + outputFileName);
    }

    private static void sendTerminationMessage(AWS aws) {
        try {
            String queueUrl = aws.getQueueUrl(AWS.INPUT_QUEUE_NAME);
            aws.getSqsClient().sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("TERMINATE")
                    .build());
        } catch (Exception e) {
            System.err.println("Error sending termination: " + e.getMessage());
        }
    }
}