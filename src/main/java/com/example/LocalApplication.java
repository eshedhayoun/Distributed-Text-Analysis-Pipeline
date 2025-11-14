package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Base64;

public class LocalApplication {

    // --- Configuration Constants ---
    private static final Region REGION = Region.EU_WEST_1;
    private static final String S3_BUCKET_NAME = "your-unique-bucket-name-here";
    private static final String INPUT_QUEUE_NAME = "Client_Manager_Queue";
    private static final String INSTANCE_TAG_KEY = "Type";
    private static final String MANAGER_TAG_VALUE = "Manager";
    private static final String AMI_ID = "ami-xxxxxxxxxxxxxxxxx"; // Replace with your configured AMI ID
    private static final String INSTANCE_TYPE = InstanceType.T2_MICRO.toString();
    private static final int MAX_MANAGER_WAIT_SECONDS = 120;
    private static final int QUEUE_CHECK_INTERVAL_SECONDS = 5;

    private static final String MANAGER_JAR_KEY = "jars/Manager.jar";

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

        String clientId = UUID.randomUUID().toString();
        String doneQueueName = "DoneQueue-" + clientId;
        boolean queueCreated = false;

        try (Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
             SqsClient sqs = SqsClient.builder().region(REGION).build();
             S3Client s3 = S3Client.builder().region(REGION).build()) {

            System.out.println("=== Starting Local Application ===");
            System.out.println("Client ID: " + clientId);
            System.out.println("Terminate mode: " + terminateMode);

            try {
                // 1. Check if Manager is running, if not - launch it
                ensureManagerIsRunning(ec2, sqs);

                // 2. Create unique done queue BEFORE sending task (CRITICAL!)
                createDoneQueue(sqs, doneQueueName);
                queueCreated = true; // Mark that we successfully created the queue

                // 3. Upload input file to S3
                String s3Key = "input/" + clientId + "/" + new File(inputFilePath).getName();
                String inputS3Url = String.format("s3://%s/%s", S3_BUCKET_NAME, s3Key);
                uploadFileToS3(s3, inputFilePath, S3_BUCKET_NAME, s3Key);

                // 4. Send task message to Manager
                sendInitialTaskMessage(sqs, inputS3Url, doneQueueName, n);

                // 5. Wait for job completion and get results (ALWAYS, even in terminate mode!)
                String summaryS3Url = waitForDoneMessageAndGetSummaryUrl(sqs, doneQueueName);

                if (summaryS3Url != null) {
                    // 6. Download summary file
                    downloadSummaryFile(s3, summaryS3Url, outputFileName);
                    System.out.println("✅ Job completed successfully!");
                } else {
                    System.err.println("❌ Failed to receive completion message from Manager.");
                }

                // 7. If terminate mode, send termination message AFTER getting results
                if (terminateMode) {
                    sendTerminationMessage(sqs);
                    System.out.println("✅ Termination message sent to Manager.");
                }

                System.out.println("=== Local Application finished ===");

            } finally {
                // CRITICAL CLEANUP: Delete done queue if it was created
                // This MUST happen regardless of success, failure, or termination
                if (queueCreated) {
                    System.out.println("🧹 Cleaning up resources...");
                    deleteQueue(sqs, doneQueueName);
                }
            }

        } catch (SqsException | S3Exception e) {
            System.err.println("AWS Service Error: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.err.println("Process interrupted.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Ensures Manager is running. Returns true if Manager was launched by this call.
     * Optimized to check queue existence instead of blind waiting.
     */
    private static boolean ensureManagerIsRunning(Ec2Client ec2, SqsClient sqs) throws InterruptedException {
        Optional<Instance> managerInstance = findManagerInstance(ec2);

        if (managerInstance.isPresent()) {
            System.out.println("✅ Manager instance is already running.");
            // Manager exists, but is its queue ready?
            if (waitForManagerQueue(sqs)) {
                System.out.println("✅ Manager queue is ready.");
                return false;
            }
        }

        // Manager not found or queue not ready - launch new manager
        System.out.println("❌ Manager not found or not ready. Launching new instance...");
        runManagerInstance(ec2, MANAGER_JAR_KEY);

        // Wait for Manager to initialize and create its queue
        System.out.println("⏳ Waiting for Manager to initialize...");
        if (!waitForManagerQueue(sqs)) {
            System.err.println("⚠️  Manager queue not detected within timeout. Proceeding anyway...");
        }

        return true;
    }

    /**
     * Waits for Manager's input queue to exist (indicates Manager is ready).
     * Returns true if queue found, false if timeout.
     */
    private static boolean waitForManagerQueue(SqsClient sqs) throws InterruptedException {
        int totalWaitTime = 0;

        while (totalWaitTime < MAX_MANAGER_WAIT_SECONDS) {
            try {
                sqs.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(INPUT_QUEUE_NAME)
                        .build());
                System.out.println("✅ Manager input queue detected!");
                return true;
            } catch (QueueDoesNotExistException e) {
                System.out.print(".");
                TimeUnit.SECONDS.sleep(QUEUE_CHECK_INTERVAL_SECONDS);
                totalWaitTime += QUEUE_CHECK_INTERVAL_SECONDS;
            }
        }

        return false;
    }

    private static Optional<Instance> findManagerInstance(Ec2Client ec2) {
        try {
            Filter runningFilter = Filter.builder()
                    .name("instance-state-name")
                    .values(InstanceStateName.RUNNING.toString())
                    .build();

            Filter managerTagFilter = Filter.builder()
                    .name("tag:" + INSTANCE_TAG_KEY)
                    .values(MANAGER_TAG_VALUE)
                    .build();

            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(runningFilter, managerTagFilter)
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);

            return response.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .findFirst();
        } catch (Ec2Exception e) {
            System.err.println("Error checking for Manager instance: " + e.getMessage());
            return Optional.empty();
        }
    }

    private static void runManagerInstance(Ec2Client ec2, String jarKey) {
        String startupScript = String.join("\n",
                "#!/bin/bash",
                "exec > >(tee /var/log/user-data.log) 2>&1",
                "apt-get update -y",
                "apt-get install -y default-jdk awscli",
                "aws s3 cp s3://" + S3_BUCKET_NAME + "/" + jarKey + " /home/ubuntu/Manager.jar",
                "cd /home/ubuntu",
                "java -jar Manager.jar > manager.log 2>&1 &",
                "echo 'Manager started' >> /var/log/user-data.log"
        );

        String userData = Base64.getEncoder().encodeToString(startupScript.getBytes());

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(AMI_ID)
                .instanceType(InstanceType.fromValue(INSTANCE_TYPE))
                .minCount(1)
                .maxCount(1)
                .userData(userData)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder()
                                .key(INSTANCE_TAG_KEY)
                                .value(MANAGER_TAG_VALUE)
                                .build())
                        .build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        System.out.println("✅ Manager instance launch requested: " +
                response.instances().get(0).instanceId());
    }

    /**
     * Creates the unique done queue for this client.
     * MUST be called BEFORE sending task to Manager!
     */
    private static void createDoneQueue(SqsClient sqs, String doneQueueName) {
        try {
            CreateQueueResponse response = sqs.createQueue(
                    CreateQueueRequest.builder()
                            .queueName(doneQueueName)
                            .build()
            );
            System.out.println("✅ Created done queue: " + doneQueueName);
        } catch (QueueNameExistsException e) {
            System.out.println("ℹ️  Done queue already exists: " + doneQueueName);
        } catch (SqsException e) {
            System.err.println("Error creating done queue: " + e.getMessage());
            throw e;
        }
    }

    private static void uploadFileToS3(S3Client s3, String localFilePath, String bucketName, String s3Key) {
        try {
            File file = new File(localFilePath);
            if (!file.exists()) {
                throw new IllegalArgumentException("Input file not found: " + localFilePath);
            }

            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            s3.putObject(putReq, RequestBody.fromFile(file));
            System.out.println("✅ Input file uploaded to S3: s3://" + bucketName + "/" + s3Key);
        } catch (S3Exception e) {
            System.err.println("Error uploading to S3: " + e.getMessage());
            throw e;
        }
    }

    private static void sendInitialTaskMessage(SqsClient sqs, String inputS3Url, String doneQueueName, int n) {
        try {
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(
                    GetQueueUrlRequest.builder()
                            .queueName(INPUT_QUEUE_NAME)
                            .build()
            );
            String queueUrl = urlResponse.queueUrl();

            // Message format: inputS3Url|doneQueueName|n
            String messageBody = String.format("%s|%s|%d", inputS3Url, doneQueueName, n);

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build());

            System.out.println("✅ Task message sent to Manager queue.");
        } catch (SqsException e) {
            System.err.println("Error sending task message: " + e.getMessage());
            throw e;
        }
    }

    private static String waitForDoneMessageAndGetSummaryUrl(SqsClient sqs, String doneQueueName)
            throws InterruptedException {

        String doneQueueUrl;
        try {
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(
                    GetQueueUrlRequest.builder()
                            .queueName(doneQueueName)
                            .build()
            );
            doneQueueUrl = urlResponse.queueUrl();
        } catch (SqsException e) {
            System.err.println("Could not get done queue URL: " + e.getMessage());
            return null;
        }

        System.out.println("⏳ Waiting for job completion...");

        while (true) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(doneQueueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20) // Long polling - efficient!
                    .build();

            ReceiveMessageResponse receiveResponse = sqs.receiveMessage(receiveRequest);
            List<Message> messages = receiveResponse.messages();

            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                String summaryS3Url = message.body();

                // Delete message from queue
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(doneQueueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());

                System.out.println("✅ Completion message received!");
                return summaryS3Url;
            } else {
                System.out.print(".");
            }
        }
    }

    private static void downloadSummaryFile(S3Client s3, String s3Url, String outputFileName) {
        try {
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

            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            s3.getObject(getReq, ResponseTransformer.toFile(Paths.get(outputFileName)));
            System.out.println("✅ Summary file downloaded: " + outputFileName);
        } catch (S3Exception e) {
            System.err.println("Error downloading summary file: " + e.getMessage());
            throw e;
        } catch (Exception e) {
            System.err.println("Error processing S3 URL: " + e.getMessage());
            throw e;
        }
    }

    private static void sendTerminationMessage(SqsClient sqs) {
        try {
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(
                    GetQueueUrlRequest.builder()
                            .queueName(INPUT_QUEUE_NAME)
                            .build()
            );
            String queueUrl = urlResponse.queueUrl();

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("TERMINATE")
                    .build());
        } catch (SqsException e) {
            System.err.println("Error sending termination message: " + e.getMessage());
        }
    }

    private static void deleteQueue(SqsClient sqs, String doneQueueName) {
        try {
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(
                    GetQueueUrlRequest.builder()
                            .queueName(doneQueueName)
                            .build()
            );

            sqs.deleteQueue(DeleteQueueRequest.builder()
                    .queueUrl(urlResponse.queueUrl())
                    .build());

            System.out.println("✅ Done queue cleaned up: " + doneQueueName);
        } catch (QueueDoesNotExistException e) {
            // Queue doesn't exist, nothing to clean up
        } catch (SqsException e) {
            System.err.println("Error deleting queue: " + e.getMessage());
        }
    }
}