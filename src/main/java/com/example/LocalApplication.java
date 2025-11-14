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
    private static final int MANAGER_STARTUP_WAIT_SECONDS = 90;

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
        } catch (NumberFormatException e) {
            System.err.println("Argument 'n' must be an integer.");
            return;
        }

        boolean terminateMode = args.length == 4 && args[3].equalsIgnoreCase("terminate");

        String clientId = UUID.randomUUID().toString();
        String doneQueueName = "DoneQueue-" + clientId;

        try (Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
             SqsClient sqs = SqsClient.builder().region(REGION).build();
             S3Client s3 = S3Client.builder().region(REGION).build()) {

            // 1. Checks if a Manager node is active on the EC2 cloud.
            Optional<Instance> managerInstance = findManagerInstance(ec2);
            if (!managerInstance.isPresent()) {
                // If it is not, the application will start the manager node.
                System.out.println("❌ Manager not found. Launching new instance...");
                runManagerInstance(ec2, MANAGER_JAR_KEY);
                System.out.printf("Waiting %d seconds for Manager to launch and initialize SQS...\n", MANAGER_STARTUP_WAIT_SECONDS);
                TimeUnit.SECONDS.sleep(MANAGER_STARTUP_WAIT_SECONDS);
            } else {
                System.out.println("✅ Manager is already running.");
            }

            // CRITICAL STEP: Ensure the unique done queue is created BEFORE the task is sent.
            createDoneQueue(sqs, doneQueueName);

            // 2. Uploads the input file to S3.
            String s3Key = "input/" + clientId + "/" + new File(inputFilePath).getName();
            String inputS3Url = String.format("s3://%s/%s", S3_BUCKET_NAME, s3Key);
            uploadFileToS3(s3, inputFilePath, S3_BUCKET_NAME, s3Key);

            // 3. Sends a message to an SQS queue, stating the location of the file on S3.
            sendInitialTaskMessage(sqs, inputS3Url, doneQueueName, n);

            // 4. Checks an SQS queue for a message indicating the process is done.
            if (!terminateMode) {
                String summaryS3Url = waitForDoneMessageAndGetSummaryUrl(sqs, doneQueueName);
                if (summaryS3Url != null) {
                    // 5. Gets the summary output file from S3.
                    downloadSummaryFile(s3, summaryS3Url, outputFileName);
                }
            }

            // 6. In case of terminate mode, sends a termination message to the Manager.
            else {
                sendTerminationMessage(sqs);
                System.out.println("✅ Termination message sent to Manager queue. Exiting.");
            }
            deleteQueue(sqs, doneQueueName);

        } catch (SqsException | S3Exception e) {
            System.err.println("AWS Service Error: " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println("Process interrupted during sleep/polling.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // --- NEW CRITICAL METHOD ---
    private static void createDoneQueue(SqsClient sqs, String doneQueueName) throws SqsException {
        try {
            sqs.createQueue(CreateQueueRequest.builder().queueName(doneQueueName).build());
            System.out.println("✅ Created unique done queue: " + doneQueueName);
        } catch (QueueNameExistsException e) {
            // This shouldn't happen with UUID, but ignore if it does.
        }
    }

    // --- Core Methods (Syntax corrected for SDK v2, using .receiptHandle()) ---

    private static Optional<Instance> findManagerInstance(Ec2Client ec2) {
        Filter runningFilter = Filter.builder().name("instance-state-name").values(InstanceStateName.RUNNING.toString()).build();
        Filter managerTagFilter = Filter.builder().name("tag:" + INSTANCE_TAG_KEY).values(MANAGER_TAG_VALUE).build();

        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(runningFilter, managerTagFilter).build();
        DescribeInstancesResponse response = ec2.describeInstances(request);

        return response.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .findFirst();
    }

    private static void runManagerInstance(Ec2Client ec2, String jarKey) {
        String startupScript = String.join("\n",
                "#!/bin/bash",
                "apt update",
                "apt install -y default-jdk awscli",
                "aws s3 cp s3://" + S3_BUCKET_NAME + "/" + jarKey + " /home/ec2-user/Manager.jar",
                "echo 'Starting Manager.jar...' >> /home/ec2-user/manager_log.txt",
                "java -jar /home/ec2-user/Manager.jar &"
        );

        String userData = Base64.getEncoder().encodeToString(startupScript.getBytes());

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(AMI_ID)
                .instanceType(InstanceType.fromValue(INSTANCE_TYPE))
                .minCount(1).maxCount(1).userData(userData)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key(INSTANCE_TAG_KEY).value(MANAGER_TAG_VALUE).build())
                        .build())
                .build();

        ec2.runInstances(runRequest);
        System.out.println("✅ Manager instance launch requested.");
    }

    private static void uploadFileToS3(S3Client s3, String localFilePath, String bucketName, String s3Key) {
        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3.putObject(putReq, RequestBody.fromFile(new File(localFilePath)));
        System.out.println("✅ Input file uploaded to S3: s3://" + bucketName + "/" + s3Key);
    }

    private static void sendInitialTaskMessage(SqsClient sqs, String inputS3Url, String doneQueueName, int n) {
        String messageBody = inputS3Url + "|" + doneQueueName + "|" + n;

        try {
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(INPUT_QUEUE_NAME).build());
            String queueUrl = urlResponse.queueUrl();

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build());
        } catch (SqsException e) {
            System.err.println("Error sending initial task message: " + e.getMessage());
        }
    }

    private static String waitForDoneMessageAndGetSummaryUrl(SqsClient sqs, String doneQueueName) throws InterruptedException {
        String summaryS3Url = null;
        String doneQueueUrl = null;

        try {
            // Now this is guaranteed to succeed because the queue was created earlier in main()
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(doneQueueName).build());
            doneQueueUrl = urlResponse.queueUrl();
            System.out.println("Polling on done queue: " + doneQueueUrl);
        } catch (SqsException e) {
            System.err.println("Could not get done queue URL: " + e.getMessage());
            return null;
        }

        while (summaryS3Url == null) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(doneQueueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .build();

            ReceiveMessageResponse receiveResponse = sqs.receiveMessage(receiveRequest);
            List<Message> messages = receiveResponse.messages();

            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                summaryS3Url = message.body();

                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(doneQueueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());

                System.out.println("✅ Process completed. Final message received.");
            } else {
                System.out.print(".");
            }
        }
        return summaryS3Url;
    }

    private static void downloadSummaryFile(S3Client s3, String s3Url, String outputFileName) {
        try {
            String s3UrlStripped = s3Url.substring(5);
            String bucket = s3UrlStripped.substring(0, s3UrlStripped.indexOf('/'));
            String key = s3UrlStripped.substring(s3UrlStripped.indexOf('/') + 1);

            GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(key).build();

            s3.getObject(getReq, ResponseTransformer.toFile(Paths.get(outputFileName)));
            System.out.println("✅ Summary file downloaded to: " + outputFileName);
        } catch (Exception e) {
            System.err.println("Error downloading summary file: " + e.getMessage());
        }
    }

    private static void sendTerminationMessage(SqsClient sqs) {
        try {
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(INPUT_QUEUE_NAME).build());
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
            GetQueueUrlResponse urlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(doneQueueName).build());
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(urlResponse.queueUrl()).build());
            System.out.println("✅ Cleaned up done queue: " + doneQueueName);
        } catch (SqsException ignored) {
            // Ignore if queue doesn't exist
        }
    }
} 
