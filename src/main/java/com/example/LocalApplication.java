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

        // Unique job ID for THIS specific job
        String jobId = UUID.randomUUID().toString();
        String doneQueueName = "DoneQueue-" + jobId;
        String doneQueueUrl = null;

        AWS aws = AWS.getInstance();

        try {
            System.out.println("=== Starting Local Application ===");
            System.out.println("Job ID: " + jobId);
            System.out.println("Terminate mode: " + terminateMode);

            // 0. Ensure S3 bucket exists
            aws.createBucketIfNotExists(AWS.S3_BUCKET_NAME);
            System.out.println("✅ S3 Bucket ready: " + AWS.S3_BUCKET_NAME);

            // 1. Check if Manager is running, if not - launch it
            ensureManagerIsRunning(aws);

            // 2. Create unique done queue for THIS job BEFORE sending task
            doneQueueUrl = aws.createQueue(doneQueueName);
            System.out.println("✅ Created done queue for this job: " + doneQueueName);

            // 3. Upload input file to S3 (unique per job)
            String s3Key = "input/" + jobId + "/" + new File(inputFilePath).getName();
            String inputS3Url = uploadFileToS3(aws, inputFilePath, s3Key);

            // 4. Send task message to Manager
            sendTaskToManager(aws, inputS3Url, doneQueueName, n);

            // 5. Wait for THIS job completion
            String summaryS3Url = waitForCompletion(aws, doneQueueUrl, jobId);

            if (summaryS3Url != null) {
                // 6. Download summary file
                downloadSummaryFile(aws, summaryS3Url, outputFileName);
                System.out.println("✅ Job completed successfully!");
            } else {
                System.err.println("❌ Failed to receive completion message.");
            }

            // 7. If terminate mode, send termination AFTER this job completes
            if (terminateMode) {
                sendTerminationMessage(aws);
                System.out.println("✅ Termination message sent to Manager.");
            }

            System.out.println("=== Local Application finished ===");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // CRITICAL: Always clean up THIS job's done queue
            if (doneQueueUrl != null) {
                System.out.println("🧹 Cleaning up job resources...");
                try {
                    aws.deleteQueue(doneQueueUrl);
                    System.out.println("✅ Job queue deleted: " + doneQueueName);
                } catch (Exception e) {
                    System.err.println("Error deleting job queue: " + e.getMessage());
                }
            }
        }
    }

    private static void ensureManagerIsRunning(AWS aws) throws InterruptedException {
        List<Instance> managers = aws.findInstancesByTag(AWS.MANAGER_TAG_VALUE,
                InstanceStateName.RUNNING);

        if (!managers.isEmpty()) {
            System.out.println("✅ Manager instance found.");
            if (waitForManagerQueue(aws)) {
                System.out.println("✅ Manager is ready.");
                return;
            }
        }

        System.out.println("❌ Manager not found. Launching...");
        launchManager(aws);

        System.out.println("⏳ Waiting for Manager to initialize...");
        if (!waitForManagerQueue(aws)) {
            System.err.println("⚠️  Manager queue not detected. Proceeding anyway...");
        }
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
                "java -cp Manager.jar:lib/* com.example.Manager > manager.log 2>&1 &",                "sleep 5",
                "ps aux | grep java",
                "echo '=== Manager log ==='",
                "cat manager.log",
                "echo 'Manager startup complete'"
        );

        String userData = Base64.getEncoder().encodeToString(startupScript.getBytes());
        List<String> instanceIds = aws.launchInstances(1, userData, AWS.MANAGER_TAG_VALUE);
        System.out.println("✅ Manager launched: " + instanceIds.get(0));
    }

    private static boolean waitForManagerQueue(AWS aws) throws InterruptedException {
        int totalWait = 0;
        while (totalWait < MAX_MANAGER_WAIT_SECONDS) {
            try {
                aws.getQueueUrl(AWS.INPUT_QUEUE_NAME);
                System.out.println("✅ Manager queue detected!");
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
        System.out.println("✅ Input uploaded: " + s3Url);
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

        System.out.println("✅ Task sent to Manager.");
    }

    private static String waitForCompletion(AWS aws, String doneQueueUrl, String jobId)
            throws InterruptedException {
        System.out.println("⏳ Waiting for job " + jobId + " completion...");

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

                System.out.println("✅ Job " + jobId + " completion message received!");
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

        // Delete existing file if it exists (so we can overwrite)
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

        System.out.println("✅ Summary downloaded: " + outputFileName);
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