package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class AWS {


    private static final Region REGION = Region.US_EAST_1;
    public static final String S3_BUCKET_NAME = "distributed-text-analysis-pipeline-inputs-us-east-1";

    public static final String AMI_ID = "ami-0ecb62995f68bb549";

    public static final String INPUT_QUEUE_NAME = "Client_Manager_Queue";
    public static final String TASK_QUEUE_NAME = "Manager_Worker_TaskQueue";
    public static final String RESULT_QUEUE_NAME = "Manager_Results_Queue";
    public static final String WATCHDOG_QUEUE_NAME = "Manager_Watchdog_Queue";

    public static final String INSTANCE_TYPE = "t3.micro";
    public static final int MAX_INSTANCES = 19;

    public static final String INSTANCE_TAG_KEY = "Type";
    public static final String MANAGER_TAG_VALUE = "Manager";
    public static final String WORKER_TAG_VALUE = "Worker";

    public static final String MANAGER_JAR_KEY = "jars/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar";
    public static final String WORKER_JAR_KEY = "jars/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar";

    private final S3Client s3Client;
    private final SqsClient sqsClient;
    private final Ec2Client ec2Client;

    private static final AWS instance = new AWS();

    private AWS() {
        this.s3Client = S3Client.builder().region(REGION).build();
        this.sqsClient = SqsClient.builder().region(REGION).build();
        this.ec2Client = Ec2Client.builder().region(REGION).build();
    }

    public static AWS getInstance() {
        return instance;
    }
    public Region getRegion() { return REGION; }
    public S3Client getS3Client() {
        return s3Client;
    }
    public SqsClient getSqsClient() {
        return sqsClient;
    }

    public void createBucketIfNotExists(String bucketName) {
        try {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            if (!e.awsErrorDetails().errorCode().contains("BucketAlready")) {
                throw e;
            }
        }
    }

    public String createQueue(String queueName) {
        try {
            return sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build()).queueUrl();
        } catch (QueueNameExistsException e) {
            return getQueueUrl(queueName);
        }
    }

    public String getQueueUrl(String queueName) {
        return sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build()).queueUrl();
    }

    public void deleteQueue(String queueUrl) {
        sqsClient.deleteQueue(DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build());
    }

    public List<String> launchInstances(int count, String userData, String tagValue) {
        RunInstancesResponse response = ec2Client.runInstances(
                RunInstancesRequest.builder()
                        .imageId(AMI_ID)
                        .instanceType(InstanceType.fromValue(INSTANCE_TYPE))
                        .minCount(1)
                        .maxCount(count)
                        .userData(userData)
                        .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                                .name("LabInstanceProfile")
                                .build())
                        .tagSpecifications(TagSpecification.builder()
                                .resourceType(ResourceType.INSTANCE)
                                .tags(Tag.builder()
                                        .key(INSTANCE_TAG_KEY)
                                        .value(tagValue)
                                        .build())
                                .build())
                        .build());

        return response.instances().stream()
                .map(Instance::instanceId)
                .collect(Collectors.toList());
    }

    public List<Instance> findInstancesByTag(String tagValue, InstanceStateName... states) {
        Filter tagFilter = Filter.builder()
                .name("tag:" + INSTANCE_TAG_KEY)
                .values(tagValue)
                .build();

        String[] stateStrings = Arrays.stream(states)
                .map(InstanceStateName::toString)
                .toArray(String[]::new);

        Filter stateFilter = Filter.builder()
                .name("instance-state-name")
                .values(stateStrings)
                .build();

        DescribeInstancesResponse response = ec2Client.describeInstances(
                DescribeInstancesRequest.builder()
                        .filters(tagFilter, stateFilter)
                        .build());

        return response.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .collect(Collectors.toList());
    }

    public void terminateInstances(List<String> instanceIds) {
        if (instanceIds == null || instanceIds.isEmpty()) return;

        ec2Client.terminateInstances(TerminateInstancesRequest.builder()
                .instanceIds(instanceIds)
                .build());
    }

    public String getCurrentInstanceId() {
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            URLConnection conn = url.openConnection();
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {
                String instanceId = reader.readLine();
                if (instanceId != null && !instanceId.isEmpty()) {
                    return instanceId;
                }
            }
        } catch (Exception e) {
            System.err.println("Metadata service unavailable: " + e.getMessage());
        }

        try {
            System.out.println("Falling back to finding Manager by tag...");
            DescribeInstancesResponse response = ec2Client.describeInstances(
                    DescribeInstancesRequest.builder()
                            .filters(
                                    Filter.builder().name("tag:" + INSTANCE_TAG_KEY).values(MANAGER_TAG_VALUE).build(),
                                    Filter.builder().name("instance-state-name").values("running").build()
                            )
                            .build()
            );

            String instanceId = response.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .findFirst()
                    .map(Instance::instanceId)
                    .orElse(null);

            if (instanceId != null) {
                System.out.println("Found Manager instance ID: " + instanceId);
            }
            return instanceId;
        } catch (Exception e) {
            System.err.println("Failed to find Manager by tag: " + e.getMessage());
            return null;
        }
    }

    public void close() {
        if (s3Client != null) s3Client.close();
        if (sqsClient != null) sqsClient.close();
        if (ec2Client != null) ec2Client.close();
    }
}