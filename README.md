# Distributed Text Analysis Pipeline

A cloud-based distributed system for parallel natural language processing using AWS infrastructure (EC2, S3, SQS).

## What It Does

Processes large volumes of text documents using three types of linguistic analysis:
- **POS Tagging** - Identifies grammatical roles
- **Constituency Parsing** - Phrase structure trees
- **Dependency Parsing** - Word relationships

## Architecture
```
Local Client → S3 → Manager Node → SQS → Worker Pool → Results
```

- **Client**: Submits jobs and retrieves results
- **Manager**: Coordinates workers based on workload (launches 1 worker per n tasks)
- **Workers**: Process tasks in parallel using Stanford CoreNLP

### Key Features
- **Auto-scaling**: Dynamically launches workers based on workload
- **Fault-tolerant**: SQS visibility timeout handles worker failures
- **Parallel processing**: Workers operate independently
- **Auto-cleanup**: Terminates all resources after completion

## Setup

### AWS Credentials

Before running AWS CLI or this application, export your credentials in the terminal:

```bash
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
export AWS_SESSION_TOKEN=your-session-token
```

### Prerequisites
- AWS Account with IAM permissions
- Java 17+
- Maven 3.6+

### Build
```bash
mvn clean package
```

### Deploy
1. Create S3 bucket:
```bash
aws s3 mb s3://your-bucket-name
```

2. Upload JARs:
```bash
aws s3 cp target/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar s3://your-bucket/jars/
aws s3 cp target/lib/ s3://your-bucket/jars/lib/ --recursive
```

3. Update `AWS.java`:
```java
public static final String S3_BUCKET_NAME = "your-bucket-name";
public static final String AMI_ID = "ami-xxxxxxxxx";
```

## Usage
```bash
java -jar distributed-text-analysis-pipeline.jar <input_file> <output_file> <n> [terminate]
```
The project contains example input files in the input-samples folder for testing and demonstration purposes.

**Parameters:**
- `input_file`: Text file with URLs and analysis types
- `output_file`: Generated HTML summary
- `n`: Tasks per worker (e.g., 3 means 1 worker per 3 tasks)
- `terminate`: (optional) Shutdown system after completion

**Input Format** (tab-separated):
```
POS	https://example.com/doc1.txt
CONSTITUENCY	https://example.com/doc2.txt
DEPENDENCY	https://example.com/doc3.txt
```

**Example:**
```bash
  java -jar target/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar input-samples/input-sample.txt test.html 1 terminate
```

## Configuration

**Instance Type**: t3.micro  
**Region**: us-east-1  
**Max Workers**: 18 (AWS limit: 19 instances including manager)

## Performance

| Tasks | Workers (n=3) | Time |
|-------|---------------|------|
| 9     | 3             | ~45s |
| 18    | 6             | ~60s |

## Architecture Details

### Scalability
- Horizontal scaling: Workers scale linearly with workload
- Manager launches workers using formula: `workers = ⌈tasks / n⌉`
- Concurrent job support: Multiple clients can submit jobs simultaneously

### Fault Tolerance
- **Worker failures**: Tasks automatically reassigned (SQS visibility timeout: 10 min)
- **Network errors**: Workers catch exceptions, send error messages, continue processing
- **Manager recovery**: Detects existing manager before launching new one

### Concurrency
- **Manager**: Thread pool (20 threads) handles multiple jobs in parallel
- **Workers**: Single-threaded polling loop (no coordination overhead)

## Troubleshooting

**Manager not starting?**
- Check S3 bucket has JARs uploaded
- Verify IAM permissions (EC2, S3, SQS)

**Workers not processing?**
- Check EC2 console for running workers
- SSH into worker: `ssh -i key.pem ubuntu@<worker-ip>` → `tail -f worker.log`

**Tasks failing?**
- Check input URLs are accessible
- Verify text files are under 10MB