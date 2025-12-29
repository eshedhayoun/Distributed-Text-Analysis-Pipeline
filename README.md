# Distributed Text Analysis Pipeline

**Author:**
Eshed Hayoun 

This is a cloud-based distributed system for parallel natural language processing using AWS infrastructure (EC2, S3, SQS).

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

**IMPORTANT - Security:** Never hardcode your AWS credentials in the code! Always export them as environment variables:
```bash
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
export AWS_SESSION_TOKEN=your-session-token
```

The application uses the AWS SDK's default credential provider chain, which reads these environment variables automatically. This ensures credentials are never exposed in source code or version control.

### Prerequisites
- AWS Account with IAM permissions
- Java 17+
- Maven 3.6+

### Build
```bash
mvn clean package
```

### Deploy

1. Create S3 Bucket:
```bash
aws s3 mb s3://distributed-text-analysis-pipeline-inputs-us-east-1 --region us-east-1
```     

2. Upload JARs:
```bash
aws s3 cp target/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar s3://distributed-text-analysis-pipeline-inputs-us-east-1/jars/
aws s3 cp target/lib/ s3://distributed-text-analysis-pipeline-inputs-us-east-1/jars/lib/ --recursive
```

3. Update `AWS.java`:
```java
public static final String AMI_ID = "ami-xxxxxxxxxxxxxxxxx"; // Ubuntu 22.04 LTS
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
java -jar target/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar input-samples/input-sample.txt output.html 3 terminate
```

## Configuration

**Instance Type**: t3.micro  
**AMI**: ami-xxxxxxxxxxxxxxxxx (Ubuntu 22.04 LTS)  
**Region**: us-east-1  
**Max Workers**: 18 (AWS limit: 19 instances including manager)

## Performance

**Sample File Output:**
- Input: `input-samples/input-sample.txt` (9 tasks)
- n value: 3
- Workers launched: 3
- Processing time: 45 seconds
- Output file: `output.html` 

## How It Works

### Process Flow

1. **Client** reads input file, uploads it to S3, and sends job request to Manager queue with job details (input location, n value). Then polls output queue for results.

2. **Manager** receives job request, calculates workers needed (⌈tasks/n⌉), creates job-specific queues, launches workers, distributes tasks to task queue, and monitors result queue. When all tasks complete, it generates HTML output, uploads to S3, and notifies client.

3. **Workers** receive queue URLs via EC2 user data, download CoreNLP, poll task queue, process NLP tasks, send results to result queue, and terminate when queue is empty.

4. **Client** receives completion message, downloads output HTML from S3, and optionally sends termination signal.

## Architecture Details

### Scalability

The system can handle many concurrent clients because:
- Workers scale horizontally based on workload, not client count
- Manager uses a thread pool (20 threads) to handle multiple jobs in parallel
- Each job gets dedicated SQS queues, preventing interference
- SQS and S3 scale automatically

For 1 million clients: The manager's thread pool handles concurrent jobs. Workers are launched per-job based on task count. As long as manager has enough threads (can be increased), the system scales.

For larger scale (billions): Would need multiple manager instances behind a load balancer, but current design easily handles thousands of concurrent clients.

### Fault Tolerance

**Worker failures:** If a worker crashes, SQS visibility timeout (10 minutes) makes the task visible again, and another worker processes it. No data is lost.

**Manager failures:** Manager checks for existing instance before launching. If it crashes mid-job, client timeout (15 minutes) alerts the user to resubmit. Future improvement would use DynamoDB for state persistence.

**Network errors:** Workers catch exceptions and send error messages to result queue. Manager marks failed tasks in output. All AWS services have built-in redundancy.

**Broken communications:** The AWS SDK automatically retries failed calls. Workers continue processing other tasks if one URL fails.

### Concurrency

**Threads usage:**
- **Manager:** Uses thread pool (20 threads) to handle multiple client jobs concurrently. This is beneficial because it prevents one slow job from blocking others.
- **Workers:** Single-threaded. No thread pool needed because CoreNLP is CPU-intensive, and we achieve parallelism by launching multiple workers instead. Simpler code, no synchronization overhead.
- **Client:** Single-threaded. Just submits job and waits for result.

**Why this design?** Threading is good when waiting for I/O or handling multiple independent tasks. Not good when tasks are CPU-bound (better to use multiple processes/instances).

### Persistence

**What if a node dies?**
- Worker death: Task automatically reassigned by SQS
- Manager death: Job must be resubmitted (state not persisted)
- Network failure: Retries handle temporary issues

**What if a node stalls?**
- Worker stalls: SQS visibility timeout reassigns task after 10 minutes
- Manager stalls: Client timeout prevents infinite waiting

### Termination

With "terminate" parameter:
1. Client sends terminate message after receiving results
2. Manager waits for active jobs to complete
3. Manager terminates all workers
4. Manager deletes job queues
5. Manager terminates itself

Without "terminate": System stays running for future jobs.

### System Limitations

- AWS allows 19 instances in us-east-1 (1 manager + 18 workers max)
- t3.micro has 1GB RAM, limits document size to ~10MB
- SQS visibility timeout is 10 minutes (sufficient for worst-case NLP processing)
- Design accounts for these: n parameter controls worker count, auto-termination frees instance quota

### Worker Efficiency

All workers work continuously until the task queue is empty. SQS distributes tasks evenly (first-available gets next task), so no worker sits idle while tasks remain. Some workers finish earlier because task complexity varies (POS is faster than Dependency parsing), but this is expected and efficient.

### Manager Responsibilities

The manager only handles coordination: accepting jobs, launching workers, distributing tasks via queues, and aggregating results. It does NOT process NLP tasks or download documents—that's the workers' job. Clear separation: manager = orchestration, workers = computation.

### Understanding Distributed Systems

This system is truly distributed because:
- Workers operate independently with no inter-worker communication
- All communication is asynchronous via message queues
- Manager doesn't wait synchronously for workers (uses result queue)
- One component failure doesn't block others
- Multiple workers process tasks in parallel simultaneously
- Nothing waits unnecessarily—the only blocking is the client waiting for final results, which is expected
