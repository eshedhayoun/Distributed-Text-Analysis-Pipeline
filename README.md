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
- **Fault-tolerant**: SQS visibility timeout handles worker failures; Manager crash recovery via S3 state persistence
- **Manager Watchdog**: Monitors manager health and automatically restarts if it crashes
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
aws s3 mb s3://your-s3-bucket-name --region <your-region>```     

2. Upload JARs:
```bash
aws s3 cp target/distributed-text-analysis-pipeline-1.0-SNAPSHOT.jar s3://your-s3-bucket-name/jars/
aws s3 cp target/lib/ s3://distributed-text-analysis-pipeline-inputs-us-east-1/jars/lib/ --recursive
```

3. Update `AWS.java`:
```java
public static final String S3_BUCKET_NAME = "your-s3-bucket-name";
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
POS     https://example.com/doc1.txt
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
- Processing time: ~90 seconds (including manager startup)
- Output file: `output.html`

## How It Works

### Process Flow

1. **Client** reads input file, uploads it to S3, and sends job request to Manager queue with job details (input location, n value). Launches manager watchdog to monitor manager health. Then polls output queue for results.

2. **Manager** receives job request, calculates workers needed (⌈tasks/n⌉), creates job-specific queues, discovers existing workers to reuse, launches additional workers if needed, distributes tasks to task queue, saves job state to S3, and monitors result queue. When all tasks complete, it generates HTML output, uploads to S3, notifies client, and marks job complete in S3.

3. **Workers** receive queue URLs via EC2 user data, download CoreNLP, poll task queue, process NLP tasks, send results to result queue (manager saves each result to S3), and continue processing until queue is empty.

4. **Watchdog** monitors manager every 60 seconds. If manager is not in "pending" or "running" state for 3 consecutive checks, examines pending work in queues. If work remains, restarts manager. New manager restores job state from S3 and continues processing. If no work remains and terminate mode is active, allows graceful shutdown.

5. **Client** receives completion message, downloads output HTML from S3, and optionally sends termination signal.

## Architecture Details

### Scalability

The system can handle many concurrent clients because:
- Workers scale horizontally based on workload, not client count
- Workers are reused across jobs - new jobs use existing workers before launching more
- Manager uses a thread pool (20 threads) to handle multiple jobs in parallel
- Each job gets dedicated result queues, preventing interference
- SQS and S3 scale automatically
- Job state persisted in S3 allows recovery without data loss

For 1 million clients: The manager's thread pool handles concurrent jobs. Workers are launched per-job based on task count. As long as manager has enough threads (can be increased), the system scales.

For larger scale (billions): Would need multiple manager instances behind a load balancer, but current design easily handles thousands of concurrent clients.

### Fault Tolerance

**Worker failures:** If a worker crashes, SQS visibility timeout (10 minutes) makes the task visible again, and another worker processes it. Results are saved to S3 as they complete, so no data is lost.

**Manager failures:**
- **During initialization:** Watchdog detects manager stuck or terminated. After 3 failed checks (3 minutes), watchdog restarts manager. New manager takes ownership of existing workers and queues.
- **After initialization:** Watchdog monitors manager state every 60 seconds. If manager crashes and work remains (tasks in queues or processing), watchdog automatically restarts manager. New manager:
    - Restores job state from S3 (which tasks were submitted, which results already received)
    - Discovers and reuses existing workers
    - Continues processing from where it left off
    - No duplicate work or lost results
- **Graceful termination:** When job completes and terminate signal received, watchdog verifies no pending work before allowing shutdown.

**Network errors:** Workers catch exceptions and send error messages to result queue. Manager marks failed tasks in output. All AWS services have built-in redundancy.

**Broken communications:** The AWS SDK automatically retries failed calls. Workers continue processing other tasks if one URL fails.

### Manager Crash Recovery Details

**State Persistence:**
- Job metadata (task count, status) saved to S3 when job starts
- Each result saved to S3 as it arrives from workers
- Job marked complete in S3 when all tasks finish

**Recovery Process:**
1. New manager launches and connects to existing queues
2. Scans S3 for in-progress jobs (status = "PROCESSING")
3. Restores job state: task counts and received results
4. Continues monitoring result queue for remaining tasks
5. Generates summary when all tasks complete

**Example Scenario:**
```
Job submitted: 100 tasks
Manager receives 50 results → crashes
Watchdog detects crash → launches new manager
New manager restores state from S3:
  - Knows 100 tasks were submitted
  - Has 50 results already saved
  - Waits for remaining 50 results
Workers continue processing (unaware of manager restart)
New manager receives remaining 50 results
Job completes successfully
```

### Concurrency

**Threads usage:**
- **Manager:** Uses thread pool (20 threads) to handle multiple client jobs concurrently. This is beneficial because it prevents one slow job from blocking others. Each job is handled independently.
- **Workers:** Single-threaded. No thread pool needed because CoreNLP is CPU-intensive, and we achieve parallelism by launching multiple workers instead. Simpler code, no synchronization overhead.
- **Client:** Single-threaded with background watchdog thread. Submits job and waits for result while watchdog monitors manager health.

**Why this design?** Threading is good when waiting for I/O or handling multiple independent tasks. Not good when tasks are CPU-bound (better to use multiple processes/instances).

### Persistence

**What if a node dies?**
- **Worker death:** Task automatically reassigned by SQS visibility timeout (10 minutes)
- **Manager death during initialization:** Watchdog detects and restarts within 3 minutes
- **Manager death after initialization:** Watchdog detects, verifies pending work exists, restarts manager. New manager restores state from S3 and continues
- **Network failure:** AWS SDK retries handle temporary issues

**What if a node stalls?**
- **Worker stalls:** SQS visibility timeout reassigns task after 10 minutes
- **Manager stalls:** Watchdog detects after 3 consecutive failed health checks (3 minutes) and restarts

**State Recovery:**
All job state is persisted in S3 under `manager-state/`:
```
manager-state/
├── DoneQueue-{jobId}/
│   ├── metadata.json          # Job info (total tasks, status)
│   └── results/
│       ├── {uuid1}.txt        # Result 1
│       ├── {uuid2}.txt        # Result 2
│       └── {uuid3}.txt        # Result 3
```

### Watchdog Monitoring

The local application runs a background watchdog thread that:
- Starts immediately when manager is launched
- Checks manager state every 60 seconds
- Considers manager healthy if state is "pending" (initializing) or "running"
- Counts 3 consecutive unhealthy checks before taking action
- Examines queue message counts to detect pending work
- Restarts manager only if work remains unfinished
- Allows graceful termination when no work remains and terminate mode is active

### Termination

With "terminate" parameter:
1. Client marks job as complete and sends terminate message
2. Manager waits for active tasks to complete (max 2 minutes)
3. Manager terminates all workers
4. Manager deletes all job queues (input, task, result, watchdog)
5. Manager terminates itself
6. Watchdog verifies no pending work and allows shutdown
7. Client confirms termination within 180 seconds

Without "terminate": System stays running for future jobs. Manager and workers remain active.

### System Limitations

- AWS allows 19 instances in us-east-1 (1 manager + 18 workers max)
- t3.micro has 1GB RAM, limits document size to ~10MB
- SQS visibility timeout is 10 minutes (sufficient for most NLP processing)
- Manager state recovery depends on S3 availability
- Design accounts for these: n parameter controls worker count, auto-termination frees instance quota, S3 provides durable state storage

### Worker Efficiency

All workers work continuously until the task queue is empty. SQS distributes tasks evenly (first-available gets next task), so no worker sits idle while tasks remain. Workers are reused across jobs - when a new job arrives, the manager discovers existing workers and launches additional ones only if needed. Some workers finish earlier because task complexity varies (POS is faster than Dependency parsing), but this is expected and efficient.

### Manager Responsibilities

The manager handles coordination: accepting jobs, launching workers (or reusing existing ones), distributing tasks via queues, aggregating results, and persisting state to S3. It does NOT process NLP tasks or download documents—that's the workers' job. Clear separation: manager = orchestration + state management, workers = computation.

### Understanding Distributed Systems

This system is truly distributed because:
- Workers operate independently with no inter-worker communication
- All communication is asynchronous via message queues
- Manager doesn't wait synchronously for workers (uses result queue)
- State is persisted externally (S3), not just in memory
- One component failure doesn't block others - system recovers automatically
- Multiple workers process tasks in parallel simultaneously
- Workers are shared across jobs for efficiency
- Watchdog provides autonomous failure detection and recovery
- Nothing waits unnecessarily—the only blocking is the client waiting for final results, which is expected

## Troubleshooting

**Manager won't start:**
- Check AWS credentials are exported
- Verify AMI ID is correct for your region
- Ensure S3 bucket exists and JARs are uploaded
- Check IAM role has necessary permissions

**Workers not launching:**
- Verify instance limit (19 total including manager)
- Check S3 bucket has worker JAR and lib/ folder
- Ensure security groups allow necessary traffic

**Job stuck:**
- Check SQS queues for messages (input, task, result)
- Verify manager is running: `aws ec2 describe-instances`
- Check manager logs via SSH or CloudWatch
- Watchdog will auto-restart manager if it crashes

**Cleanup not happening:**
- Ensure "terminate" parameter is included
- Wait 180 seconds for full cleanup
- Manually clean up if needed:
```bash
  aws ec2 terminate-instances --instance-ids $(aws ec2 describe-instances --filters "Name=instance-state-name,Values=running" --query 'Reservations[].Instances[].InstanceId' --output text)
  aws sqs list-queues --query 'QueueUrls[]' --output text | xargs -I {} aws sqs delete-queue --queue-url {}
```

## Future Improvements

- **Multiple managers:** Load balancer for extreme scale
- **Better monitoring:** CloudWatch metrics and alarms
- **Cost optimization:** Spot instances for workers
- **Enhanced recovery:** DynamoDB for faster state queries
- **UI dashboard:** Web interface for job monitoring