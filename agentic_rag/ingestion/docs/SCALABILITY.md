# Scalability Architecture

Ingestion server, as prototyped in code in this folder, can be scaled into a high-throughput, low latency system.

This document describes the scalability architecture to support the following requirements:
* 500+ TPS
* Thousands of PDF being processed concurrently

# TL;DR

The architecture scales to 500+ TPS by horizontally scaling all components and using async processing with queue-based workflows.
A Load Balancer intelligently routes PDFs from storage to GPU Workers based on load metrics and PDF characteristics.
Workers maintain local queues for efficient batching and expose metrics for smart routing decisions.
The system requires high-bandwidth networking and uses acknowledgment-based message handling to prevent data loss.

# Derived Characteristics

* Assume typical PDF size of 5 MB
* Assume 3x peak loads at 1500 TPS
* Peak ingress network bandwidth requirement: 1500 TPS * 5 MB = ~8 GB/s
  * Even ingress alone requires more bandwidth than a typical single machine can handle, before we account for returning the results, storage writes, inter-component communication, etc.

# Key Challenges

* Distribute processing efficiently across GPU Cluster when PDFs vary in size and complexity
* Ensure high network bandwidth to avoid bottlenecking due to I/O

# Architecture

Components:
* Public Gateway
* Incoming Queue (e.g. RabbitMQ)
* PDF Storage
* Load Balancer
* Workers (Worker Pool / GPU Cluster)

Additional components (detailed analysis out of scope for this document):
* Status Tracking (e.g. MongoDB)
* Vector database (or other result storage) - possibly external

Notes:
* Public Gateway is user-facing, accepts incoming requests, and puts:
  * processing request metadata into Incoming Queue
  * PDF file into PDF Storage
* Load Balancer is subscribed to Incoming Queue and routes requests to Workers for computationally expensive processing.
* Each of the above components is horizontally scaled for high I/O and high availability.
* Worker Pool deploys data processing engine (based on the ingestion server prototype) to multiple GPU-equipped instances.
* Worker writes final results into Vector database (or other result storage).

# Response delivery

* Ingestion request returns immediately after submission with asynchronous processing scheduled.
* User can poll a separate status endpoint (hosted by Public Gateway) to get current processing status and retrieve results when ready.

# Workers
* Each Worker has its own dedicated input queue (local, on-device) - worker queue. Each Worker can have requests currently running and/or queued. Worker-side queued requests enable efficient batching and the ability to temporarily put a request back into the queue to avoid OOM.
* Each Worker exposes current load metrics (requests running, requests queued, latency, throughput, memory usage). This enables efficient load balancing, monitoring and troubleshooting.
* Worker can temporarily refuse new requests if overloaded

# Load Balancing
* Load Balancer retrieves metadata from Incoming Queue, fetches corresponding PDFs from Storage, and forwards both to Workers via internal worker API.
* Since different requests can require different amount of computational resources and/or processing time, we can use more advanced load balancing instead of round-robin or random. Instead, load balancing can be both:
  * load-aware based on the Worker-exposed metrics to efficiently route differently-sized requests. For instance, this enables us to avoid pushing more data to saturated Workers (workers with many queued requests).
  * resource-aware based on initial analysis of PDF characteristics (e.g. file size or number of pages). Intelligently route PDFs based on size or split them up.
* Load Balancer has a delay-based retry mechanism (e.g. exponential delays)
* Load Balancer acknowledges Incoming Queue requests only once data processing is complete to avoid data loss

Note: A future optimization to consider - eliminate Load Balancer, ensure Workers pull from Incoming Queue and self-manage their load, and ensure Workers download from PDF Storage directly. This would reduce one network hop and eliminate a single point of failure, but skipped for the initial version for simplicity.

# I/O

* Public Gateway, Incoming Queue, PDF Storage, Load Balancer are horizontally scaled to ensure I/O capacity.
* Networking considerations for self-hosted:
  * Fast network interfaces for all key components
  * Fast underlying networking e.g. data center switches
  * Isolate into multiple networks

# Autoscaling

* Automatically spin up / down Workers based on metrics such as:
  * Incoming Queue depth
  * Average per-worker queue depth
  * Average Worker GPU memory utilization
  * Average Worker request latency
* Maintain a capacity buffer (e.g., 20-30% above current demand) to avoid cold-start delays for sudden spikes

# Reliability

* Load Balancer acknowledges Incoming Queue requests only once data processing is complete to avoid data loss
* Components are horizontally scaled for redundancy
* Users have rate limits (enforced at Gateway) for:
  * concurrent requests for a single user
  * total processing volume per minute for a single user (e.g. tokens/minute or pages/minute)
* Future improvement to consider: scan PDFs for malware before accepting for processing

# Error handling

* While worker is processing, Incoming Queue still keeps the message unacknowledged.
* If worker fails explicitly, the error is returned to Load Balancer and worker no longer holds the processing request.
  * Depending on the error, Load Balancer can retry the request on another worker after a delay, or return the error back to the user.
  * For retryable errors, number of retries is limited.
* If worker crashes or becomes unresponsive, Load Balancer will retry the request on another worker after a delay.

# Observability / alerting

* End-to-end request and PDF identifiers propagated through the system for tracing
* Alerts per worker and for the overall system:
  * queue depth too high
  * request latency too high
  * error rates too high
  * failing health checks
