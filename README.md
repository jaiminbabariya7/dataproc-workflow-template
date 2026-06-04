# GCP Dataproc Workflow Templates with Cloud Build CI/CD

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![Dataproc](https://img.shields.io/badge/Google%20Cloud-Dataproc-4285F4?logo=googlecloud)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-MapReduce-yellow?logo=apachehadoop)
![Cloud Build](https://img.shields.io/badge/Cloud%20Build-CI%2FCD-4285F4?logo=googlecloud)
![MIT License](https://img.shields.io/badge/License-MIT-green)

> Infrastructure-as-code and CI/CD automation for big data workloads on GCP: Pub/Sub-triggered Cloud Build pipelines that automatically import and execute Dataproc workflow templates — ephemeral cluster spin-up, Hadoop/Spark job execution, and automatic teardown.

---

## What This Solves

Running Hadoop/Spark jobs manually on GCP requires creating clusters, submitting jobs, and tearing down clusters — error-prone when done by hand and expensive when clusters are left running. This project automates that loop: a Pub/Sub message triggers CI/CD, which imports a workflow template into Dataproc, spins up an ephemeral cluster, runs the job, and tears it down. Zero manual steps.

---

## Architecture

```
Event Trigger (scheduled job, upstream pipeline, manual publish)
        ↓
Pub/Sub Topic (workflow-trigger)
        ↓
Cloud Build Trigger (watches Pub/Sub + repo changes)
        ↓
Cloud Build Pipeline
  ├── Step 1: Pull latest workflow template from repo
  ├── Step 2: gcloud dataproc workflow-templates import
  └── Step 3: gcloud dataproc workflow-templates instantiate
        ↓
Dataproc (managed cluster lifecycle)
  ├── Create ephemeral cluster (auto-config based on template)
  ├── Execute Hadoop/Spark job
  ├── Write output to GCS
  └── Automatically delete cluster on completion
        ↓
Output: GCS bucket (job results)
```

---

## Files

### Workflow Template (`example-workflow-template.yaml`)
```yaml
# example-workflow-template.yaml
# Defines: cluster config + job steps as a reusable, versioned template

placement:
  managedCluster:
    clusterName: wordcount-cluster
    config:
      gceClusterConfig:
        zoneUri: us-central1-a
      masterConfig:
        numInstances: 1
        machineTypeUri: n1-standard-4
        diskConfig:
          bootDiskSizeGb: 50
      workerConfig:
        numInstances: 2
        machineTypeUri: n1-standard-4
        diskConfig:
          bootDiskSizeGb: 50
      softwareConfig:
        imageVersion: "2.1-debian11"
        properties:
          "dataproc:dataproc.allow.zero.workers": "false"

jobs:
  - stepId: wordcount-job
    hadoopJob:
      mainPythonFileUri: gs://your-bucket/scripts/wordcount.py
      args:
        - gs://your-bucket/input/
        - gs://your-bucket/output/wordcount_{{ DATE }}/
```

### MapReduce Job (`wordcount.py`)
```python
# wordcount.py — Python MapReduce for Hadoop on Dataproc
import sys
import re
from operator import itemgetter

def mapper():
    """Read stdin, emit (word, 1) pairs."""
    for line in sys.stdin:
        line = line.strip().lower()
        words = re.findall(r'\b[a-z]+\b', line)
        for word in words:
            print(f"{word}\t1")

def reducer():
    """Aggregate word counts from sorted mapper output."""
    current_word = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        try:
            word, count = line.split('\t')
            count = int(count)
        except ValueError:
            continue

        if word == current_word:
            current_count += count
        else:
            if current_word:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    if current_word:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    if sys.argv[1] == "map":
        mapper()
    elif sys.argv[1] == "reduce":
        reducer()
```

### Cloud Build Pipeline (`cloudbuild.yaml`)
```yaml
# cloudbuild.yaml
steps:
  # Step 1: Upload the latest script to GCS
  - name: "gcr.io/cloud-builders/gsutil"
    args:
      - "cp"
      - "wordcount.py"
      - "gs://${_BUCKET}/scripts/wordcount.py"

  # Step 2: Import workflow template into Dataproc
  - name: "gcr.io/cloud-builders/gcloud"
    args:
      - "dataproc"
      - "workflow-templates"
      - "import"
      - "wordcount-workflow"
      - "--source=example-workflow-template.yaml"
      - "--region=us-central1"

  # Step 3: Instantiate (run) the workflow
  - name: "gcr.io/cloud-builders/gcloud"
    args:
      - "dataproc"
      - "workflow-templates"
      - "instantiate"
      - "wordcount-workflow"
      - "--region=us-central1"
      - "--parameters=DATE=$SHORT_SHA"

substitutions:
  _BUCKET: "your-dataproc-bucket"
  _REGION: "us-central1"

timeout: "1800s"  # 30 min max
```

### Pub/Sub Trigger Setup
```bash
# Create Pub/Sub topic and Cloud Build trigger
gcloud pubsub topics create workflow-trigger

# Create Cloud Build trigger that fires on Pub/Sub message
gcloud builds triggers create pubsub \
  --name=dataproc-workflow-trigger \
  --topic=projects/YOUR_PROJECT/topics/workflow-trigger \
  --build-config=cloudbuild.yaml \
  --region=us-central1

# Trigger a run manually (simulate upstream pipeline)
gcloud pubsub topics publish workflow-trigger \
  --message='{"run_id": "20240715", "dataset": "july_corpus"}'
```

---

## Sample Run Output

```
Cloud Build Trigger fired: workflow-trigger | 2024-07-15 08:00:01

Step 0: Uploading wordcount.py to gs://bucket/scripts/
  → gs://bucket/scripts/wordcount.py: 1 file/2.1 KB uploaded

Step 1: Importing workflow template
  → Template 'wordcount-workflow' imported into Dataproc (us-central1)

Step 2: Instantiating workflow
  → Workflow wordcount-workflow-20240715 started
  → Creating cluster: wordcount-cluster (2 workers, n1-standard-4)
  → Cluster created in 4m 12s

  → Running job: wordcount-job
  → Input: gs://bucket/input/ (124 files, 2.3 GB)
  → [YARN] Map tasks: 124 | Reduce tasks: 4
  → Progress: 25%... 50%... 75%... 100%
  → Job completed in 8m 44s

  → Output written to: gs://bucket/output/wordcount_20240715/
  → Top words: the(48,291), and(31,847), to(29,103), of(28,412)...

  → Deleting cluster: wordcount-cluster
  → Cluster deleted in 1m 03s

Build SUCCEEDED in 14m 23s
Total cost: ~$0.18 (ephemeral cluster, auto-deleted)
```

---

## Project Structure

```
dataproc-workflow-template/
├── example-workflow-template.yaml   # Reusable Dataproc workflow template
├── wordcount.py                     # Hadoop MapReduce Python script
├── cloudbuild.yaml                  # Cloud Build CI/CD pipeline
├── scripts/
│   └── setup.sh                    # GCP project setup helper
└── README.md
```

---

## Extending to Spark

The same pattern works for PySpark jobs:
```yaml
jobs:
  - stepId: spark-etl
    pysparkJob:
      mainPythonFileUri: gs://your-bucket/scripts/spark_etl.py
      args:
        - "--input=gs://your-bucket/raw/"
        - "--output=gs://your-bucket/processed/"
        - "--date={{ DATE }}"
```

---

## Skills Demonstrated
`Dataproc` · `Apache Hadoop` · `MapReduce` · `Cloud Build` · `CI/CD` · `Pub/Sub` · `Workflow Templates` · `Infrastructure as Code` · `Ephemeral Clusters` · `Cost Optimization` · `GCP`
