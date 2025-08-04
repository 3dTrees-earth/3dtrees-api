## 1. API Application Overview

The API application serves as a **unified orchestration layer** between the frontend/CLI and Galaxy (the job processing system), managing all database interactions and Galaxy communication.

**v1 Capabilities:**

- **Single endpoint**: `POST /jobs` - accepts pipeline definitions for data processing workflows
- **Job orchestration**: Manages complex job dependencies (standardization → segmentation → species ID) through database state
- **Pipeline validation**: Uses Pydantic models to validate job structures and dependencies
- **Background processing**: Async job execution with periodic Galaxy API polling (every 5 seconds)
- **Database management**: Updates job statuses in database based on Galaxy responses; frontend subscribes to database for real-time updates
- **Rerun/override support**: Ability to reprocess existing jobs with override flags

**Future Extensions:**

- Upload URL generation (currently handled by separate cloud function)
- Download URL management
- CLI support for job management
- Enhanced job parameter customization

The core philosophy: **Database as single source of truth** with the API handling all Galaxy communication through background tasks and database updates.

## 2. System Flow Diagram

```mermaid
sequenceDiagram
    participant FE as Frontend
    participant API as API Application
    participant DB as Database
    participant BG as Background Task
    participant Galaxy as Galaxy API

    Note over FE: User uploads dataset
    FE->>DB: Create dataset record
    DB-->>FE: Return dataset_id

    FE->>API: POST /jobs<br/>{pipeline: [job1, job2, job3], dataset_id, override: false}
    API->>API: Validate pipeline with Pydantic
    API->>DB: Check dependencies & existing jobs
    API->>DB: Create job records with status "pending"
    API-->>FE: Return success (job_ids)

    API->>BG: Start background task (async)

    loop For each job in pipeline
        BG->>DB: Check if dependencies complete
        alt Dependencies ready
            BG->>Galaxy: Start job with parameters
            Galaxy-->>BG: Return galaxy_job_id
            BG->>DB: Update job status to "running"

            loop Every 5 seconds
                BG->>Galaxy: Query job status
                Galaxy-->>BG: Return current status
                alt Status changed
                    BG->>DB: Update job status
                end
            end

            alt Job complete
                BG->>Galaxy: Retrieve job outputs
                Galaxy-->>BG: Return output file locations
                BG->>DB: Update job status to "complete"<br/>Store output metadata
            end
        end
    end

    Note over DB,FE: Real-time subscription
    DB-->>FE: Status updates (pending→running→complete)
```

## 3. Feature Breakdown

### **Feature 1: Pydantic Job Validation System**

This feature creates a comprehensive validation framework that handles all complex pipeline logic including job type validation, dependency chain verification (DAG structure), parameter validation, and database lookups for existing jobs. It serves as the "smart" component that ensures incoming pipelines are valid, properly ordered, and ready for execution before passing them to the processing engine.

### **Feature 2: Job Pipeline Processing Engine**

This component is intentionally "dumb" - it takes pre-validated pipeline structures from the validation system and simply executes them in order. It manages job queue mechanics in the database, tracks execution state, and handles the basic flow of moving jobs from pending to running status without making complex decisions about pipeline validity or dependencies.

### **Feature 3: Background Task Runner & Job Monitor**

This feature implements the async background processing system that monitors Galaxy jobs every 5 seconds and updates the database accordingly. It manages the lifecycle of jobs from submission through completion, handles parallel job execution, and ensures database consistency throughout the process.

### **Feature 4: Galaxy API Integration Layer**

This component provides a clean interface to Galaxy's API for job submission, status monitoring, and output retrieval. It abstracts away Galaxy-specific communication details, handles API errors gracefully, and manages the mapping between internal job representations and Galaxy's job format.
