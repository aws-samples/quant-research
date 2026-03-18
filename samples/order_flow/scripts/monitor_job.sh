#!/bin/bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# monitor_job.sh - Complete job monitoring script for AWS Batch jobs
# Usage: ./monitor_job.sh <job-id>

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    print_error "jq is required but not installed. Please install jq first."
    exit 1
fi

JOB_ID="$1"
if [ -z "$JOB_ID" ]; then
    echo "Usage: $0 <job-id>"
    echo ""
    echo "Examples:"
    echo "  $0 12345678-1234-1234-1234-123456789012"
    echo "  $0 \$(aws batch list-jobs --job-queue my-queue --job-status RUNNING --query 'jobList[0].jobId' --output text)"
    exit 1
fi

# Try to source environment variables
if [ -f "../../infrastructure/.env" ]; then
    source ../../infrastructure/.env
    print_status "Loaded environment from ../../infrastructure/.env"
elif [ -f "infrastructure/.env" ]; then
    source infrastructure/.env
    print_status "Loaded environment from infrastructure/.env"
else
    print_warning "Could not find .env file. Some log group detection may not work."
fi

echo "=== Monitoring AWS Batch Job: $JOB_ID ==="

# Get job details
print_status "Getting job details..."
JOB_DETAILS=$(aws batch describe-jobs --jobs $JOB_ID --output json 2>/dev/null)

if [ $? -ne 0 ] || [ -z "$JOB_DETAILS" ]; then
    print_error "Failed to get job details. Check if job ID is correct and you have AWS permissions."
    exit 1
fi

# Extract basic job info
JOB_NAME=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobName // "unknown"')
JOB_STATUS=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobStatus // "unknown"')
JOB_QUEUE=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobQueue // "unknown"')
JOB_DEFINITION=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobDefinition // "unknown"')
CREATED_AT=$(echo $JOB_DETAILS | jq -r '.jobs[0].createdAt // "unknown"')

echo ""
echo "📋 Job Information:"
echo "   Name: $JOB_NAME"
echo "   Status: $JOB_STATUS"
echo "   Queue: $JOB_QUEUE"
echo "   Definition: $JOB_DEFINITION"
echo "   Created: $(date -d @$((CREATED_AT/1000)) 2>/dev/null || echo $CREATED_AT)"

# Determine log group based on job queue
print_status "Determining log group..."

if [[ $JOB_QUEUE == *"cpu"* ]]; then
    if [ -n "$NAMESPACE" ]; then
        LOG_GROUP="/${NAMESPACE}/batch-job/single-node-with-cpu"
    else
        LOG_GROUP="/aws/batch/job"
    fi
    print_status "Detected CPU job queue, using log group: $LOG_GROUP"
elif [[ $JOB_QUEUE == *"gpu"* ]]; then
    if [ -n "$NAMESPACE" ]; then
        LOG_GROUP="/${NAMESPACE}/batch-job/multi-node-with-gpu"
    else
        LOG_GROUP="/aws/batch/job"
    fi
    print_status "Detected GPU job queue, using log group: $LOG_GROUP"
else
    LOG_GROUP="/aws/batch/job"
    print_status "Using default log group: $LOG_GROUP"
fi

# Try to get log stream from job container
print_status "Getting log stream information..."
LOG_STREAM=$(echo $JOB_DETAILS | jq -r '.jobs[0].container.logStreamName // empty' 2>/dev/null)

# Get log group from logConfiguration
ACTUAL_LOG_GROUP=$(echo $JOB_DETAILS | jq -r '.jobs[0].container.logConfiguration.options."awslogs-group" // empty' 2>/dev/null)

# Use actual log group if available, otherwise fall back to determined one
if [ -n "$ACTUAL_LOG_GROUP" ] && [ "$ACTUAL_LOG_GROUP" != "null" ] && [ "$ACTUAL_LOG_GROUP" != "empty" ]; then
    LOG_GROUP="$ACTUAL_LOG_GROUP"
    print_status "Using actual log group from job: $LOG_GROUP"
fi

if [ -z "$LOG_STREAM" ] || [ "$LOG_STREAM" = "null" ] || [ "$LOG_STREAM" = "empty" ]; then
    print_warning "Log stream not available yet. Job may be in SUBMITTED/PENDING/RUNNABLE status."
    
    # Show current status and wait
    echo ""
    echo "🔄 Current job status: $JOB_STATUS"
    
    if [[ "$JOB_STATUS" == "SUBMITTED" || "$JOB_STATUS" == "PENDING" || "$JOB_STATUS" == "RUNNABLE" ]]; then
        print_status "Waiting for job to start (checking every 30 seconds)..."
        
        while true; do
            sleep 30
            JOB_DETAILS=$(aws batch describe-jobs --jobs $JOB_ID --output json 2>/dev/null)
            JOB_STATUS=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobStatus // "unknown"')
            
            echo "$(date): Job status: $JOB_STATUS"
            
            if [[ "$JOB_STATUS" == "RUNNING" ]]; then
                LOG_STREAM=$(echo $JOB_DETAILS | jq -r '.jobs[0].attempts[0].taskProperties.containers[0].logStreamName // empty' 2>/dev/null)
                if [ -n "$LOG_STREAM" ] && [ "$LOG_STREAM" != "null" ] && [ "$LOG_STREAM" != "empty" ]; then
                    break
                fi
            elif [[ "$JOB_STATUS" == "SUCCEEDED" || "$JOB_STATUS" == "FAILED" ]]; then
                LOG_STREAM=$(echo $JOB_DETAILS | jq -r '.jobs[0].attempts[0].taskProperties.containers[0].logStreamName // empty' 2>/dev/null)
                break
            fi
        done
    fi
fi

if [ -n "$LOG_STREAM" ] && [ "$LOG_STREAM" != "null" ] && [ "$LOG_STREAM" != "empty" ]; then
    echo ""
    echo "📊 Log Details:"
    echo "   Log Group: $LOG_GROUP"
    echo "   Log Stream: $LOG_STREAM"
    
    print_status "Fetching recent logs..."
    
    # Check if log group exists
    if aws logs describe-log-groups --log-group-name-prefix "$LOG_GROUP" --query 'logGroups[0].logGroupName' --output text 2>/dev/null | grep -q "$LOG_GROUP"; then
        
        # Check if log stream exists
        if aws logs describe-log-streams --log-group-name "$LOG_GROUP" --log-stream-name-prefix "$LOG_STREAM" --query 'logStreams[0].logStreamName' --output text 2>/dev/null | grep -q "$LOG_STREAM"; then
            
            echo ""
            echo "📜 Recent Logs (last 50 events):"
            echo "$(printf '=%.0s' {1..80})"
            
            aws logs get-log-events \
              --log-group-name "$LOG_GROUP" \
              --log-stream-name "$LOG_STREAM" \
              --start-from-head \
              --query 'events[-50:].[timestamp,message]' \
              --output text | while IFS=$'\t' read -r timestamp message; do
                if [ -n "$timestamp" ] && [ "$timestamp" != "None" ]; then
                    formatted_time=$(date -d @$((timestamp/1000)) '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "[$timestamp]")
                    echo "[$formatted_time] $message"
                fi
            done
            
            echo "$(printf '=%.0s' {1..80})"
            
        else
            print_warning "Log stream '$LOG_STREAM' not found in log group '$LOG_GROUP'"
        fi
    else
        print_warning "Log group '$LOG_GROUP' not found"
    fi
else
    print_warning "No log stream available for this job"
fi

# Monitor job progress if it's still running
echo ""
if [[ "$JOB_STATUS" == "RUNNING" || "$JOB_STATUS" == "RUNNABLE" || "$JOB_STATUS" == "PENDING" ]]; then
    print_status "Job is still active. Monitoring progress (Ctrl+C to stop)..."
    
    while true; do
        sleep 30
        JOB_DETAILS=$(aws batch describe-jobs --jobs $JOB_ID --output json 2>/dev/null)
        NEW_STATUS=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobStatus // "unknown"')
        
        if [ "$NEW_STATUS" != "$JOB_STATUS" ]; then
            JOB_STATUS=$NEW_STATUS
            echo "$(date): Job status changed to: $JOB_STATUS"
        else
            echo "$(date): Job status: $JOB_STATUS"
        fi
        
        if [[ "$JOB_STATUS" == "SUCCEEDED" ]]; then
            print_success "Job completed successfully!"
            break
        elif [[ "$JOB_STATUS" == "FAILED" ]]; then
            print_error "Job failed!"
            
            # Try to get failure reason
            FAILURE_REASON=$(echo $JOB_DETAILS | jq -r '.jobs[0].attempts[0].statusReason // "No failure reason available"')
            echo "Failure reason: $FAILURE_REASON"
            break
        fi
    done
else
    echo "🏁 Job has completed with status: $JOB_STATUS"
    
    if [ "$JOB_STATUS" == "FAILED" ]; then
        FAILURE_REASON=$(echo $JOB_DETAILS | jq -r '.jobs[0].attempts[0].statusReason // "No failure reason available"')
        print_error "Failure reason: $FAILURE_REASON"
    fi
fi

# Show log streaming command for future reference
echo ""
print_status "Commands for future monitoring:"
echo "  View job status: aws batch describe-jobs --jobs $JOB_ID"

if [ -n "$LOG_STREAM" ] && [ "$LOG_STREAM" != "null" ] && [ "$LOG_STREAM" != "empty" ]; then
    echo "  Stream logs live: aws logs tail '$LOG_GROUP' --log-stream-names '$LOG_STREAM' --follow"
    echo "  Get all logs: aws logs get-log-events --log-group-name '$LOG_GROUP' --log-stream-name '$LOG_STREAM' --start-from-head"
fi
