

variable "batch_ingestion_lambda_name" {
  type = string
}

variable "etl_glue_job_name" {
  type = string
}

variable "clean_glue_job_name" {
  type = string
}

variable "crawler_names" {
  description = "List of crawler names to run in parallel"
  type        = list(string)
}



variable "env" {
  type = string
}

# EventBridge Variables
variable "sf_eventbridge_rule_name" {
  description = "Name of the EventBridge rule for Step Functions trigger"
  type        = string
  default     = "step-functions-trigger"
}

variable "sf_eventbridge_rule_description" {
  description = "Description of the EventBridge rule for Step Functions trigger"
  type        = string
  default     = "Triggers Step Functions data pipeline execution"
}

variable "sf_eventbridge_schedule_expression" {
  description = "Schedule expression for EventBridge rule (e.g., 'rate(1 hour)' or 'cron(0 2 * * ? *)' (2 AM every day))"
  type        = string
}

