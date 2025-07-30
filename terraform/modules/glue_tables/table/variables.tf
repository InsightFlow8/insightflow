variable "name" {
  type = string
}

variable "database_name" {
  type = string
}

variable "location" {
  description = "S3 location for the table data"
  type        = string
}

variable "columns" {
  description = "List of columns for the Glue table"
  type = list(object({
    name = string
    type = string
  }))
}

variable "compression_type" {
  description = "Compression type for the table (not used for CSV)"
  type        = string
  default     = "none"
}
