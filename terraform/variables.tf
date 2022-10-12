# Input variable definitions

variable "aws_region" {
  description = "AWS region for all resources."

  type    = string
  default = "eu-west-2"
}

variable "client_name"{
  description = "Suffix for all resources. Should be formatted like so: organization-project, for example, bloomingdales-150"
  default = "bloomingdales-150-v3"
}
variable "prefix"{
description = "The prefix that is used for all resource deployments."
default = "emperia-standard-api"
}
