provider "aws" {
  region = var.aws_region
}

resource "random_pet" "lambda_bucket_name" {
  length = 3
}

resource "aws_s3_bucket" "lambda_bucket" {
  bucket = "${var.prefix}-lambda-container-${var.client_name}"
}

resource "aws_s3_bucket_acl" "bucket_acl" {
  bucket = aws_s3_bucket.lambda_bucket.id
  acl    = "private"
}


 data "archive_file" "source" {
   type = "zip"

   source_dir  = "${path.module}/../Src"
   output_path = "${path.module}/../Src.zip"
 }

resource "aws_s3_object" "source" {
  bucket = aws_s3_bucket.lambda_bucket.id

  key    = "executable.zip"
  source = data.archive_file.source.output_path

  etag = filemd5(data.archive_file.source.output_path)
}