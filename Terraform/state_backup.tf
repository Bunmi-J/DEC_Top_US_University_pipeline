terraform {
  backend "s3" {
    bucket = "Top_university_bucket"
    key    = "backend/terraform/state"
    region = "us-east-1"
  }
}
