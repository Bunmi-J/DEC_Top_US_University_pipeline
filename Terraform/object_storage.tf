resource "aws_s3_bucket" "dec_hackathon_team1" {
  bucket = "top-university-bucket"
}

resource "aws_s3_bucket_versioning" "hackathon-bucket" {
  bucket = aws_s3_bucket.dec_hackathon_team1.id
  versioning_configuration {
    status = "Enabled"
  }
}
