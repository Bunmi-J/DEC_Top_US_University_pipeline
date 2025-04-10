resource "aws_iam_user" "uni_rank" {
  name = "test_protectorate"


  tags = {
    tag-key = "tag-test"
  }
}

resource "aws_iam_access_key" "access_uni_rank" {
  user = aws_iam_user.uni_rank.name
}

resource "aws_ssm_parameter" "access_test" {
  name  = "test_access_key"
  type  = "String"
  value = aws_iam_access_key.access_uni_rank.id
}


resource "aws_ssm_parameter" "secret_test" {
  name  = "test_secret_key"
  type  = "SecureString"
  value = aws_iam_access_key.access_uni_rank.secret
}

resource "aws_iam_policy" "policy_test_protectorate" {
  name        = "demo_s3_test_protectorate"
  path        = "/"
  description = "S3 policy for test_protectorate"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        "Action" : [
          "s3:ListBucket",
          "s3:Get*",
          "s3:Put*",
          "s3:CreateBucket",
          "s3:Deleteobject",
        ],
        "Resource" : [
          "arn:aws:s3:::top-university-bucket",
          "arn:aws:s3:::top-university-bucket/*"
        ]
      },
    ]
  })
}


resource "aws_iam_user_policy_attachment" "policy_test_pro" {
  user       = aws_iam_user.uni_rank.name
  policy_arn = aws_iam_policy.policy_test_protectorate.arn
}