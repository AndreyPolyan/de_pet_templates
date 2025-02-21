import boto3


class S3Connect:
    """Handles connection to an S3-compatible storage service."""

    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, endpoint_url: str
    ) -> None:
        """
        Initialize the S3 client with authentication details.

        Args:
            aws_access_key_id (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
            endpoint_url (str): URL of the S3-compatible endpoint.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url

    def client(self) -> boto3.client:
        """
        Create and return an S3 client instance.

        Returns:
            boto3.client: A client for interacting with the S3-compatible service.
        """
        return boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
        )