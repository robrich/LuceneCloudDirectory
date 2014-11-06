namespace Lucene.Net.Store.Cloud.Amazon
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using Lucene.Net.Store.Cloud;
    using Lucene.Net.Store.Cloud.Models;
    using global::Amazon;
    using global::Amazon.S3;
    using global::Amazon.S3.Model;

    public class AmazonCloudProvider : ICloudProvider
    {
        private readonly string amazonKey;
        private readonly string amazonSecret;
        private readonly string amazonBucket;

        public AmazonCloudProvider(string AmazonKey, string AmazonSecret, string AmazonBucket)
        {
            if (string.IsNullOrEmpty(AmazonKey))
            {
                throw new ArgumentNullException("AmazonKey");
            }
            if (string.IsNullOrEmpty(AmazonSecret))
            {
                throw new ArgumentNullException("AmazonSecret");
            }
            if (string.IsNullOrEmpty(AmazonBucket))
            {
                throw new ArgumentNullException("AmazonBucket");
            }
            this.amazonKey = AmazonKey;
            this.amazonSecret = AmazonSecret;
            this.amazonBucket = AmazonBucket;
        }

        public void InitializeStorage()
        {
            // If these error with things like invalid credentials, now you know
            using (AmazonS3 client = AWSClientFactory.CreateAmazonS3Client(this.amazonKey, this.amazonSecret))
            {

                ListBucketsRequest request = new ListBucketsRequest
                {
                };
                ListBucketsResponse response = client.ListBuckets(request);

                List<S3Bucket> buckets = response.Buckets;
                bool exists = (
                    from b in buckets
                    where string.Equals(b.BucketName, this.amazonBucket, StringComparison.InvariantCultureIgnoreCase)
                    select b
                ).Any();

                if (!exists)
                {
                    PutBucketResponse bucketResponse = client.PutBucket(new PutBucketRequest
                    {
                        BucketName = this.amazonBucket
                    });
                }
            }
        }
        public List<string> ListAll()
        {

            List<string> files = new List<string>();
            using (AmazonS3 client = AWSClientFactory.CreateAmazonS3Client(this.amazonKey, this.amazonSecret))
            {

                ListObjectsRequest request = new ListObjectsRequest
                {
                    BucketName = this.amazonBucket
                };
                do
                {
                    ListObjectsResponse response = client.ListObjects(request);

                    foreach (S3Object s3Object in response.S3Objects)
                    {
                        files.Add(s3Object.Key);
                    }

                    // If response is truncated, get the next page
                    if (response.IsTruncated)
                    {
                        request.Marker = response.NextMarker;
                    }
                    else
                    {
                        request = null;
                    }
                } while (request != null);
            }

            return files;
        }
        // Always returns an object even if file doesn't exist
        public FileMetadata FileMetadata(string name)
        {
            FileMetadata results = new FileMetadata
            {
                Name = name
            };
            using (AmazonS3 client = AWSClientFactory.CreateAmazonS3Client(this.amazonKey, this.amazonSecret))
            {

                GetObjectMetadataRequest request = new GetObjectMetadataRequest
                {
                    BucketName = this.amazonBucket,
                    Key = name
                };

                try
                {
                    GetObjectMetadataResponse response = client.GetObjectMetadata(request);

                    results.Exists = true; // else AWSSDK threw
                    results.Length = response.ContentLength;
                    results.LastModified = response.LastModified;

                }
                catch (AmazonS3Exception ex)
                {
                    if (ex.ErrorCode == "NoSuchKey")
                    {
                        results.Exists = false; // File doesn't exist
                    }
                    else
                    {
                        throw;
                    }
                }

            }
            return results;
        }
        public void Delete(string name)
        {
            using (AmazonS3 client = AWSClientFactory.CreateAmazonS3Client(this.amazonKey, this.amazonSecret))
            {
                DeleteObjectRequest request = new DeleteObjectRequest
                {
                    BucketName = this.amazonBucket,
                    Key = name
                };
                client.DeleteObject(request);
            }
            if (!name.EndsWith(".lock"))
            {
                this.Delete(name + ".lock");
            }
        }
        public Stream Download(string name)
        {
            using (AmazonS3 client = AWSClientFactory.CreateAmazonS3Client(this.amazonKey, this.amazonSecret))
            {

                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = this.amazonBucket,
                    Key = name
                };

                try
                {
                    GetObjectResponse response = client.GetObject(request);

                    MemoryStream ms = new MemoryStream();
                    response.ResponseStream.CopyTo(ms);
                    ms.Position = 0;

                    return ms;
                }
                catch (AmazonS3Exception ex)
                {
                    if (ex.ErrorCode == "NoSuchKey")
                    {
                        return null; // File doesn't exist
                    }
                    else
                    {
                        throw;
                    }
                }

            }
        }
        public void Upload(string name, Stream content, FileMetadata FileMetadata)
        {
            using (AmazonS3 client = AWSClientFactory.CreateAmazonS3Client(this.amazonKey, this.amazonSecret))
            {
                PutObjectRequest request = new PutObjectRequest
                {
                    BucketName = this.amazonBucket,
                    Key = name,
                    InputStream = content
                };
                client.PutObject(request);
            }
        }
        public void Touch(string name)
        {
            // FRAGILE: Amazon doesn't support modifying the date
            // TODO: Download then re-upload it?
        }
        // FRAGILE: It's likely much less chatty to store locks in an external data store like Redis or SimpleDB http://stackoverflow.com/questions/3431418/locking-with-s3
        public bool ObtainLock(string name)
        {
            if (this.IsLocked(name))
            {
                return false;
            }
            using (MemoryStream ms = new MemoryStream())
            {
                this.Upload(name, ms, new FileMetadata());
            }
            return true;
        }
        public void Releaselock(string name)
        {
            this.Delete(name);
        }
        public bool IsLocked(string name)
        {
            return this.FileMetadata(name).Exists;
        }

    }
}
