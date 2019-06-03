package gcp

import (
	"context"
	"fmt"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/gruntwork-io/terratest/modules/logger"
	"google.golang.org/api/iterator"
)

// CreateStorageBucket creates a Google Cloud bucket with the given BucketAttrs. Note that Google Storage bucket names must be globally unique.
func CreateStorageBucket(t *testing.T, projectID string, name string, attr *storage.BucketAttrs) {
	err := CreateStorageBucketE(t, projectID, name, attr)
	if err != nil {
		t.Fatal(err)
	}
}

// CreateStorageBucketE creates a Google Cloud bucket with the given BucketAttrs. Note that Google Storage bucket names must be globally unique.
func CreateStorageBucketE(t *testing.T, projectID string, name string, attr *storage.BucketAttrs) error {
	logger.Logf(t, "Creating bucket %s", name)

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	// Creates a Bucket instance.
	bucket := client.Bucket(name)

	// Creates the new bucket.
	return bucket.Create(ctx, projectID, attr)
}

// DeleteStorageBucket destroys the Google Storage bucket.
func DeleteStorageBucket(t *testing.T, name string) {
	err := DeleteStorageBucketE(t, name)
	if err != nil {
		t.Fatal(err)
	}
}

// DeleteStorageBucketE destroys the S3 bucket in the given region with the given name.
func DeleteStorageBucketE(t *testing.T, name string) error {
	logger.Logf(t, "Deleting bucket %s", name)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	return client.Bucket(name).Delete(ctx)
}

// ReadBucketObject reads an object from the given Storage Bucket and returns its contents.
func ReadBucketObject(t *testing.T, bucketName string, filePath string) io.Reader {
	out, err := ReadBucketObjectE(t, bucketName, filePath)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// ReadBucketObjectE reads an object from the given Storage Bucket and returns its contents.
func ReadBucketObjectE(t *testing.T, bucketName string, filePath string) (io.Reader, error) {
	logger.Logf(t, "Reading object from bucket %s using path %s", bucketName, filePath)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)
	r, err := bucket.Object(filePath).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// WriteBucketObject writes an object to the given Storage Bucket and returns its URL.
func WriteBucketObject(t *testing.T, bucketName string, filePath string, body io.Reader, contentType string) string {
	out, err := WriteBucketObjectE(t, bucketName, filePath, body, contentType)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// WriteBucketObjectE writes an object to the given Storage Bucket and returns its URL.
func WriteBucketObjectE(t *testing.T, bucketName string, filePath string, body io.Reader, contentType string) (string, error) {
	// set a default content type
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	logger.Logf(t, "Writing object to bucket %s using path %s and content type %s", bucketName, filePath, contentType)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}

	w := client.Bucket(bucketName).Object(filePath).NewWriter(ctx)
	w.ContentType = contentType

	// Don't set any ACL or cache control properties for now
	//w.ACL = []storage.ACLRule{{Entity: storage.AllAuthenticatedUsers, Role: storage.RoleReader}}
	// set a default cache control (1 day)
	//w.CacheControl = "public, max-age=86400"

	if _, err := io.Copy(w, body); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}

	const publicURL = "https://storage.googleapis.com/%s/%s"
	return fmt.Sprintf(publicURL, bucketName, filePath), nil
}

// EmptyStorageBucket removes the contents of a storage bucket with the given name.
func EmptyStorageBucket(t *testing.T, name string) {
	err := EmptyStorageBucketE(t, name)
	if err != nil {
		t.Fatal(err)
	}
}

// EmptyStorageBucketE removes the contents of a storage bucket with the given name.
func EmptyStorageBucketE(t *testing.T, name string) error {
	logger.Logf(t, "Emptying storage bucket %s", name)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	// List all objects in the bucket
	//
	// TODO - we should really do a bulk delete call here, but I couldn't find
	// anything in the SDK.
	bucket := client.Bucket(name)
	it := bucket.Objects(ctx, nil)
	for {
		objectAttrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return err
		}

		// purge the object
		logger.Logf(t, "Deleting storage bucket object %s", objectAttrs.Name)
		bucket.Object(objectAttrs.Name).Delete(ctx)
	}

	return nil
}

// AssertStorageBucketExists checks if the given storage bucket exists and fails the test if it does not.
func AssertStorageBucketExists(t *testing.T, name string) {
	err := AssertStorageBucketExistsE(t, name)
	if err != nil {
		t.Fatal(err)
	}
}

// AssertStorageBucketExistsE checks if the given storage bucket exists and returns an error if it does not.
func AssertStorageBucketExistsE(t *testing.T, name string) error {
	logger.Logf(t, "Finding bucket %s", name)

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	// Creates a Bucket instance.
	bucket := client.Bucket(name)

	// TODO - the code below attempts to determine whether the storage bucket
	// exists by making a making a number of API calls, then attemping to
	// list the contents of the bucket. It was adapted from Google's own integration
	// tests and should be improved once the appropriate API call is added.
	// For more info see: https://github.com/GoogleCloudPlatform/google-cloud-go/blob/de879f7be552d57556875b8aaa383bce9396cc8c/storage/integration_test.go#L1231
	if _, err := bucket.Attrs(ctx); err != nil {
		// ErrBucketNotExist
		return err
	}

	it := bucket.Objects(ctx, nil)
	if _, err := it.Next(); err == storage.ErrBucketNotExist {
		return err
	}

	return nil
}

// ReadBucketObject reads an object from the given Storage Bucket and returns its contents.
func CheckBucketAttribs(t *testing.T, bucketName string, attributeName string, attributeValue string) string {
	result, err := CheckBucketAttribsE(t, bucketName, attributeName,attributeValue)
	if err != nil {
		t.Fatal(err)
	}
	if result !="success" {
		t.Fatal(result)
	}
	return result
}


// ReadBucketObjectE reads an object from the given Storage Bucket and returns its contents.
func CheckBucketAttribsE(t *testing.T, bucketName string, attributeName string, attributeValue string) (string, error) {
	logger.Logf(t, "Reading object attrib %s for bucket %s with value %s", attributeName,bucketName,attributeValue)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return "error", err
	}

	attrs, err := client.Bucket(bucketName).Attrs(ctx)
	if (attrs.Name == bucketName) {
		switch strings.ToLower(attributeName) {
		case "location":
			logger.Logf(t,"LOCATION ")
			if (strings.HasPrefix(strings.ToLower(attrs.Location),strings.ToLower(attributeValue))) {
				return "success",nil
			}else{
				if err != nil {
					return "error", err
				}
				return join("Bucket Location and Region must start with ",attributeValue),nil
			}		
		case "storageclass":
			logger.Logf(t,"StorageClass")
			if (strings.Compare(strings.ToUpper(attrs.StorageClass),strings.ToUpper(attributeValue))==0) {
				return "success",nil
			}else{
				if err != nil {
					return "error", err
				}
				return join("Storage Class is ", strings.ToUpper(attrs.StorageClass), " does not match to what is expected - ",attributeValue),nil
			}		
		case "version":
			logger.Logf(t,"version")
			logger.Logf(t,"versioning enabled? %t", attrs.VersioningEnabled) 
			if (strings.ToLower(attributeValue) == "true") {
				if  attrs.VersioningEnabled  {
					return "success",nil
				}else{
					return join("Bucket Versioning should be enabled but is not enabled "),nil
				}	
			}else {
				if  attrs.VersioningEnabled  {
					return join("Bucket Versioning should not be enabled but is enabled "),nil
				} else{
					return "success",nil
				}
			}
		case "labels":
			logger.Logf(t,"Labels %s", attrs.Labels)
		}
	}
	return "success", nil
}


// ReadBucketObject reads an object from the given Storage Bucket and returns its contents.
func CheckBucketLabels(t *testing.T, bucketName string, labelName string, labelValue string) string {
	result, err := CheckBucketLabelsE(t, bucketName, "labels",labelName,labelValue)
	if err != nil {
		t.Fatal(err)
	}
	if result !="success" {
		t.Fatal(result)
	}
	return result
}	

// ReadBucketObjectE reads an object from the given Storage Bucket and returns its contents.
func CheckBucketLabelsE(t *testing.T, bucketName string, attributeName string, labelName string, labelValue string) (string, error) {
	logger.Logf(t, "Reading object attrib %s for bucket %s with value %s", labelName,bucketName,labelValue)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return "error", err
	}

	attrs, err := client.Bucket(bucketName).Attrs(ctx)
	if err != nil {
		return "error", err
	}
	if (attrs.Name == bucketName) {
		logger.Logf(t,"Labels %s", attrs.Labels)
		var mapLabels map[string]string = attrs.Labels

		logger.Logf(t,"Labels variable %s", mapLabels)
		if mapLabels == nil {
			return "error", err
		}
		if mapLabels != nil {
			logger.Logf(t,"Labels %s %s", labelName, mapLabels[labelName])
			if (strings.Compare(mapLabels[labelName],labelValue)==0){ 
			//if (mapLabels[labelName]== mapLabels[labelValue]){
				logger.Logf(t,"Matching Labels found %s = %s", labelName, mapLabels[labelName])
				return "success", nil
			}else{
				if err != nil {
					return "error", err
				}
				return join("Expected value for label ",labelName," is ", labelValue, "but the value is ", mapLabels[labelName] ),nil
			}
		}
			
		

	}

	return "success", nil
}
func join(strs ...string) string {
	var sb strings.Builder
	for _, str := range strs {
		sb.WriteString(str)
	}
	return sb.String()
}
