package bulkprocessor

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

// S3Client handles interactions with S3
type S3Client struct {
	client     *s3.S3
	uploader   *s3manager.Uploader
	config     S3Config
	bucketName string
}

// S3Writer implements a writer that streams data directly to S3
type S3Writer struct {
	s3Client   *S3Client
	s3Key      string
	pipe       *io.PipeWriter
	uploadDone chan error
	closed     bool
	uploadErr  error // Store the upload error for subsequent operations
	mutex      sync.Mutex
	wg         sync.WaitGroup // Add a WaitGroup to properly track upload completion
}

// NewS3Client creates a new S3 client
func NewS3Client(config S3Config) (*S3Client, error) {
	var s3Config *aws.Config

	if config.Endpoint != "" {
		// For custom S3-compatible services like MinIO
		s3Config = &aws.Config{
			Endpoint:    aws.String(config.Endpoint),
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		}
	} else {
		// For AWS S3
		s3Config = &aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		}
	}

	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create S3 session")
	}

	// Create S3 client and uploader
	s3Client := s3.New(sess)
	uploader := s3manager.NewUploaderWithClient(s3Client, func(u *s3manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // 5MB part size
		u.Concurrency = 5            // 5 concurrent uploads
	})

	return &S3Client{
		client:     s3Client,
		uploader:   uploader,
		config:     config,
		bucketName: config.BucketName,
	}, nil
}

// Upload uploads data to S3
func (c *S3Client) Upload(reader io.Reader, s3Key string) error {
	// Ensure the key has the correct prefix
	s3Key = c.ensurePrefix(s3Key)

	// Use S3 uploader for efficient uploads
	_, err := c.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(s3Key),
		Body:   reader,
	})

	if err != nil {
		return errors.Wrap(err, "failed to upload data to S3")
	}

	return nil
}

// NewStreamingWriter creates a new S3Writer for streaming data directly to S3
func (c *S3Client) NewStreamingWriter(s3Key string) (*S3Writer, error) {
	// Ensure the key has the correct prefix
	s3Key = c.ensurePrefix(s3Key)

	// Create a pipe
	pipeReader, pipeWriter := io.Pipe()

	// Create upload done channel for signaling when upload is complete
	uploadDone := make(chan error, 1)

	writer := &S3Writer{
		s3Client:   c,
		s3Key:      s3Key,
		pipe:       pipeWriter,
		uploadDone: uploadDone,
	}

	// Use WaitGroup to properly track when the upload goroutine completes
	writer.wg.Add(1)

	// Start upload in a goroutine
	go func() {
		defer writer.wg.Done()

		_, err := c.uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(c.bucketName),
			Key:    aws.String(s3Key),
			Body:   pipeReader,
		})

		// Close the reader when upload is done
		pipeReader.Close()

		// Signal upload completion and error status
		select {
		case uploadDone <- err:
			// Error was sent
		default:
			// Channel already has an error or is closed
		}

		// If there's an error, also propagate it through the pipe reader
		if err != nil {
			pipeReader.CloseWithError(errors.Wrap(err, "S3 upload failed"))
		}
	}()

	return writer, nil
}

// Write implements the io.Writer interface for S3Writer
func (w *S3Writer) Write(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return 0, errors.New("writer is closed")
	}

	// Check if we already have an upload error
	if w.uploadErr != nil {
		return 0, errors.Wrap(w.uploadErr, "previous S3 upload error")
	}

	// Check for new upload errors (non-blocking)
	select {
	case err := <-w.uploadDone:
		if err != nil {
			w.uploadErr = err
			w.pipe.CloseWithError(err) // Close the pipe to prevent further writes
			return 0, errors.Wrap(err, "S3 upload failed")
		}
	default:
		// No message in the channel, upload still in progress or completed successfully
	}

	return w.pipe.Write(p)
}

// Close closes the writer and waits for the upload to complete
func (w *S3Writer) Close() error {
	w.mutex.Lock()

	if w.closed {
		w.mutex.Unlock()
		return nil
	}

	// Mark as closed
	w.closed = true

	// If we already detected an error in Write(), return it now
	if w.uploadErr != nil {
		w.mutex.Unlock()
		return errors.Wrap(w.uploadErr, "previous S3 upload error detected during close")
	}

	// Close the pipe to signal EOF to the reader
	pipeErr := w.pipe.Close()
	w.mutex.Unlock()

	if pipeErr != nil {
		return errors.Wrap(pipeErr, "failed to close pipe writer")
	}

	// Wait for upload to complete
	w.wg.Wait()

	// Check for any errors that occurred during upload
	select {
	case err := <-w.uploadDone:
		if err != nil {
			return errors.Wrap(err, "failed to upload data to S3")
		}
	default:
		// No error in channel means upload succeeded
	}

	return nil
}

// GetS3URL returns the S3 URL for the given key
func (c *S3Client) GetS3URL(s3Key string) string {
	// Ensure the key has the correct prefix
	s3Key = c.ensurePrefix(s3Key)

	return fmt.Sprintf("s3://%s/%s/%s", c.config.Endpoint, c.bucketName, s3Key)
}

// ensurePrefix ensures the key has the correct prefix
func (c *S3Client) ensurePrefix(s3Key string) string {
	if c.config.Prefix != "" && !strings.HasPrefix(s3Key, c.config.Prefix) {
		s3Key = filepath.Join(c.config.Prefix, s3Key)
	}
	return s3Key
}
