package S3EventLambda.lambdademo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import net.coobird.thumbnailator.Thumbnails;

public class App implements RequestHandler<S3Event, String> {
	private static final String BUCKET_NAME = "event-source-aws-bucket";
	private static final String THUMBNAIL_BUCKET_NAME = "event-source-aws-bucket-thumbnail";

	AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

	@Override
	public String handleRequest(S3Event s3Event, Context context) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(client);

		// Specify the table name
		String tableName = "track-s3bucket-events";

		// Get a reference to the table
		Table table = dynamoDB.getTable(tableName);

		for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
			String bucketName = record.getS3().getBucket().getName();
			String key = record.getS3().getObject().getKey();
			String objectKey = key.replace("+", " ");
			String uri = String.format("s3://%s/%s", BUCKET_NAME, objectKey);
			String objectType = objectKey.substring(objectKey.lastIndexOf('.') + 1);
			long objectSize = record.getS3().getObject().getSize();

			// Create thumbnail for image uploads
			if (objectType.equalsIgnoreCase("jpg") || objectType.equalsIgnoreCase("jpeg")
					|| objectType.equalsIgnoreCase("png")) {
				createThumbnail(THUMBNAIL_BUCKET_NAME, objectKey);
			}

			// Get current date
			LocalDate currentDate = LocalDate.now();

			// Format date as string
			String dateString = currentDate.format(DateTimeFormatter.ISO_LOCAL_DATE);

			// Create an Item to be inserted
			Item item = new Item().withPrimaryKey("key", objectKey) // Primary key attribute and its value
					.withString("uri", uri).withString("bucketName", bucketName).withString("objectType", objectType)
					.withLong("objectSize", objectSize).withString("date", dateString);

			// Create a PutItemSpec to specify additional parameters if needed
			PutItemSpec putItemSpec = new PutItemSpec().withItem(item);

			// Insert the item into the table
			table.putItem(putItemSpec);

			System.out.println("Succesfully executed function");

		}

		return "Success";
	}

	private void createThumbnail(String bucketName, String objectKey) {
		try {
			// Retrieve original image from S3
			S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, objectKey));
			InputStream inputStream = object.getObjectContent();

			// Create thumbnail
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			Thumbnails.of(inputStream).size(100, 100).toOutputStream(outputStream);

			// Upload thumbnail to the S3
			ByteArrayInputStream thumbnailInputStream = new ByteArrayInputStream(outputStream.toByteArray());
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(outputStream.size());
			s3.putObject(bucketName, objectKey + "-thumbnail.jpg", thumbnailInputStream, metadata);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
