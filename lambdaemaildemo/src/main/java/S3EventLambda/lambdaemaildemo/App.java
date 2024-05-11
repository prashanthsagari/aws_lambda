package S3EventLambda.lambdaemaildemo;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;

public class App implements RequestHandler<S3Event, String> {
	private final AmazonSimpleEmailService sesClient = AmazonSimpleEmailServiceClientBuilder.standard().build();
	private final String EMAIL_SUBJECT = "S3 Upload Status";

	public String handleRequest(S3Event ss, Context cc) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(client);

		// Specify the table name
		String tableName = "track-s3bucket-events";

		// Get current date
		LocalDate currentDate = LocalDate.now();

		// Format date as string
		String dateString = currentDate.format(DateTimeFormatter.ISO_LOCAL_DATE);

		StringBuffer htmlBuilder = new StringBuffer();
		try {
			ScanFilter filter = new ScanFilter("date").eq(dateString);
			ScanSpec spec = new ScanSpec().withScanFilters(filter);
			// Get a reference to the table
			Table table = dynamoDB.getTable(tableName);
			ItemCollection<ScanOutcome> items = table.scan(spec);
			// Construct the email body
			htmlBuilder.append("Hi Prashanth").append(",\n\n");
			htmlBuilder.append("Please find today's list of S3 uploads\n\n");

			htmlBuilder.append("<table border=\"1\">");
			htmlBuilder.append(
					"<tr><th>S3 URI</th><th>Object Name</th><th>Object Size</th><th>Object Type</th><th>Creation Date</th></tr>");

			for (Item item : items) {
				htmlBuilder.append("<tr>");
				htmlBuilder.append("<td>").append(item.getString("uri")).append("</td>");
				htmlBuilder.append("<td>").append(item.getString("key")).append("</td>");
				htmlBuilder.append("<td>").append(item.getLong("objectSize")).append("</td>");
				htmlBuilder.append("<td>").append(item.getString("objectType")).append("</td>");
				htmlBuilder.append("<td>").append(item.getString("date")).append("</td>");
				htmlBuilder.append("</tr>");
			}
			htmlBuilder.append("</table>");
			htmlBuilder.append("Regards AWS Scheduler.");

			if (htmlBuilder != null) {
				sendEmailNotification(htmlBuilder.toString());
			}
		} catch (Exception e) {
			System.out.println("Error while communicating with dynamo db.");
		}

		return "Success";
	}

	private void sendEmailNotification(String messageBody) {
		try {
			SendEmailRequest request = new SendEmailRequest()
					.withDestination(new Destination().withToAddresses("prashanthps7013@gmail.com"))
					.withMessage(new Message()
							.withBody(new Body().withHtml(new Content().withCharset("UTF-8").withData(messageBody)))
							.withSubject(new Content().withCharset("UTF-8").withData(EMAIL_SUBJECT)))
					.withSource("admin@prashanthsagari.online");
			sesClient.sendEmail(request);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
