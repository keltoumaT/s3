import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;


import java.io.*;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {

        UUID uuid = UUID.randomUUID();
        String tempFolder = "working-temp__" + uuid + "/";
        String bucketName = "test-s3-s";

        try {


            ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("/").withPrefix("");
            ListObjectsV2Result result;

            do {
                result = amazonS3.listObjectsV2(listObjectsV2Request);

                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
                    S3Object object = amazonS3.getObject(bucketName, objectSummary.getKey());
                    InputStream inputStream = object.getObjectContent();

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();

                    StringWriter s = new StringWriter();
                    CSVWriter writer = new CSVWriter(s, '|',
                            CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\n");

                    CSVReader reader = new CSVReader(new InputStreamReader(inputStream, "UTF-8"));
                    String[] nextLine;
                    while ((nextLine = reader.readNext()) != null) {
                        writer.writeNext(nextLine);
                    }





                    ObjectMetadata metadata = new ObjectMetadata();
                    // metadata.setContentLength(data.length);



                    //writer.flush();
                    writer.close();
                    String finalString = s.toString();

                    System.out.println("Actual data:- {} " +  finalString);
                    amazonS3.putObject(bucketName, object.getKey(),new ByteArrayInputStream(finalString.getBytes()), metadata);

                    System.out.println(String.valueOf(writer));



                }
                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                System.out.println("Next Continuation Token: " + token);
                listObjectsV2Request.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (
                SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }// Amazon S3 couldn't be contacted for a response, or the client
// couldn't parse the response from Amazon S3.
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        }
    }
}




