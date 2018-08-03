package officer2.hajj.org.officer2.read_hajj;

import android.content.Intent;
import android.nfc.NdefMessage;
import android.nfc.NfcAdapter;
import android.os.Parcelable;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        InputStream ins = getResources().openRawResource(
                getResources().getIdentifier("a_121_hajj_ef9552373bb6",
                        "raw", getPackageName()));
        select_hajj("hjk", ins);
    }
    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        if (intent != null && NfcAdapter.ACTION_NDEF_DISCOVERED.equals(intent.getAction())) {
            Parcelable[] rawMessages =
                    intent.getParcelableArrayExtra(NfcAdapter.EXTRA_NDEF_MESSAGES);
            if (rawMessages != null) {
                NdefMessage[] messages = new NdefMessage[rawMessages.length];
                for (int i = 0; i < rawMessages.length; i++) {
                    messages[i] = (NdefMessage) rawMessages[i];
                }
                // Process the messages array.
            }
        }
    }

    public static void select_hajj(String hajj_uid, InputStream ins) {

        /*
        File credentialsPath = new File("client_secrets.json");  // TODO: update to your key path.
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        credentials = ServiceAccountCredentials.fromPkcs8()
        String client_id = "48262551211-6fs3mr3u7t1919hirdcg7lorlu9lv2hb.apps.googleusercontent.com";
        String client_email = "ahmedrefaat532@aucegypt.edu";
        String private_key = "Iaj-_X5K3NxH6yw6bu9vesp8";
        credentials = ServiceAccountCredentials.fromPkcs8(client_id,client_email, private_key);
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        */
        try {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(ins);
            BigQuery bigquery =
                    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(
                            "SELECT * FROM a-121-hajj.a_121_hajj.hajji")
                            // Use standard SQL syntax for queries.
                            // See: https://cloud.google.com/bigquery/sql-reference/
                            .setUseLegacySql(false)
                            .build();

            // Create a job ID so that we can safely retry.
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            // Wait for the query to complete.
            try {
                queryJob = queryJob.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Check for errors
            if (queryJob == null) {
                throw new RuntimeException("Job no longer exists");
            } else if (queryJob.getStatus().getError() != null) {
                // You can also look at queryJob.getStatus().getExecutionErrors() for all
                // errors, not just the latest one.
                throw new RuntimeException(queryJob.getStatus().getError().toString());
            }

            // Get the results.
            QueryResponse response = bigquery.getQueryResults(jobId);

            TableResult result = null;
            try {
                result = queryJob.getQueryResults();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Print all pages of the results.
            for (FieldValueList row : result.iterateAll()) {
                String url = row.get("url").getStringValue();
                long viewCount = row.get("view_count").getLongValue();
                System.out.printf("url: %s views: %d%n", url, viewCount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
