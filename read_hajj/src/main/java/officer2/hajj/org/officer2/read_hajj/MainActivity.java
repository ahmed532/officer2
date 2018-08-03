package officer2.hajj.org.officer2.read_hajj;

import android.content.Intent;
import android.nfc.NdefMessage;
import android.nfc.NfcAdapter;
import android.os.AsyncTask;
import android.os.Parcelable;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;

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
    TextView textView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // select_hajj("hjk", ins);
        new BigQueryTask().execute("fdsf");
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        if (intent != null && NfcAdapter.ACTION_NDEF_DISCOVERED.equals(intent.getAction())) {
            Parcelable[] rawMessages =
                    intent.getParcelableArrayExtra(NfcAdapter.EXTRA_NDEF_MESSAGES);
            if (rawMessages != null) {
                NdefMessage[] messages = new NdefMessage[rawMessages.length];
                String nfcres ="";
                for (int i = 0; i < rawMessages.length; i++) {
                    messages[i] = (NdefMessage) rawMessages[i];
                    System.out.println(messages[i].toString());
                    nfcres += messages[i].toString() + "\n";

                }
                textView = (TextView) findViewById(R.id.nfctextview);
                textView.setText(nfcres+"kjhk");
                new BigQueryTask().execute(nfcres);
                // Process the messages array.
            }
        }
    }

    private class BigQueryTask extends AsyncTask<String, Void, String> {
        @Override
        protected String doInBackground(String... params) {
            try {
                InputStream ins = getResources().openRawResource(
                        getResources().getIdentifier("a_121_hajj_ef9552373bb6",
                                "raw", getPackageName()));
                GoogleCredentials credentials = ServiceAccountCredentials.fromStream(ins);

                BigQuery bigquery =
                        BigQueryOptions.newBuilder().setProjectId("a-121-hajj").setCredentials(credentials).build().getService();
                QueryJobConfiguration queryConfig =
                        QueryJobConfiguration.newBuilder(
                                "SELECT * FROM `a_121_hajj_dataset.hajji`")
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
                String res = "";
                for (FieldValueList row : result.iterateAll()) {
                    String url = row.get("uid").getStringValue();
                    res += "uid: " + url + " name: " + row.get("name");

                }
                return res;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return "";
        }
        @Override
        protected void onPostExecute(String result) {
            System.out.println(result);
            textView = (TextView) findViewById(R.id.textview);
            textView.setText(result);

            // might want to change "executed" for the returned string passed
            // into onPostExecute() but that is upto you
        }

        @Override
        protected void onPreExecute() {}

        @Override
        protected void onProgressUpdate(Void... values) {}
    }
}
