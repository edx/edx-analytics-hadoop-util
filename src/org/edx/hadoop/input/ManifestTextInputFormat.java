package org.edx.hadoop.input;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;


public class ManifestTextInputFormat extends KeyValueTextInputFormat {
    public static int RETRY_LIMIT = 10;
    public static long REPORTING_INTERVAL = 1000l * 60 * 5;

    protected FileStatus[] listStatus(JobConf job) throws IOException {
        FileStatus[] manifests = super.listStatus(job);
        List<FileStatus> paths = new ArrayList<FileStatus>();
        long lastReportTime = System.currentTimeMillis();

        for (FileStatus manifest : manifests) {
            List<Path> globPaths = this.readManifest(manifest.getPath(), job);

            for(int i = 0; i < globPaths.size(); i++) {
                Path globPath = globPaths.get(i);

                paths.addAll(this.expandPath(globPath, job));

                if ((System.currentTimeMillis() - lastReportTime) >= REPORTING_INTERVAL) {
                    lastReportTime = System.currentTimeMillis();

                    if (globPath != null)
                        LOG.info("Fetched " + i + " of " + globPaths.size() + " paths, most recently: " + globPath.toUri());
                    else
                        LOG.info("Fetched " + i + " of " + globPaths.size() + " paths.");
                }
            }
        }

        return paths.toArray(new FileStatus[1]);
    }

    private List<Path> readManifest(Path manifestPath, JobConf job) throws IOException {
        FileSystem fs = manifestPath.getFileSystem(job);
        List<Path> paths = new ArrayList<Path>();
        DataInputStream dataStream = fs.open(manifestPath);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(dataStream));
            String line = reader.readLine();
            while(line != null) {
                paths.add(new Path(line));
                line = reader.readLine();
            }
        } finally {
            dataStream.close();
        }

        return paths;
    }

    private List<FileStatus> expandPath(Path globPath, JobConf conf) throws IOException {
        FileSystem fs = globPath.getFileSystem(conf);

        int attempts = 0;
        boolean success = false;
        FileStatus[] matches = null;
        while (!success && attempts < RETRY_LIMIT) {
            attempts += 1;
            try {
                matches = fs.globStatus(globPath);
                success = true;
            } catch (Exception exc) {
                LOG.info("Exception while calling globStatus, retrying...", exc);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("This should never happen", e);
                }
            }
        }

        if (!success) {
            throw new RuntimeException("Unable to fetch glob status '" + globPath.toUri() + "' after " + RETRY_LIMIT + " attempts");
        }

        List<FileStatus> paths = new ArrayList<FileStatus>();

        // fs.globStatus returns null when the path looks like a file and it does not exist.
        if (matches == null) {
            return paths;
        }

        for (int i = 0; i < matches.length; i++) {
            FileStatus match = matches[i];
            if (match.isDirectory()) {
                FileStatus[] childStatuses = fs.listStatus(match.getPath());
                for (int j = 0; j < childStatuses.length; j++) {
                    paths.add(childStatuses[j]);
                }
            } else {
                paths.add(match);
            }
        }

        return paths;
    }
}
