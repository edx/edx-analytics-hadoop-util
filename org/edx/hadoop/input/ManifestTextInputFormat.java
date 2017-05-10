package org.edx.hadoop.input;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.commons.logging.LogFactory;


public class ManifestTextInputFormat extends KeyValueTextInputFormat {

    protected FileStatus[] listStatus(JobConf job) throws IOException {
        FileStatus[] manifests = super.listStatus(job);
        List<FileStatus> paths = new ArrayList<FileStatus>();
        for(int i = 0; i < manifests.length; i++) {
            List<Path> globPaths = this.readManifest(manifests[i].getPath(), job);
            for (Path globPath : globPaths) {
                paths.addAll(this.expandPath(globPath, job));
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
        while (!success && attempts < 10) {
            attempts += 1;
            try {
                LOG.info("Fetching file status: " + globPath.toUri());
                matches = fs.globStatus(globPath);
                success = true;
            } catch (Exception exc) {
                LOG.info("Unable to call globStatus, retrying...", exc);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("This should never happen");
                }
            }
        }

        if (!success) {
            throw new RuntimeException("Unable to fetch glob status after several attempts");
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
